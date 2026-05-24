import asyncio
import itertools
from typing import (
    List,
    Iterable,
    Set,
    Dict,
    Tuple,
    Union
)

from binance.processors import (
    PROCESSORS,
    ExceptionProcessor
)

from binance.processors.base import Processor

from binance.common.constants import (
    SubType
)
from binance.common.exceptions import (
    InvalidSubParamsException,
    UnsupportedSubTypeException
)

from binance.common.utils import (
    make_list,
    wrap_coroutine
)


class HandlerContext:
    """Internal: routes incoming WebSocket messages to registered handlers.

    Created lazily by ``SubscriptionManager._get_handler_ctx()`` and held for
    the lifetime of a session (reset by ``SubscriptionManager.close()``).

    Responsibilities:
    - Maintain the set of active ``Processor`` instances (one per stream
      sub-type) that have at least one handler registered.
    - Translate ``subscribe``/``unsubscribe`` argument overloads into
      canonical ``(SubType, ...)`` tuples via ``overload_subscriptions``.
    - Resolve those tuples into wire-format subscribe params (stream-name
      strings or dicts for the WS API) via ``subscribe_params``.
    - Fan out each incoming message to all active processors that claim it,
      delegating exception handling to ``ExceptionProcessor``.
    """

    PROCESSORS = PROCESSORS

    # all supported processors
    _all_processors: List[Processor]

    # processors that current used
    _processors: Set[Processor]

    # The map of payload_type -> processor
    _processor_cache: Dict[SubType, Processor]

    def __init__(self, client) -> None:
        self._handler_table = {}
        self._all_processors = [Factory(client) for Factory in self.PROCESSORS]
        self._processors = set()
        self._processor_cache = {}
        self._exception_processor = ExceptionProcessor(client)

    def set_handler(self, handler) -> bool:
        """Register a handler with the appropriate processor.

        Checks the exception processor first (``HandlerExceptionHandlerBase``),
        then iterates ``_all_processors`` in order. The first processor whose
        ``supports_handler`` returns ``True`` receives the handler and is added
        to the active ``_processors`` set.

        Args:
            handler: A ``Handler`` subclass instance to register.

        Returns:
            bool: ``True`` if a matching processor was found and the handler
            was registered; ``False`` if no processor claimed the handler
            (caller should raise ``InvalidHandlerException``).
        """
        if self._exception_processor.supports_handler(handler):
            self._exception_processor.add_handler(handler)
            return True

        for processor in self._all_processors:
            if processor.supports_handler(handler):
                self._processors.add(processor)
                processor.add_handler(handler)
                return True

        return False

    # client.subscribe(subtype_needs_no_param_or_has_default_param)
    # -> client.subscribe(SubType.ALL_MARKET_MINI_TICKERS)

    # client.subscribe(subtype, param)
    # -> client.subscribe(SubType.TICKER, 'BTCUSDT')

    # client.subscribe(subtypes, params)
    # -> client.subscribe(
    #   [SubType.TICKER, SubType.ORDER_BOOK],
    #   ['BTCUSDT', 'BNBUSDT']
    # )

    # client.subscribe((subtype, param), *subtype_param_pairs)
    # -> client.subscribe(
    #       (SubType.TICKER, 'BNBUSDT)
    # )
    def overload_subscriptions(self, *args) -> List[tuple]:
        """Normalize ``subscribe``/``unsubscribe`` positional arguments into canonical tuples.

        Supports four calling shapes:

        - ``(SubType,)`` — subtype with no extra params (e.g. ``ALL_MARKET_MINI_TICKERS``).
        - ``(SubType, symbol)`` or ``([SubType, ...], [symbol, ...])`` — the
          Cartesian product of subtypes and symbols is expanded.
        - ``(SubType, symbol, interval_or_window)`` — for subtypes that require
          a third parameter (``KLINE``, ``KLINE_UTC8``, ``ORDER_BOOK``,
          ``PARTIAL_ORDER_BOOK``, ``WINDOW_TICKER``).
        - ``((SubType, ...), (SubType, ...), ...)`` — multiple pre-formed tuples
          are processed individually.

        Args:
            *args: Raw positional arguments forwarded from ``subscribe``/``unsubscribe``.

        Returns:
            List[tuple]: Canonical subscription tuples, each of the form
            ``(SubType, ...)`` ready for ``subscribe_params``.

        Raises:
            InvalidSubParamsException: If the argument shape does not match
                any supported overload.
        """
        # Subs is a Tuple[tuple]
        subs = args if type(args[0]) is tuple else (args,)
        params = []

        for subtype_param in subs:
            length = len(subtype_param)
            prefix = None

            # subtype without params
            # ('allMarketMiniTickers',)
            if length == 1:
                args_iter = itertools.product(
                    make_list(subtype_param[0])
                )
            # ('trade', 'BNBUSDT')
            # (['trade'], ['BNBUSDT'])
            elif length == 2:
                args_iter = itertools.product(
                    make_list(subtype_param[0]),
                    make_list(subtype_param[1])
                )

            # subtypes with three args:
            #   (subtype, symbol, interval/window/level)
            elif length == 3 and subtype_param[0] in (
                SubType.KLINE,
                SubType.KLINE_UTC8,
                SubType.ORDER_BOOK,
                SubType.PARTIAL_ORDER_BOOK,
                SubType.WINDOW_TICKER
            ):
                prefix = subtype_param[0]

                args_iter = itertools.product(
                    make_list(subtype_param[1]),
                    make_list(subtype_param[2])
                )
            elif (
                length == 4
                and subtype_param[0] == SubType.PARTIAL_ORDER_BOOK
            ):
                prefix = subtype_param[0]

                args_iter = itertools.product(
                    make_list(subtype_param[1]),
                    make_list(subtype_param[2]),
                    make_list(subtype_param[3])
                )

            else:
                raise InvalidSubParamsException('please check the document')

            for partial_args in args_iter:
                if prefix is None:
                    params.append(partial_args)
                else:
                    params.append(
                        (prefix, *partial_args)
                    )

        return params

    async def subscribe_params(
        self,
        subscribe: bool,
        subscriptions: Iterable[tuple]
    ) -> Tuple[Union[str, dict], ...]:
        """Resolve canonical subscription tuples into wire-format subscribe params.

        Delegates to each subscription's processor via ``_subscribe_param``,
        running all resolutions concurrently with ``asyncio.gather``.

        Args:
            subscribe: ``True`` to build subscribe params; ``False`` for
                unsubscribe. Passed through to each processor so it can
                apply direction-specific logic (e.g. ``UserProcessor`` checks
                subscription state).
            subscriptions: Iterable of canonical ``(SubType, ...)`` tuples as
                produced by ``overload_subscriptions``.

        Returns:
            Tuple[Union[str, dict], ...]: One param per subscription. Market
            streams yield a stream-name ``str`` (e.g. ``'btcusdt@aggTrade'``);
            user-stream subscriptions yield a ``dict`` of WS-API parameters.
        """
        tasks = [
            self._subscribe_param(subscribe, *params)
            for params in subscriptions
        ]

        return await asyncio.gather(*tasks)

    async def _subscribe_param(
        self,
        subscribe: bool,
        *args
    ) -> Union[str, dict]:
        processor = self._get_processor(args[0])
        return await wrap_coroutine(
            processor.subscribe_param(subscribe, *args)
        )

    def _get_processor(
        self,
        subtype: SubType
    ) -> Processor:
        processor = self._processor_cache.get(subtype)
        if processor:
            return processor

        for p in self._all_processors:
            if p.supports_subtype(subtype):
                self._processor_cache[subtype] = p
                return p

        raise UnsupportedSubTypeException(subtype)

    async def _receive(self, msg) -> None:
        for processor in self._processors:
            is_payload, payload = processor.is_message_type(msg)

            if is_payload:
                await processor.dispatch(payload)

    async def receive(self, msg) -> None:
        """Dispatch a parsed WebSocket message to all active processors.

        Calls the internal ``_receive`` which fans out to every processor in
        ``_processors`` that claims the message via ``is_message_type``. Any
        exception raised during dispatch is caught and forwarded to
        ``_exception_processor.dispatch`` so registered exception handlers
        receive it instead of crashing the receive loop.

        Args:
            msg: Parsed JSON dict from the WebSocket. Shape varies by stream
                type; the processors are responsible for recognising their own
                payload format.
        """
        try:
            await self._receive(msg)
        except Exception as e:
            await self._exception_processor.dispatch(e)
