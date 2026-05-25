import asyncio
import itertools
from typing import (
    Any,
    List,
    Iterable,
    Set,
    Dict,
    Tuple,
    Union
)

from binance.core.processors.base import Processor

from binance.core.common.constants import (
    SubType
)
from binance.core.common.exceptions import (
    InvalidSubParamsException,
    UnsupportedSubTypeException
)

from binance.core.common.types import StreamError

from binance.core.common.utils import (
    make_list,
    wrap_coroutine
)


class HandlerContext:
    """Internal: routes incoming stream messages to handlers and builds subscribe params.

    The set of processors is supplied by the client's market (injected, not
    hardcoded): the client class provides ``PROCESSORS`` (the subtype processor
    classes), ``EXCEPTION_PROCESSOR`` and ``STREAM_ERROR_PROCESSOR``.
    """

    # all supported processors
    _all_processors: List[Processor]

    # processors that current used
    _processors: Set[Processor]

    # The map of payload_type -> processor
    _processor_cache: Dict[SubType, Processor]

    def __init__(self, client) -> None:
        self._handler_table: Dict[str, Any] = {}
        self._all_processors = [Factory(client) for Factory in client.PROCESSORS]
        self._processors = set()
        self._processor_cache = {}
        self._exception_processor = client.EXCEPTION_PROCESSOR(client)
        self._stream_error_processor = client.STREAM_ERROR_PROCESSOR(client)

    def set_handler(self, handler) -> bool:
        """Register a handler with the first processor that claims it; return False if none does."""
        if self._exception_processor.supports_handler(handler):
            self._exception_processor.add_handler(handler)
            return True

        if self._stream_error_processor.supports_handler(handler):
            self._stream_error_processor.add_handler(handler)
            return True

        for processor in self._all_processors:
            if processor.supports_handler(handler):
                self._processors.add(processor)
                processor.add_handler(handler)
                return True

        return False

    async def dispatch_stream_error(self, error: StreamError) -> None:
        """Deliver a ``StreamError`` to all registered ``StreamErrorHandlerBase`` instances.

        No-op when no ``StreamErrorHandlerBase`` has been registered.

        Args:
            error: the structured stream-control error to dispatch.
        """
        await self._stream_error_processor.dispatch(error)

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
        """Normalize subscribe/unsubscribe positional args into canonical ``(SubType, ...)`` tuples."""
        # Subs is a Tuple[tuple]
        subs = args if type(args[0]) is tuple else (args,)
        params = []

        for subtype_param in subs:
            length = len(subtype_param)
            prefix = None
            args_iter: Any  # product arity varies by branch

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
        """Resolve canonical subscription tuples into wire-format params concurrently."""
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
        """Fan out a message to active processors; forward any exception to the exception processor."""
        try:
            await self._receive(msg)
        except Exception as e:
            await self._exception_processor.dispatch(e)
