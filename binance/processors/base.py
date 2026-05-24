import asyncio
import inspect
from typing import (
    Optional,
    Set,
    Awaitable,
    Union
)

from binance.common.exceptions import (
    InvalidSubTypeParamException
)
from binance.common.utils import normalize_symbol
from binance.common.constants import (
    SubType,
    ATOM,
    KEY_PAYLOAD,
    KEY_PAYLOAD_TYPE
)
from binance.handlers.base import Handler


class Processor:
    """Internal base class for a single stream sub-type.

    Each concrete ``Processor`` subclass handles one ``SubType`` (e.g.
    ``SubType.TRADE``) end-to-end:

    - Recognising its own messages via ``is_message_type``.
    - Building the Binance wire-format subscribe param string via
      ``subscribe_param``.
    - Registering handlers via ``add_handler`` and dispatching payloads to
      them via ``dispatch``.

    Class attributes that subclasses must or may override:

    - ``HANDLER`` — the ``Handler`` base class whose instances this processor
      accepts (checked by ``supports_handler``).
    - ``SUB_TYPE`` — the ``SubType`` enum value this processor handles
      (checked by ``supports_subtype``). Also used as the default
      ``PAYLOAD_TYPE`` when left as ``ATOM``.
    - ``PAYLOAD_TYPE`` — the ``'e'`` field value expected in the ``'data'``
      envelope. Defaults to ``SUB_TYPE.value``; override explicitly when the
      stream's event type differs (e.g. ``'depthUpdate'`` for order-book).
    """

    # The handler class
    HANDLER: type

    # The payload['e'] of message
    PAYLOAD_TYPE = ATOM

    # subtype used by client.subscribe
    SUB_TYPE: Optional[SubType] = None

    def __init__(
        self,
        client
    ):
        self._client = client

        self._handlers = set()

        if self.PAYLOAD_TYPE == ATOM and self.SUB_TYPE is not None:
            self.PAYLOAD_TYPE = self.SUB_TYPE.value

    def supports_subtype(
        self,
        t: SubType
    ) -> bool:
        """Return whether this processor handles the given ``SubType``.

        Used by ``HandlerContext._get_processor`` to look up which processor
        owns a particular subscription key.

        Args:
            t: The ``SubType`` to check.

        Returns:
            bool: ``True`` if ``t == self.SUB_TYPE``.
        """
        return t == self.SUB_TYPE

    # -----------------------------------------------

    def _get_param_symbol(self, t, args):
        if len(args) == 0:
            raise InvalidSubTypeParamException(
                t, 'symbol', 'string expected but not specified')

        symbol = args[0]

        if type(symbol) is not str:
            raise InvalidSubTypeParamException(
                t, 'symbol', 'string expected but got `%s`' % symbol)

        return symbol

    def subscribe_param(
        self,
        subscribe: bool,
        t: SubType,
        *args
    ) -> Union[str, dict]:
        """Build the Binance wire-format subscribe parameter for this stream.

        The default implementation produces ``<SYMBOL_LOWER>@<subtype>``
        (e.g. ``'btcusdt@trade'``). Subclasses override this when the stream
        name has a different structure (extra suffixes, no symbol, or when a
        signed dict must be returned for WS-API streams).

        Args:
            subscribe: ``True`` for subscribe, ``False`` for unsubscribe.
                Processors that need direction-specific behaviour (e.g.
                ``UserProcessor``) can inspect this.
            t: The ``SubType`` being subscribed.
            *args: Additional parameters — typically the symbol string, and
                optionally a time-frame, depth level, or update interval
                depending on the subtype.

        Returns:
            Union[str, dict]: A stream-name string for market streams, or a
            dict of WS-API request parameters for user-data streams.

        Raises:
            InvalidSubTypeParamException: If a required parameter (e.g.
                symbol) is missing or has the wrong type.
        """
        symbol = self._get_param_symbol(t, args)

        return f'{normalize_symbol(symbol)}@{t}'

    def supports_handler(
        self,
        handler: Handler
    ) -> bool:
        """Return whether this processor can accept the given handler.

        Checks ``isinstance(handler, self.HANDLER)``. Subclasses that support
        multiple handler base classes (like ``UserProcessor``) override this
        to check against a tuple of handler types.

        Args:
            handler: The handler instance to inspect.

        Returns:
            bool: ``True`` if the handler belongs to this processor's stream
            type.
        """
        return isinstance(handler, self.HANDLER)

    def is_message_type(self, msg):
        """Determine whether an incoming message belongs to this processor.

        The default implementation looks for ``msg['data']['e'] == PAYLOAD_TYPE``
        (the standard combined-stream envelope). Subclasses override this for
        streams that use a different envelope shape — for example,
        ``BookTickerProcessor`` matches on ``msg['stream']`` suffix, and
        ``PartialOrderBookProcessor`` additionally checks for ``'bids'``/
        ``'asks'`` keys.

        Args:
            msg: Parsed WebSocket JSON dict.

        Returns:
            Tuple[bool, Optional[dict]]: ``(True, payload_dict)`` when the
            message matches, ``(False, None)`` otherwise. ``payload_dict`` is
            the inner payload that will be forwarded to ``dispatch``.
        """
        payload = msg.get(KEY_PAYLOAD)

        if (
            payload is not None
            and type(payload) is dict
            and payload.get(KEY_PAYLOAD_TYPE) == self.PAYLOAD_TYPE
        ):
            return True, payload

        return False, None

    def add_handler(
        self,
        handler: Handler
    ) -> None:
        """Register a handler instance with this processor.

        Sets the client reference on the handler (via ``handler.set_client``)
        so the handler can make API calls, then adds it to the internal
        ``_handlers`` set. Duplicate registrations are silently ignored.

        Args:
            handler: The handler instance to register. Must be an instance of
                ``self.HANDLER`` (enforced upstream by ``supports_handler``).
        """
        if handler not in self._handlers:
            # set the client to handler
            handler.set_client(self._client)

            self._handlers.add(handler)

    def dispatch(
        self,
        payload
    ) -> Awaitable[None]:
        """Fan out a matched payload to all registered handlers.

        Delegates to ``_dispatch``, which calls ``handler.receiveDispatch``
        on every handler in ``_handlers`` and gathers any resulting coroutines
        concurrently. Subclasses (e.g. ``UserProcessor``) override this to
        route payloads to per-event-type handler sub-sets.

        Args:
            payload: The inner payload dict extracted by ``is_message_type``.

        Returns:
            Awaitable[None]: A coroutine that resolves when all handlers have
            finished processing the payload.
        """
        return self._dispatch(payload, self._handlers)

    async def _dispatch(
        self,
        payload,
        handlers: Set[Handler]
    ):
        coro = []

        for handler in handlers:
            ret = handler.receiveDispatch(payload)
            if inspect.iscoroutine(ret):
                coro.append(ret)

        if len(coro) > 0:
            await asyncio.gather(*coro)
