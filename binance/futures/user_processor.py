"""Shared USDⓈ-M and COIN-M Futures user-data-stream processor.

``FuturesUserProcessor`` mirrors the Spot ``UserProcessor`` (in
``binance.spot.user_processor``) but routes the five event types specific to
the futures user-data stream.  Both UM and CM clients include this processor
in their ``PROCESSORS`` list so ``subscribe(SubType.USER)`` delivers events to
the appropriate handler bases.

Event types and their handler mappings are confirmed from the official
USDⓈ-M Futures user-data-stream documentation (2026-05-25).

Ref:
https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams
"""

from binance.core.common.constants import (
    SubType,
    KEY_PAYLOAD,
    KEY_PAYLOAD_TYPE,
    EVENT_STREAM_TERMINATED,
)

from binance.core.common.exceptions import UserStreamNotSubscribedException

from binance.futures.user_handlers import (
    FuturesAccountUpdateHandlerBase,
    FuturesOrderUpdateHandlerBase,
    FuturesMarginCallHandlerBase,
    FuturesAccountConfigUpdateHandlerBase,
    FuturesListenKeyExpiredHandlerBase,
    FuturesEventStreamTerminatedHandlerBase,
)

from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor


class FuturesUserProcessor(Processor):
    """Processor for the authenticated futures user-data stream.

    Routes each futures event type to its own handler set.  Mirrors the Spot
    ``UserProcessor`` interface so the shared ``_subscribe_user_only`` core
    machinery works unchanged for both Spot and futures clients.

    The five routed event types (confirmed from USDⓈ-M Futures docs, 2026-05-25):

    - ``ACCOUNT_UPDATE``:          balance + position update
    - ``ORDER_TRADE_UPDATE``:      order lifecycle event
    - ``MARGIN_CALL``:             margin-ratio warning
    - ``ACCOUNT_CONFIG_UPDATE``:   leverage or multi-assets-mode change
    - ``listenKeyExpired``:        listen-key expiry notification
    - ``eventStreamTerminated``:   SDK-synthesized stream-termination sentinel
    """

    SUB_TYPE = SubType.USER

    PAYLOAD_TYPES = (
        'ACCOUNT_UPDATE',
        'ORDER_TRADE_UPDATE',
        'MARGIN_CALL',
        'ACCOUNT_CONFIG_UPDATE',
        'listenKeyExpired',
        EVENT_STREAM_TERMINATED,
    )

    HANDLERS = (
        FuturesAccountUpdateHandlerBase,
        FuturesOrderUpdateHandlerBase,
        FuturesMarginCallHandlerBase,
        FuturesAccountConfigUpdateHandlerBase,
        FuturesListenKeyExpiredHandlerBase,
        FuturesEventStreamTerminatedHandlerBase,
    )

    def __init__(self, *args) -> None:
        super().__init__(*args)

        self._subscribed = False

        self._handlers: dict = {}

    def subscribe_param(  # type: ignore[override]
        self,
        subscribe: bool,
        t: SubType
    ) -> dict:
        """Track subscription state; return ``{}`` (futures listenKey flow is handled by the mixin).

        The ``FuturesUserStreamMixin`` overrides ``_subscribe_user_only`` and
        calls ``userDataStream.start/ping/stop`` directly via ``_ws_api_request``
        (security: ``USER_STREAM``, weight 1). This method only tracks whether
        the user stream is currently subscribed so that
        ``UserStreamNotSubscribedException`` is raised on a spurious unsubscribe.
        """
        if not subscribe:
            if not self._subscribed:
                raise UserStreamNotSubscribedException()

            self._subscribed = False
            return {}

        self._subscribed = True
        return {}

    def is_message_type(self, msg):
        """Match futures user-event messages from either the WS-API or data-stream envelope."""
        event = msg.get('event')
        if (
            event is not None
            and type(event) is dict
            and event.get(KEY_PAYLOAD_TYPE) in self.PAYLOAD_TYPES
        ):
            return True, event

        payload = msg.get(KEY_PAYLOAD)

        if (
            payload is not None
            and type(payload) is dict
            and payload.get(KEY_PAYLOAD_TYPE) in self.PAYLOAD_TYPES
        ):
            return True, payload

        return False, None

    def supports_handler(
        self,
        handler: Handler
    ) -> bool:
        return isinstance(handler, self.HANDLERS)

    def add_handler(
        self,
        handler: Handler
    ) -> None:
        for i, HandlerBase in enumerate(self.HANDLERS):
            if isinstance(handler, HandlerBase):
                payload_type = self.PAYLOAD_TYPES[i]
                self._add_handler(payload_type, handler)

    def _add_handler(
        self,
        payload_type: str,
        handler: Handler
    ) -> None:
        handlers = self._handlers.get(payload_type)

        if handlers is None:
            handlers = set()
            self._handlers[payload_type] = handlers

        if handler not in handlers:
            handler.set_client(self._client)
            handlers.add(handler)

    async def dispatch(self, payload) -> None:
        """Route the payload to the handler set for its ``'e'`` event type only."""
        payload_type = payload.get(KEY_PAYLOAD_TYPE)
        handlers = self._handlers.get(payload_type)

        if handlers is not None:
            await self._dispatch(payload, handlers)
