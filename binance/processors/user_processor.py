from binance.common.constants import (
    SubType,
    KEY_PAYLOAD,
    KEY_PAYLOAD_TYPE
)

from binance.common.exceptions import UserStreamNotSubscribedException

from binance.handlers.user_handlers import (
    AccountInfoHandlerBase,
    AccountPositionHandlerBase,
    BalanceUpdateHandlerBase,
    OrderUpdateHandlerBase,
    OrderListStatusHandlerBase,
    ExternalLockUpdateHandlerBase,
    EventStreamTerminatedHandlerBase
)

from binance.handlers.base import Handler

from .base import Processor


class UserProcessor(Processor):
    SUB_TYPE = SubType.USER

    PAYLOAD_TYPES = (
        'outboundAccountInfo',
        'outboundAccountPosition',
        'balanceUpdate',
        'executionReport',
        'listStatus',
        'externalLockUpdate',
        'eventStreamTerminated'
    )

    HANDLERS = (
        AccountInfoHandlerBase,
        AccountPositionHandlerBase,
        BalanceUpdateHandlerBase,
        OrderUpdateHandlerBase,
        OrderListStatusHandlerBase,
        ExternalLockUpdateHandlerBase,
        EventStreamTerminatedHandlerBase
    )

    def __init__(self, *args) -> None:
        super().__init__(*args)

        self._subscribed = False

        self._handlers = {}

    async def subscribe_param(
        self,
        subscribe: bool,
        t: SubType
    ) -> dict:
        if not subscribe:
            if not self._subscribed:
                raise UserStreamNotSubscribedException()

            self._subscribed = False
            return {}

        # New user stream flow uses WebSocket API.
        params = self._client._ws_api_signature_params()
        self._subscribed = True
        return params

    def is_message_type(self, msg):
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
        payload_type,
        handler
    ) -> None:
        handlers = self._handlers.get(payload_type)

        if handlers is None:
            handlers = set()
            self._handlers[payload_type] = handlers

        if handler not in handlers:
            # set the client to handler
            handler.set_client(self._client)

            handlers.add(handler)

    async def dispatch(self, payload) -> None:
        payload_type = payload.get(KEY_PAYLOAD_TYPE)
        handlers = self._handlers.get(payload_type)

        if handlers is None:
            return

        await self._dispatch(payload, handlers)
