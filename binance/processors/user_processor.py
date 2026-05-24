"""
Ref:
https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream
"""

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
    """Processor for ``SubType.USER`` — authenticated user-data-stream events.

    Unlike market-data processors, ``UserProcessor`` does not use a single
    ``HANDLER`` class; instead it maps each Binance user-event type to a
    dedicated handler base class via parallel ``PAYLOAD_TYPES`` and
    ``HANDLERS`` tuples. Incoming messages are routed to the handler set for
    their specific ``'e'`` event type.

    Supported event types and their handler bases:

    - ``'outboundAccountInfo'`` — ``AccountInfoHandlerBase``
    - ``'outboundAccountPosition'`` — ``AccountPositionHandlerBase``
    - ``'balanceUpdate'`` — ``BalanceUpdateHandlerBase``
    - ``'executionReport'`` — ``OrderUpdateHandlerBase``
    - ``'listStatus'`` — ``OrderListStatusHandlerBase``
    - ``'externalLockUpdate'`` — ``ExternalLockUpdateHandlerBase``
    - ``'eventStreamTerminated'`` — ``EventStreamTerminatedHandlerBase``

    ``_handlers`` is a ``dict[str, set[Handler]]`` keyed by event-type string,
    overriding the base class's flat ``set``.
    """

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
        """Build the WS-API parameters for subscribing or unsubscribing the user-data stream.

        For subscribe: calls ``client._ws_api_signature_params()`` to obtain
        the signed parameters required by ``userDataStream.subscribe.signature``
        and marks the processor as subscribed.

        For unsubscribe: returns an empty dict (the WS-API
        ``userDataStream.unsubscribe`` method needs no parameters) and marks
        the processor as unsubscribed.

        Args:
            subscribe: ``True`` to subscribe; ``False`` to unsubscribe.
            t: ``SubType.USER`` (unused beyond type identity).

        Returns:
            dict: Signed parameter dict for subscribe, or ``{}`` for
            unsubscribe.

        Raises:
            UserStreamNotSubscribedException: If ``subscribe=False`` is called
                when no user-data stream is currently subscribed.
        """
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
        """Match user-data-stream event messages from either the WS-API or data-stream envelope.

        Checks two envelope shapes:
        - ``msg['event']`` dict — used by the WebSocket API (``wss://ws-api.binance.com``).
        - ``msg['data']`` dict — used by the combined data stream.

        In both cases the inner dict's ``'e'`` field must be one of
        ``PAYLOAD_TYPES``.

        Args:
            msg: Parsed WebSocket JSON dict.

        Returns:
            Tuple[bool, Optional[dict]]: ``(True, event_dict)`` when matched,
            ``(False, None)`` otherwise.
        """
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
        """Return whether the handler is an instance of any of the user-event handler bases.

        Overrides the base class to check against the ``HANDLERS`` tuple
        (multiple handler base classes) rather than a single ``HANDLER``.

        Args:
            handler: Handler instance to inspect.

        Returns:
            bool: ``True`` if the handler matches any entry in ``HANDLERS``.
        """
        return isinstance(handler, self.HANDLERS)

    def add_handler(
        self,
        handler: Handler
    ) -> None:
        """Register a user-event handler, keyed by its corresponding event type.

        Iterates ``HANDLERS`` in parallel with ``PAYLOAD_TYPES`` to find the
        matching event type, then delegates to ``_add_handler`` which buckets
        the handler into the per-event-type set in ``_handlers``.

        Args:
            handler: A handler instance that is an instance of one of the
                ``HANDLERS`` base classes.
        """
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
        """Route a user-data event payload to the handlers registered for its event type.

        Looks up the ``'e'`` field of the payload in the ``_handlers`` dict and
        dispatches to that handler set only, rather than broadcasting to all
        registered handlers.

        Args:
            payload: The matched event dict (already extracted by
                ``is_message_type``), containing at least ``'e'`` (event type).
        """
        payload_type = payload.get(KEY_PAYLOAD_TYPE)
        handlers = self._handlers.get(payload_type)

        if handlers is not None:
            await self._dispatch(payload, handlers)
