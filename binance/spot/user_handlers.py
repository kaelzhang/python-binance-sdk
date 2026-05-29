from binance.core.common.types import DictPayload

from .handlers import Handler


class SimpleHandler(Handler):
    """A pass-through handler that returns the raw payload dict unchanged.

    Used as the base for all user-data-stream handlers.  Unlike the market-data
    handlers, user-stream payloads are not converted to a ``StockDataFrame``;
    the raw Binance API dict is passed directly to ``receive``.
    """

    def receive(self, msg: DictPayload):
        """Return the raw user-stream payload without transformation.

        Args:
            msg (dict): The raw user-data-stream event payload received from
                Binance.

        Returns:
            dict: The same ``msg`` dict, unmodified.
        """
        return msg


class AccountPositionHandlerBase(SimpleHandler):
    """Base handler for the ``outboundAccountPosition`` user-data-stream event.

    Receives the current account balances for all assets that changed whenever
    an order is placed, filled, or cancelled, or whenever funds are deposited
    or withdrawn.  This is the recommended way to track balance changes.

    Subclass this and override ``receive(payload)`` to process the event.
    The raw Binance payload dict is passed to ``receive`` unchanged.
    """

    pass


class BalanceUpdateHandlerBase(SimpleHandler):
    """Base handler for the ``balanceUpdate`` user-data-stream event.

    Receives a balance-delta event whenever funds are deposited to or
    withdrawn from the account (i.e. transfers, not trading fills).  Each
    payload identifies the asset, the balance delta, and the clear time.

    Subclass this and override ``receive(payload)`` to process the event.
    The raw Binance payload dict is passed to ``receive`` unchanged.
    """

    pass


class OrderUpdateHandlerBase(SimpleHandler):
    """Base handler for the ``executionReport`` user-data-stream event.

    Receives an order-lifecycle update whenever an order is created, partially
    filled, fully filled, cancelled, rejected, or expired.  Each payload
    contains the full order state including symbol, order ID, side, type,
    time-in-force, quantity, price, execution type, order status, and fill
    details.

    Subclass this and override ``receive(payload)`` to process the event.
    The raw Binance payload dict is passed to ``receive`` unchanged.
    """

    pass


class OrderListStatusHandlerBase(SimpleHandler):
    """Base handler for the ``listStatus`` user-data-stream event.

    Receives a status update for an OCO (One-Cancels-the-Other) order list.
    The payload describes the list's current status (e.g. ``EXEC_STARTED``,
    ``ALL_DONE``), the list order ID, and the component orders within it.

    Subclass this and override ``receive(payload)`` to process the event.
    The raw Binance payload dict is passed to ``receive`` unchanged.
    """

    pass


class ExternalLockUpdateHandlerBase(SimpleHandler):
    """Base handler for the ``externalLockUpdate`` user-data-stream event.

    Receives a notification when an external lock on an asset changes (for
    example, when margin collateral is locked or released by an external
    system).  The payload contains the affected asset and the updated locked
    quantity.

    Subclass this and override ``receive(payload)`` to process the event.
    The raw Binance payload dict is passed to ``receive`` unchanged.
    """

    pass


class EventStreamTerminatedHandlerBase(SimpleHandler):
    """Base handler for the ``eventStreamTerminated`` event.

    Server-pushed by Binance on the spot WS-API user-data stream when the
    user-data subscription is closed (``userDataStream.unsubscribe``), the
    session is logged out, or the listen-token expires.  This event is
    emitted by Binance itself — the SDK does NOT synthesize it.

    After receiving this event, the SDK automatically attempts to
    re-establish the user-data subscription on the same WS-API connection
    (see ``_recover_user_stream_if_needed`` in
    ``binance.core.transport.subscription``).  Override ``receive(payload)``
    to log or react to the disconnect.

    Source: https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream

    The raw payload dict is passed to ``receive`` unchanged.
    """

    pass
