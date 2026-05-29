from binance.core.common.types import DictPayload

from .handlers import Handler


# ---------------------------------------------------------------------------
# Spot executionReport COLUMNS_MAP
# Per developers.binance.com (Spot User Data Stream, "executionReport") --
# strict-coverage column map enumerating every documented field, standard
# and conditional, so callers introspecting the map see the full surface.
# Conditional fields are documented as appearing only for certain order
# types (peg, OCO/list, SOR allocations, STP/prevented match, trailing
# stop, etc.); their absence in a given payload is normal.
#
# The 2025-08-12 Spot CHANGELOG added a top-level ``subscriptionId`` to
# user-data events delivered through the WS-API; the SDK preserves and
# surfaces it via the ``subscription_id`` rename so multi-subscription
# routing works.
#
# Docs:
# - https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream
# - https://developers.binance.com/docs/binance-spot-api-docs/CHANGELOG
# ---------------------------------------------------------------------------

EXECUTION_REPORT_COLUMNS_MAP = {
    # Standard fields
    'e': 'type',
    'E': 'event_time',
    's': 'symbol',
    'c': 'client_order_id',
    'S': 'side',
    'o': 'order_type',
    'f': 'time_in_force',
    'q': 'orig_quantity',
    'p': 'orig_price',
    'P': 'stop_price',
    'F': 'iceberg_quantity',
    'g': 'order_list_id',
    'C': 'orig_client_order_id',
    'x': 'execution_type',
    'X': 'order_status',
    'r': 'reject_reason',
    'i': 'order_id',
    'l': 'last_filled_qty',
    'z': 'cumulative_filled_qty',
    'L': 'last_filled_price',
    'n': 'commission_amount',
    'N': 'commission_asset',
    'T': 'transaction_time',
    't': 'trade_id',
    'I': 'execution_id',
    'w': 'is_on_book',
    'm': 'is_maker',
    # `M` is explicitly marked "Ignore" in the docs but IS documented;
    # surfaced as `_ignore_M` so downstream code can see the field exists
    # while knowing it is to be dropped.
    'M': '_ignore_M',
    'O': 'order_creation_time',
    'Z': 'cumulative_quote_qty',
    'Y': 'last_quote_qty',
    'Q': 'quote_order_qty',
    'V': 'stp_mode',
    # Conditional fields (appear only for certain order types per docs)
    'd': 'trailing_delta',
    'D': 'trailing_time',
    'j': 'strategy_id',
    'J': 'strategy_type',
    'v': 'prevented_match_id',
    'A': 'prevented_quantity',
    'B': 'last_prevented_quantity',
    'u': 'trade_group_id',
    'U': 'counter_order_id',
    'Cs': 'counter_symbol',
    'pl': 'prevented_execution_qty',
    'pL': 'prevented_execution_price',
    'pY': 'prevented_execution_quote_qty',
    'W': 'working_time',
    'b': 'match_type',
    'a': 'allocation_id',
    'k': 'working_floor',
    'uS': 'used_sor',
    'gP': 'pegged_price_type',
    'gOT': 'pegged_offset_type',
    'gOV': 'pegged_offset_value',
    'gp': 'pegged_price',
    'eR': 'expiry_reason',
    # Top-level (outside the `event` container) -- added 2025-08-12 per the
    # Spot CHANGELOG; the SDK preserves it on the dispatched payload via
    # ``UserProcessor.is_message_type``.
    'subscriptionId': 'subscription_id',
}


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
    filled, fully filled, cancelled, rejected, or expired.  Per
    developers.binance.com (Spot User Data Stream, "executionReport") the
    payload covers symbol, order IDs, side, type, time-in-force, quantity,
    price, execution type, order status, fill details, peg/SOR/OCO/STP
    metadata, and -- since the 2025-08-12 CHANGELOG -- a top-level
    ``subscriptionId`` that identifies which subscription delivered the
    event when listening through the WS-API.

    The handler applies :data:`EXECUTION_REPORT_COLUMNS_MAP` to rename every
    documented raw single-letter key to a human-readable Python name
    (``e -> 'type'``, ``s -> 'symbol'``, ...) and forwards the renamed dict.
    Unknown keys are preserved unchanged.  The ``M`` field, which docs mark
    as "Ignore", is renamed to ``_ignore_M`` so downstream code can see
    the field exists while knowing it is to be dropped.

    Subclass this and override ``receive(payload)`` to process the event.

    Docs:
    https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream
    """

    COLUMNS_MAP = EXECUTION_REPORT_COLUMNS_MAP

    def receive(self, msg: DictPayload):
        """Return the renamed executionReport dict.

        Each documented key in :data:`EXECUTION_REPORT_COLUMNS_MAP` is
        renamed; unknown keys are passed through as-is so unexpected
        future fields surface to downstream code.
        """
        return {
            self.COLUMNS_MAP.get(k, k): v
            for k, v in msg.items()
        }


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
