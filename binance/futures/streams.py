"""Shared futures stream handlers and processors.

Contains the genuinely market-agnostic parts of the USDⓈ-M and COIN-M futures
stream handling: the base handler classes (column maps + flattening logic) and
their associated processors.

Key verified findings (2026-05-25):
- Both markets share the *same* ``markPriceUpdate`` event type.
- Both markets share the *same* ``forceOrder`` event type and nested ``o`` structure.
- USDⓈ-M markPrice payload includes ``ap`` (mark price moving average); COIN-M does NOT.
- COIN-M forceOrder nested ``o`` includes ``ps`` (pair); USDⓈ-M does NOT.

Therefore the shared base:
- ``MarkPriceHandlerBase``: exposes only the common fields; UM adds ``ap``, CM adds nothing extra.
- ``ForceOrderHandlerBase``: exposes only the common nested ``o`` fields; CM adds ``ps``.

The UM and CM modules provide *market-specific* column maps (via subclasses/overrides) while
inheriting the common ``_receive`` flattening logic from ``ForceOrderHandlerBase``.

Stream docs:
- UM Mark Price: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
- CM Mark Price: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Stream
- UM Force Order: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
- CM Force Order: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Liquidation-Order-Streams
"""

from typing import List, Optional

from binance.core.common.constants import SubType, STREAM_TYPE_MAP
from binance.core.common.types import DictPayload
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor
from binance.core.common.utils import normalize_symbol


# ---------------------------------------------------------------------------
# Shared Mark Price base
# Common fields confirmed present in BOTH UM and CM (2026-05-25):
#   e  event type ('markPriceUpdate')
#   E  event time
#   s  symbol
#   p  mark price
#   i  index price
#   P  estimated settle price
#   r  funding rate
#   T  next funding time
#
# UM-only field: ap (mark price moving average)  — added by UMMarkPriceHandlerBase
# CM lacks 'ap' entirely.
# ---------------------------------------------------------------------------

# Common mark price fields shared by both markets.
MARK_PRICE_COLUMNS_MAP_BASE = {
    **STREAM_TYPE_MAP,        # 'e' -> 'type'
    'E': 'event_time',
    's': 'symbol',
    'p': 'mark_price',
    'i': 'index_price',
    'P': 'est_settle_price',
    'r': 'funding_rate',
    'T': 'next_funding_time',
}


class MarkPriceHandlerBase(Handler):
    """Base handler for the futures ``SubType.MARK_PRICE`` stream.

    Shared across USDⓈ-M (``UMFuturesClient``) and COIN-M (``CMFuturesClient``) markets.
    Covers the common payload fields: mark price, index price, estimated settlement price,
    funding rate, and next funding time.

    USDⓈ-M extends this with ``ap`` (mark price moving average) via the market-specific
    ``COLUMNS_MAP`` override in ``binance.futures.um.streams``.  COIN-M uses this base
    directly (COIN-M does not publish ``ap``).

    Subclass and override ``receive(payload)`` to handle events.  The base ``receive``
    converts the raw dict into a ``StockDataFrame`` with human-readable column names.

    Example (COIN-M)::

        from binance import CMFuturesClient, MarkPriceHandlerBase

        class MyHandler(MarkPriceHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['mark_price'])

        client = CMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.MARK_PRICE, 'btcusd_perp')
    """

    COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP_BASE
    COLUMNS = MARK_PRICE_COLUMNS_MAP_BASE.keys()


# ---------------------------------------------------------------------------
# Shared Force Order (liquidation) base
# Common nested 'o' fields confirmed in BOTH UM and CM (2026-05-25):
#   e  event type ('forceOrder')
#   E  event time
#   o.s   symbol
#   o.S   side
#   o.o   order type
#   o.f   time in force
#   o.q   original quantity
#   o.p   price
#   o.ap  average price
#   o.X   order status
#   o.l   last filled quantity
#   o.z   accumulated filled quantity
#   o.T   order trade time
#
# CM-only nested field: o.ps (pair) — added in CMForceOrderHandlerBase
# UM lacks 'ps'.
# ---------------------------------------------------------------------------

# Common force order fields shared by both markets.
FORCE_ORDER_COLUMNS_MAP_BASE = {
    **STREAM_TYPE_MAP,        # 'e' -> 'type'
    'E': 'event_time',
    's': 'symbol',
    'S': 'side',
    'o': 'order_type',
    'f': 'time_in_force',
    'q': 'orig_quantity',
    'p': 'price',
    'ap': 'avg_price',
    'X': 'order_status',
    'l': 'last_filled_qty',
    'z': 'acc_filled_qty',
    'T': 'trade_time',
}


class ForceOrderHandlerBase(Handler):
    """Base handler for the futures ``SubType.FORCE_ORDER`` (liquidation order) stream.

    Shared across USDⓈ-M (``UMFuturesClient``) and COIN-M (``CMFuturesClient``) markets.
    The raw payload nests order details under an ``'o'`` key.  The internal ``_receive``
    flattens it — merging ``payload['o']`` fields into the top level with ``'e'`` and
    ``'E'`` from the outer payload — so the standard column-mapping applies uniformly.

    COIN-M extends this with ``ps`` (pair) in the nested order object via the market-specific
    ``COLUMNS_MAP`` override in ``binance.futures.cm.streams``.  USDⓈ-M uses the common
    column map directly (USDⓈ-M does not publish ``ps``).

    Subclass and override ``receive(payload)`` to handle events.  The base ``receive``
    converts the raw dict into a ``StockDataFrame`` with human-readable column names.

    Example (COIN-M)::

        from binance import CMFuturesClient, ForceOrderHandlerBase, SubType

        class MyHandler(ForceOrderHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['symbol'], df['price'])

        client = CMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.FORCE_ORDER, 'btcusd_perp')
    """

    COLUMNS_MAP = FORCE_ORDER_COLUMNS_MAP_BASE
    COLUMNS = FORCE_ORDER_COLUMNS_MAP_BASE.keys()

    def _receive(  # type: ignore[override]  # intentional narrowing: only dict payloads (with nested 'o') are valid for this handler
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        """Flatten the nested order object before standard column conversion.

        The raw liquidation payload has the form::

            {
                'e': 'forceOrder',
                'E': <event_time>,
                'o': {
                    's': <symbol>,
                    'S': <side>,
                    ...
                }
            }

        This method merges the 'o' dict into a flat dict (preserving 'e' and
        'E' from the outer payload) so the COLUMNS_MAP applies directly.
        """
        o = payload['o']
        flat = {
            'e': payload['e'],
            'E': payload['E'],
            **o,
        }
        return super()._receive(flat, index)


# ---------------------------------------------------------------------------
# Shared processors
# These are concrete because both markets use the same SubType enum members,
# the same event type strings, and the same subscribe_param logic.
# ---------------------------------------------------------------------------

class MarkPriceProcessor(Processor):
    """Processor for the futures mark-price stream (``<symbol>@markPrice``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = MarkPriceHandlerBase
    SUB_TYPE = SubType.MARK_PRICE
    PAYLOAD_TYPE = 'markPriceUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@markPrice``.

        Accepts an optional second positional argument ``update_speed``:
        pass ``'1s'`` to get the 1-second stream (``<symbol>@markPrice@1s``).
        """
        symbol = self._get_param_symbol(t, args)
        base = f'{normalize_symbol(symbol)}@{SubType.MARK_PRICE}'
        if len(args) >= 2 and args[1] == '1s':
            return f'{base}@1s'
        return base


class ForceOrderProcessor(Processor):
    """Processor for the futures liquidation order stream (``<symbol>@forceOrder``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = ForceOrderHandlerBase
    SUB_TYPE = SubType.FORCE_ORDER
    PAYLOAD_TYPE = 'forceOrder'


__all__ = [
    'MARK_PRICE_COLUMNS_MAP_BASE',
    'FORCE_ORDER_COLUMNS_MAP_BASE',
    'MarkPriceHandlerBase',
    'ForceOrderHandlerBase',
    'MarkPriceProcessor',
    'ForceOrderProcessor',
]
