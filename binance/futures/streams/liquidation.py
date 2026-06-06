"""Liquidation (force-order) stream handlers and processors.

Hosts ``ForceOrderHandlerBase`` (per-symbol, with nested-``o`` flattening) and
``AllMarketLiquidationHandlerBase`` (``!forceOrder@arr``) plus their processors.
See :mod:`binance.futures.streams._common` for the per-stream verified findings.
"""

from typing import ClassVar, List, Optional

from binance.core.common.constants import (
    SubType,
    STREAM_TYPE_MAP,
    KEY_STREAM_TYPE,
    KEY_PAYLOAD,
)
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.common.types import DictPayload
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor


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

    Aggregation semantics: per developers.binance.com (2026-04-10 derivatives
    changelog, effective 2026-04-14), the stream emits ONLY the **largest**
    liquidation order within each 1000ms window per symbol (NOT every event,
    NOT the most recent).  Strategies that need tick-level liquidation
    coverage cannot rely on this stream as a complete feed.

    Subclass and override ``receive(payload)`` to handle events.  The base ``receive``
    converts the raw dict into a ``volas.DataFrame`` with human-readable column names.

    Example (COIN-M)::

        from binance import CMFuturesClient, ForceOrderHandlerBase, SubType

        class MyHandler(ForceOrderHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['symbol'], df['price'])

        client = CMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.FORCE_ORDER, 'btcusd_perp')

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Liquidation-Order-Streams
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
# All-market arrays: AllMarketLiquidation
# Wire stream: !forceOrder@arr
# Each element is a forceOrder payload with nested 'o' (same as per-symbol).
# CM elements in nested 'o' include 'ps' (pair); UM do not.
# The base handler does NOT flatten (array handler receives the list as-is).
# Flattening is done per-element by user code or the subclass.
# ---------------------------------------------------------------------------

class AllMarketLiquidationHandlerBase(Handler):
    """Base handler for the ``SubType.ALL_MARKET_LIQUIDATION`` stream (``!forceOrder@arr``).

    Shared by USDⓈ-M and COIN-M markets.  Receives an array of liquidation order
    events (``forceOrder``) covering all symbols on the market.  Each element has
    the same nested ``o`` structure as the per-symbol ``ForceOrderHandlerBase``.

    The base ``receive`` passes the raw array payload to a ``volas.DataFrame`` with
    only the outer envelope column map (event type and event time per element).
    Subclasses may override to flatten the nested ``o`` objects.

    Aggregation semantics: per developers.binance.com (2026-04-10 derivatives
    changelog, effective 2026-04-14), for each symbol the stream emits ONLY the
    **largest** liquidation order within each 1000ms window (NOT every event,
    NOT the most recent) — same aggregation as the per-symbol ``forceOrder``
    stream.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
    """

    COLUMNS_MAP = STREAM_TYPE_MAP
    COLUMNS = STREAM_TYPE_MAP.keys()


class ForceOrderProcessor(Processor):
    """Processor for the futures liquidation order stream (``<symbol>@forceOrder``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = ForceOrderHandlerBase
    SUB_TYPE = SubType.FORCE_ORDER
    PAYLOAD_TYPE = 'forceOrder'


class AllMarketLiquidationProcessor(Processor):
    """Processor for the futures all-market liquidation stream (``!forceOrder@arr``).

    Routing is by stream name.  Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = AllMarketLiquidationHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_LIQUIDATION
    STREAM_TYPE_NAME: ClassVar[str] = '!forceOrder@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if stream_type == self.STREAM_TYPE_NAME:
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.ALL_MARKET_LIQUIDATION` expects no parameters'
            )
        return self.STREAM_TYPE_NAME
