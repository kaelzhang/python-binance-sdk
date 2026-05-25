"""USDⓈ-M Futures stream wiring: handlers, processors, and the PROCESSORS list.

Confirmed payload field mappings (2026-05-25) against official Binance docs:
- Mark Price stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
- Liquidation Order stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
"""

from typing import List, Optional

from binance.core.common.constants import SubType, STREAM_TYPE_MAP
from binance.core.common.types import DictPayload
from binance.core.common.utils import normalize_symbol
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor


# ---------------------------------------------------------------------------
# Mark Price stream
# Event 'e': 'markPriceUpdate'
# Stream names: <symbol>@markPrice / <symbol>@markPrice@1s /
#               !markPrice@arr / !markPrice@arr@1s
# Fields confirmed from docs (2026-05-25):
#   e  event type
#   E  event time
#   s  symbol
#   p  mark price
#   ap mark price moving average (note: docs show 'ap', distinct from order field)
#   i  index price
#   P  estimated settle price
#   r  funding rate
#   T  next funding time
# ---------------------------------------------------------------------------

MARK_PRICE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,        # 'e' -> 'type'
    'E': 'event_time',
    's': 'symbol',
    'p': 'mark_price',
    'ap': 'mark_price_avg',
    'i': 'index_price',
    'P': 'est_settle_price',
    'r': 'funding_rate',
    'T': 'next_funding_time',
}

MARK_PRICE_COLUMNS = MARK_PRICE_COLUMNS_MAP.keys()


class MarkPriceHandlerBase(Handler):
    """Base handler for the USDⓈ-M ``SubType.MARK_PRICE`` stream.

    Receives a mark-price update for the subscribed symbol (every 3 s by default,
    or every 1 s with ``<symbol>@markPrice@1s``).  Each payload contains the
    mark price, index price, estimated settlement price, funding rate, and the
    time of the next funding settlement.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``mark_price``, ``funding_rate``,
    ``next_funding_time``).

    Example::

        from binance import UMFuturesClient, MarkPriceHandlerBase

        class MyHandler(MarkPriceHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['mark_price'])

        client = UMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.MARK_PRICE, 'btcusdt')
    """

    COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP
    COLUMNS = MARK_PRICE_COLUMNS


# ---------------------------------------------------------------------------
# Force Order (liquidation) stream
# Event 'e': 'forceOrder'
# Stream names: <symbol>@forceOrder / !forceOrder@arr
# Payload structure (confirmed from docs 2026-05-25):
#   e  event type
#   E  event time
#   o  nested order object:
#      s   symbol
#      S   side
#      o   order type
#      f   time in force
#      q   original quantity
#      p   price
#      ap  average price
#      X   order status
#      l   last filled quantity
#      z   accumulated filled quantity
#      T   order trade time
# ---------------------------------------------------------------------------

# Flattened column map — the nested 'o' dict is merged into the top-level
# payload by _receive() before conversion, exactly like KlineHandlerBase
# flattens the 'k' sub-dict.
FORCE_ORDER_COLUMNS_MAP = {
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

FORCE_ORDER_COLUMNS = FORCE_ORDER_COLUMNS_MAP.keys()


class ForceOrderHandlerBase(Handler):
    """Base handler for the USDⓈ-M ``SubType.FORCE_ORDER`` (liquidation order) stream.

    Receives a liquidation order event whenever a force-liquidation occurs for
    the subscribed symbol.  Binance pushes at most one event per symbol per
    second (the largest liquidation within each 1000 ms window).

    The raw payload nests the order details under an ``'o'`` key.  The internal
    ``_receive`` flattens it — merging ``payload['o']`` fields into the top
    level together with ``'e'`` and ``'E'`` from the outer payload — so the
    standard column-mapping works uniformly.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``symbol``, ``side``, ``price``,
    ``avg_price``, ``order_status``).

    Example::

        from binance import UMFuturesClient, ForceOrderHandlerBase, SubType

        class MyHandler(ForceOrderHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['symbol'], df['price'])

        client = UMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.FORCE_ORDER, 'btcusdt')
    """

    COLUMNS_MAP = FORCE_ORDER_COLUMNS_MAP
    COLUMNS = FORCE_ORDER_COLUMNS

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
# Processors
# ---------------------------------------------------------------------------

class MarkPriceProcessor(Processor):
    """Processor for the USDⓈ-M mark-price stream (``<symbol>@markPrice``)."""

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
    """Processor for the USDⓈ-M liquidation order stream (``<symbol>@forceOrder``)."""

    HANDLER = ForceOrderHandlerBase
    SUB_TYPE = SubType.FORCE_ORDER
    PAYLOAD_TYPE = 'forceOrder'


PROCESSORS = [
    MarkPriceProcessor,
    ForceOrderProcessor,
]
