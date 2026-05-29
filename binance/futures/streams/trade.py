"""Aggregate trade stream handler and processor.

Hosts ``AggTradeHandlerBase`` and ``AggTradeProcessor`` for the per-symbol
futures aggregate-trade stream.  See :mod:`binance.futures.streams._common`
for the per-stream verified findings.
"""

from binance.core.common.constants import (
    SubType,
    STREAM_TYPE_MAP,
)
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor


# ---------------------------------------------------------------------------
# Futures AggTrade — shared column map
# Common fields per developers.binance.com (UM + CM, 2026-05):
#   e  event type ('aggTrade')
#   E  event time
#   s  symbol
#   a  agg trade id
#   p  price
#   q  quantity
#   f  first trade id
#   l  last trade id
#   T  trade time
#   m  is maker
# USDⓈ-M additionally publishes ``nq`` (normal quantity excluding RPI
# trades); the UM-specific column map (in binance.futures.um.streams) adds
# it.  COIN-M uses this shared base directly.
# Futures aggTrade does NOT include Spot's ``b``/``a`` (buyer/seller order
# id) fields.
# Docs:
# - UM https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
# - CM https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
# ---------------------------------------------------------------------------

FUTURES_AGG_TRADE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    'a': 'agg_trade_id',
    'p': 'price',
    'q': 'quantity',
    'f': 'first_trade_id',
    'l': 'last_trade_id',
    'T': 'trade_time',
    'm': 'is_maker',
}

FUTURES_AGG_TRADE_COLUMNS = FUTURES_AGG_TRADE_COLUMNS_MAP.keys()


class AggTradeHandlerBase(Handler):
    """Base handler for the futures ``SubType.AGG_TRADE`` stream.

    Provides the COIN-M agg-trade column map.  COIN-M uses this base directly.
    USDⓈ-M extends it with the ``nq`` (normal_quantity) column via
    ``binance.futures.um.streams.AggTradeHandlerBase``.

    Subclass this and override ``receive(payload)`` to handle events.  The base
    ``receive`` converts the raw dict into a ``StockDataFrame`` with human-readable
    column names.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
    """

    COLUMNS_MAP = FUTURES_AGG_TRADE_COLUMNS_MAP
    COLUMNS = FUTURES_AGG_TRADE_COLUMNS


class AggTradeProcessor(Processor):
    """Processor for the futures aggregate trade stream (``<symbol>@aggTrade``).

    Bound here to the shared (CM-shaped) base; the UM-specific
    ``AggTradeProcessor`` in ``binance.futures.um.streams`` binds to the UM
    handler that adds ``normal_quantity``.
    """

    HANDLER = AggTradeHandlerBase
    SUB_TYPE = SubType.AGG_TRADE
    PAYLOAD_TYPE = 'aggTrade'
