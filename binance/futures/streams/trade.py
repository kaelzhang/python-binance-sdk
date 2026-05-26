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
# Futures AggTrade
# Confirmed fields (UM + CM identical, 2026-05-26):
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
# (Futures aggTrade does NOT have Spot's 'b' / 'a' buyer/seller order ids)
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

    Shared across USDⓈ-M and COIN-M markets.  Futures aggregate-trade payloads
    differ from Spot: they include ``agg_trade_id``, ``price``, ``quantity``,
    ``first_trade_id``, ``last_trade_id``, ``trade_time``, and ``is_maker``.
    Unlike the Spot variant, they do NOT include buyer/seller order IDs.

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

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = AggTradeHandlerBase
    SUB_TYPE = SubType.AGG_TRADE
    PAYLOAD_TYPE = 'aggTrade'
