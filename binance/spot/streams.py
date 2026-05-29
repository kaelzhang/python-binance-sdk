"""Spot stream wiring: the SubType -> processor registry.

Collects the Spot market-data and user-data stream processors (each binding a
handler base class to a :class:`~binance.core.common.constants.SubType`) into
the ``PROCESSORS`` list consumed by
:class:`~binance.core.handlers.context.HandlerContext`. The framework
processors for handler exceptions and stream-control errors are wired
separately on the client (see :mod:`binance.spot.spec`).
"""

from binance.spot.processors import (
    KlineProcessor,
    KlineUTC8Processor,
    TradeProcessor,
    AggTradeProcessor,
    BlockTradeProcessor,
    ReferencePriceProcessor,
    BookTickerProcessor,
    AvgPriceProcessor,
    WindowTickerProcessor,
    OrderBookProcessor,
    PartialOrderBookProcessor,
    MiniTickerProcessor,
    TickerProcessor,
    AllMarketMiniTickersProcessor,
    AllMarketWindowTickersProcessor,
)

from binance.spot.user_processor import UserProcessor


PROCESSORS = [
    KlineProcessor,
    KlineUTC8Processor,
    TradeProcessor,
    AggTradeProcessor,
    BlockTradeProcessor,
    ReferencePriceProcessor,
    BookTickerProcessor,
    AvgPriceProcessor,
    WindowTickerProcessor,
    OrderBookProcessor,
    PartialOrderBookProcessor,
    MiniTickerProcessor,
    TickerProcessor,
    AllMarketMiniTickersProcessor,
    AllMarketWindowTickersProcessor,
    # NOTE: ``AllMarketTickersProcessor`` was REMOVED -- the standalone Spot
    # ``!ticker@arr`` stream is not documented on the Spot WebSocket Streams
    # page.  Futures still ships an ``AllMarketTickersProcessor`` (UM + CM
    # docs explicitly cover ``!ticker@arr``).
    UserProcessor,
]
