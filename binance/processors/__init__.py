from .processors import (
    KlineProcessor,
    KlineUTC8Processor,
    TradeProcessor,
    AggTradeProcessor,
    BlockTradeProcessor,
    BookTickerProcessor,
    AvgPriceProcessor,
    WindowTickerProcessor,
    OrderBookProcessor,
    PartialOrderBookProcessor,
    MiniTickerProcessor,
    TickerProcessor,
    AllMarketMiniTickersProcessor,
    AllMarketWindowTickersProcessor,
    ExceptionProcessor,
    StreamErrorProcessor
)

from .user_processor import UserProcessor


PROCESSORS = [
    KlineProcessor,
    KlineUTC8Processor,
    TradeProcessor,
    AggTradeProcessor,
    BlockTradeProcessor,
    BookTickerProcessor,
    AvgPriceProcessor,
    WindowTickerProcessor,
    OrderBookProcessor,
    PartialOrderBookProcessor,
    MiniTickerProcessor,
    TickerProcessor,
    AllMarketMiniTickersProcessor,
    AllMarketWindowTickersProcessor,
    UserProcessor
]
