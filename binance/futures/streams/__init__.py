"""Shared futures stream handlers and processors.

Public surface preserves the pre-split flat-module imports.  This package groups
the genuinely market-agnostic parts of the USDⓈ-M and COIN-M futures stream
handling into per-stream-family sub-modules:

- :mod:`._common`        — verified per-stream findings and shared depth helpers.
- :mod:`.mark_price`     — per-symbol and all-market mark price.
- :mod:`.trade`          — aggregate trade.
- :mod:`.liquidation`    — per-symbol and all-market force-order (liquidation).
- :mod:`.kline`          — per-symbol kline and continuous-contract kline.
- :mod:`.ticker`         — mini-ticker, ticker, and their all-market variants.
- :mod:`.book`           — book-ticker, partial depth, diff depth, all-market book ticker.
- :mod:`.contract_info`  — ``!contractInfo`` stream.

USDⓈ-M (:mod:`binance.futures.um.streams`) and COIN-M
(:mod:`binance.futures.cm.streams`) import the bases from this package and
extend them with market-specific column maps / processor overrides.
"""

# Depth constants and helpers (shared by partial-depth and diff-depth processors)
from binance.futures.streams._common import (
    FUTURES_DEPTH_LEVELS,
    FUTURES_DEPTH_SPEEDS,
    _get_futures_depth_level,
    _get_futures_depth_speed,
)

# Mark price (per-symbol and all-market)
from binance.futures.streams.mark_price import (
    MARK_PRICE_COLUMNS_MAP_BASE,
    MarkPriceHandlerBase,
    AllMarketMarkPriceHandlerBase,
    MarkPriceProcessor,
    AllMarketMarkPriceProcessor,
)

# Aggregate trade
from binance.futures.streams.trade import (
    FUTURES_AGG_TRADE_COLUMNS_MAP,
    AggTradeHandlerBase,
    AggTradeProcessor,
)

# Force order / liquidation (per-symbol and all-market)
from binance.futures.streams.liquidation import (
    FORCE_ORDER_COLUMNS_MAP_BASE,
    ForceOrderHandlerBase,
    AllMarketLiquidationHandlerBase,
    ForceOrderProcessor,
    AllMarketLiquidationProcessor,
)

# Kline and continuous kline
from binance.futures.streams.kline import (
    FUTURES_KLINE_COLUMNS_MAP,
    FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP,
    VALID_FUTURES_KLINE_INTERVALS,
    VALID_CONTRACT_TYPES,
    KlineHandlerBase,
    ContinuousKlineHandlerBase,
    KlineProcessor,
    ContinuousKlineProcessor,
)

# Mini ticker, ticker, and their all-market variants
from binance.futures.streams.ticker import (
    FUTURES_MINI_TICKER_COLUMNS_MAP,
    FUTURES_TICKER_COLUMNS_MAP,
    MiniTickerHandlerBase,
    TickerHandlerBase,
    AllMarketMiniTickersHandlerBase,
    AllMarketTickersHandlerBase,
    MiniTickerProcessor,
    TickerProcessor,
    AllMarketMiniTickersProcessor,
    AllMarketTickersProcessor,
)

# Book ticker, partial depth, diff depth (per-symbol and all-market book ticker)
from binance.futures.streams.book import (
    FUTURES_BOOK_TICKER_COLUMNS_MAP,
    FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP,
    FUTURES_ORDER_BOOK_COLUMNS_MAP,
    BookTickerHandlerBase,
    PartialOrderBookHandlerBase,
    OrderBookHandlerBase,
    AllMarketBookTickerHandlerBase,
    BookTickerProcessor,
    PartialOrderBookProcessor,
    OrderBookProcessor,
    AllMarketBookTickerProcessor,
)

# Contract info
from binance.futures.streams.contract_info import (
    CONTRACT_INFO_COLUMNS_MAP,
    ContractInfoHandlerBase,
    ContractInfoProcessor,
)


__all__ = [
    # Column maps
    'MARK_PRICE_COLUMNS_MAP_BASE',
    'FORCE_ORDER_COLUMNS_MAP_BASE',
    'FUTURES_AGG_TRADE_COLUMNS_MAP',
    'FUTURES_KLINE_COLUMNS_MAP',
    'FUTURES_MINI_TICKER_COLUMNS_MAP',
    'FUTURES_TICKER_COLUMNS_MAP',
    'FUTURES_BOOK_TICKER_COLUMNS_MAP',
    'FUTURES_PARTIAL_ORDER_BOOK_COLUMNS_MAP',
    'FUTURES_ORDER_BOOK_COLUMNS_MAP',
    'FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP',
    'CONTRACT_INFO_COLUMNS_MAP',
    # Constants
    'FUTURES_DEPTH_LEVELS',
    'FUTURES_DEPTH_SPEEDS',
    'VALID_FUTURES_KLINE_INTERVALS',
    'VALID_CONTRACT_TYPES',
    # Handler bases
    'MarkPriceHandlerBase',
    'ForceOrderHandlerBase',
    'AggTradeHandlerBase',
    'KlineHandlerBase',
    'MiniTickerHandlerBase',
    'TickerHandlerBase',
    'BookTickerHandlerBase',
    'PartialOrderBookHandlerBase',
    'OrderBookHandlerBase',
    'ContinuousKlineHandlerBase',
    'ContractInfoHandlerBase',
    'AllMarketMarkPriceHandlerBase',
    'AllMarketLiquidationHandlerBase',
    'AllMarketMiniTickersHandlerBase',
    'AllMarketTickersHandlerBase',
    'AllMarketBookTickerHandlerBase',
    # Processors
    'MarkPriceProcessor',
    'ForceOrderProcessor',
    'AggTradeProcessor',
    'KlineProcessor',
    'MiniTickerProcessor',
    'TickerProcessor',
    'BookTickerProcessor',
    'PartialOrderBookProcessor',
    'OrderBookProcessor',
    'ContinuousKlineProcessor',
    'ContractInfoProcessor',
    'AllMarketMarkPriceProcessor',
    'AllMarketLiquidationProcessor',
    'AllMarketMiniTickersProcessor',
    'AllMarketTickersProcessor',
    'AllMarketBookTickerProcessor',
    # Depth helpers (used by CM streams package internally)
    '_get_futures_depth_level',
    '_get_futures_depth_speed',
]
