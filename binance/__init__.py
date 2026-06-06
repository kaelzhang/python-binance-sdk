__version__ = '6.0.0'

from aioretry import (
    RetryPolicy,
    RetryPolicyStrategy,
    RetryInfo
)
from volas import TimeFrame

from binance.spot import SpotClient
from binance.futures.um import UMFuturesClient
from binance.futures.cm import CMFuturesClient
from binance.core.auth import Credentials
from binance.core.common.constants import (
    SubType,
    SecurityType,
    RequestMethod,
    OrderSide,
    OrderType,
    OrderRespType,
    TimeInForce
)

from binance.core.common.exceptions import (
    UserStreamNotSubscribedException,
    StreamDisconnectedException,
    StreamSubscribeException,
    APIKeyNotDefinedException,
    APISecretNotDefinedException,
    StatusException,
    RateLimitException,
    RateLimitReachedException,
    IPBannedException,
    TooManyStreamsException,
    StreamRateLimitException,
    InvalidResponseException,
    InvalidSubParamsException,
    UnsupportedSubTypeException,
    InvalidSubTypeParamException,
    InvalidHandlerException,
    ReuseHandlerException,
    OrderBookFetchAbandonedException
)

from binance.core.rate_limit import (
    RateLimiter,
    RateLimitSnapshot,
    RateLimitWindow
)

from binance.core.common.types import StreamError, StreamName, StreamErrorPhase

from binance.core.handlers.framework import (
    StreamErrorHandlerBase,
    HandlerExceptionHandlerBase,
)

from binance.spot.handlers import (
    TradeHandlerBase,
    AggTradeHandlerBase,
    BlockTradeHandlerBase,
    ReferencePriceHandlerBase,
    BookTickerHandlerBase,
    PartialOrderBookHandlerBase,
    AvgPriceHandlerBase,
    WindowTickerHandlerBase,
    KlineHandlerBase,
    MiniTickerHandlerBase,
    TickerHandlerBase,
    AllMarketMiniTickersHandlerBase,
    AllMarketWindowTickersHandlerBase,
    # NOTE: Spot ``AllMarketTickersHandlerBase`` was REMOVED -- the standalone
    # Spot ``!ticker@arr`` stream is not documented on
    # https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams.
    # For all-market full 24hr tickers on Spot, use
    # ``AllMarketWindowTickersHandlerBase`` (rolling window) instead.
    # Futures `!ticker@arr` IS documented and is still re-exported below as
    # ``FuturesAllMarketTickersHandlerBase``.
)

from binance.core.handlers.orderbook import OrderBookHandlerBase

from binance.spot.user_handlers import (
    AccountPositionHandlerBase,
    BalanceUpdateHandlerBase,
    OrderUpdateHandlerBase,
    OrderListStatusHandlerBase,
    ExternalLockUpdateHandlerBase,
    EventStreamTerminatedHandlerBase
)

from binance.core.orderbook import OrderBook
from binance.core.transport.stream import Stream

from binance.futures.um.streams import (
    MarkPriceHandlerBase,
    ForceOrderHandlerBase,
    # UM-only handler bases
    AllMarketMarkPriceHandlerBase,
    CompositeIndexHandlerBase,
    AssetIndexHandlerBase,
    AllAssetIndexHandlerBase,
    TradingSessionHandlerBase,
    UMRpiDepthHandlerBase,
)

from binance.futures.streams import (
    # Shared futures handler bases
    AggTradeHandlerBase as FuturesAggTradeHandlerBase,
    KlineHandlerBase as FuturesKlineHandlerBase,
    MiniTickerHandlerBase as FuturesMiniTickerHandlerBase,
    TickerHandlerBase as FuturesTickerHandlerBase,
    BookTickerHandlerBase as FuturesBookTickerHandlerBase,
    PartialOrderBookHandlerBase as FuturesPartialOrderBookHandlerBase,
    ContinuousKlineHandlerBase as FuturesContinuousKlineHandlerBase,
    ContractInfoHandlerBase as FuturesContractInfoHandlerBase,
    AllMarketMarkPriceHandlerBase as FuturesAllMarketMarkPriceHandlerBase,
    AllMarketLiquidationHandlerBase as FuturesAllMarketLiquidationHandlerBase,
    AllMarketMiniTickersHandlerBase as FuturesAllMarketMiniTickersHandlerBase,
    AllMarketTickersHandlerBase as FuturesAllMarketTickersHandlerBase,
    AllMarketBookTickerHandlerBase as FuturesAllMarketBookTickerHandlerBase,
)

from binance.futures.cm.streams import (
    ForceOrderHandlerBase as CMForceOrderHandlerBase,
    PartialOrderBookHandlerBase as CMPartialOrderBookHandlerBase,
    OrderBookHandlerBase as CMOrderBookHandlerBase,
    CMPairMarkPriceHandlerBase,
    IndexPriceHandlerBase,
    IndexPriceKlineHandlerBase,
    MarkPriceKlineHandlerBase,
)

from binance.futures.user_handlers import (
    FuturesAccountUpdateHandlerBase,
    FuturesOrderUpdateHandlerBase,
    FuturesMarginCallHandlerBase,
    FuturesAccountConfigUpdateHandlerBase,
    FuturesListenKeyExpiredHandlerBase,
    FuturesTradeLiteHandlerBase,
    FuturesStrategyUpdateHandlerBase,
    FuturesAlgoUpdateHandlerBase,
)

from binance.futures.enums import (
    PositionSide,
    FuturesOrderType,
    WorkingType,
    MarginType,
    FuturesTimeInForce,
)
