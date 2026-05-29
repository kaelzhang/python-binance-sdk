"""Market description.

A :class:`MarketSpec` is the small bundle of facts a market (Spot, USDⓈ-M
Futures, …) hands to the shared :class:`~binance.core.client_base.BaseClient`:
the connection hosts, the endpoint registry used to install the market's getter
methods, the stream processor set (SubType routing + the framework
exception / stream-error processors), the market's default rate-limit rules,
and the concrete :class:`~binance.core.orderbook.OrderBook` subclass used by
:class:`~binance.core.handlers.orderbook.OrderBookHandlerBase` to build the
local order book for that venue.
"""

from dataclasses import dataclass, field
from typing import Callable, List, Tuple, Type

from binance.core.orderbook import OrderBook
from binance.core.processors.base import Processor
from binance.core.rate_limit import RateLimitRule


def _default_data_stream_router(_stream_name: str) -> str:
    """Default router for markets with a single ``/stream`` connection.

    Spot and COIN-M both use one data-stream connection at
    ``<stream_host>/stream`` for every subscription, so the router returns the
    legacy ``/stream`` path regardless of the stream name.
    """
    return '/stream'


@dataclass(frozen=True)
class MarketSpec:
    """Immutable description of a single Binance market.

    Attributes:
        rest_host: REST API base host (e.g. ``https://api.binance.com``).
        ws_api_host: WebSocket-API base host (e.g.
            ``wss://ws-api.binance.com/ws-api/v3``).
        stream_host: market-data stream base host (e.g.
            ``wss://stream.binance.com``).
        rules: the market's default rate-limit rules; a per-client
            :class:`~binance.core.rate_limit.RateLimiter` is built from these.
        ws_message_rule: the per-connection ws-messages rule for this market.
            Spot caps incoming messages at 5/s, futures at 10/s (per
            ``developers.binance.com``); each market builds its rule with
            :func:`~binance.core.rate_limit.defaults.build_ws_message_rule`
            and the value from its own constants module.
        processors: the subtype stream-processor classes (SubType routing).
        exception_processor: the processor class for handler exceptions.
        stream_error_processor: the processor class for stream-control errors.
        endpoints: the endpoint registry passed to
            :func:`~binance.core.getters.define_getter` to install the market's
            getter methods. Each entry is a ``dict`` of getter settings.
        orderbook_impl: the concrete :class:`~binance.core.orderbook.OrderBook`
            subclass used by
            :class:`~binance.core.handlers.orderbook.OrderBookHandlerBase` to
            build the local order book.  Each market supplies its own
            implementation (e.g. ``SpotOrderBook`` for spot,
            ``FuturesOrderBook`` for USDⓈ-M / COIN-M futures).  Defaults to the
            abstract :class:`~binance.core.orderbook.OrderBook` as a sentinel:
            constructing it raises :class:`TypeError`, surfacing markets that
            forgot to override.
        data_stream_paths: the path(s) appended to ``stream_host`` for
            market-data subscriptions.  Spot and COIN-M open a single
            connection at ``/stream``; USDⓈ-M splits subscriptions across
            ``/public/stream`` (depth + bookTicker + rpiDepth) and
            ``/market/stream`` (aggTrade, markPrice, klines, tickers,
            liquidations, compositeIndex, contractInfo, assetIndex,
            tradingSession) -- per the 2026-04-23 decommission notice.
            Defaults to ``('/stream',)``.
        data_stream_router: a callable mapping a stream name (e.g.
            ``'btcusdt@depth'``) to its path key.  The returned value MUST be
            one of the strings in ``data_stream_paths``.  Defaults to
            :func:`_default_data_stream_router` which returns ``'/stream'``.
        user_stream_path_template: the path template for the dedicated
            listenKey-based user-data stream (futures only).  Spot does NOT
            open a per-listenKey stream so the value is unused there.  USDⓈ-M
            uses ``'/private/ws/{listen_key}'`` after the 2026-04-23
            decommission; COIN-M keeps the legacy ``'/ws/{listen_key}'``.
            The placeholder is the literal text ``{listen_key}``.
    """

    rest_host: str
    ws_api_host: str
    stream_host: str
    rules: Tuple[RateLimitRule, ...]
    ws_message_rule: RateLimitRule
    processors: List[Type[Processor]]
    exception_processor: Type[Processor]
    stream_error_processor: Type[Processor]
    endpoints: List[dict] = field(default_factory=list)
    # The default is the abstract base itself (used as a sentinel "not
    # overridden"); concrete markets supply their own subclass.  Mypy would
    # normally reject assigning an abstract class to a ``Type[OrderBook]``,
    # but the sentinel value is intentional and surfaces a clear ``TypeError``
    # at first instantiation if a market forgets to override.
    orderbook_impl: Type[OrderBook] = OrderBook  # type: ignore[type-abstract]
    # Data-stream path layout — see attribute docs.  Defaults model Spot/CM.
    data_stream_paths: Tuple[str, ...] = ('/stream',)
    data_stream_router: Callable[[str], str] = field(
        default=_default_data_stream_router
    )
    # Futures user-data fstream path template — see attribute docs.
    user_stream_path_template: str = '/ws/{listen_key}'
