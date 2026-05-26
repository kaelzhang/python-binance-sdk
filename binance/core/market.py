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
from typing import List, Tuple, Type

from binance.core.orderbook import OrderBook
from binance.core.processors.base import Processor
from binance.core.rate_limit import RateLimitRule


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
    """

    rest_host: str
    ws_api_host: str
    stream_host: str
    rules: Tuple[RateLimitRule, ...]
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
