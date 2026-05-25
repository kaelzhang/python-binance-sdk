"""Market description.

A :class:`MarketSpec` is the small bundle of facts a market (Spot, USDⓈ-M
Futures, …) hands to the shared :class:`~binance.core.client_base.BaseClient`:
the connection hosts, the endpoint registry used to install the market's getter
methods, the stream processor set (SubType routing + the framework
exception / stream-error processors), and the market's default rate-limit rules.
"""

from dataclasses import dataclass, field
from typing import List, Tuple, Type

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
    """

    rest_host: str
    ws_api_host: str
    stream_host: str
    rules: Tuple[RateLimitRule, ...]
    processors: List[Type[Processor]]
    exception_processor: Type[Processor]
    stream_error_processor: Type[Processor]
    endpoints: List[dict] = field(default_factory=list)
