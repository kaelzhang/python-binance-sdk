"""The Spot market client."""

from typing import ClassVar

from binance.core.client_base import BaseClient
from binance.core.market import MarketSpec
from binance.spot.endpoints import WsApiGetters
from binance.spot.spec import SPOT_MARKET
from binance.spot.streams import PROCESSORS
from binance.spot.processors import (
    ExceptionProcessor,
    StreamErrorProcessor,
)


class SpotClient(BaseClient, WsApiGetters):  # type: ignore[misc]  # diamond mixin: _ws_api_request is a Callable hint in WsApiGetters and an actual method in SubscriptionManager; compatible at runtime
    """Async Binance Spot REST + WebSocket-API client.

    Binds the Spot :class:`~binance.core.market.MarketSpec` (hosts, rate-limit
    rules, stream processors) onto the shared
    :class:`~binance.core.client_base.BaseClient` and mixes in ``WsApiGetters``,
    the generated async methods for every Spot WebSocket-API endpoint --
    general (``get_server_time``, ``get_exchange_info``), market-data
    (``get_orderbook``, ``get_klines``, ``get_ticker``, ...), account
    (``get_account``, ``get_commission``, ...) and trading (``create_order``,
    ``cancel_order``, ``create_oco``, ...).

    Construct with an optional :class:`~binance.core.auth.Credentials`::

        from binance import SpotClient, Credentials

        client = SpotClient(Credentials(api_key='KEY', api_secret='SECRET'))

        info = await client.get_exchange_info()       # WS-API coroutine

        client.handler(on_trade)                       # attach a handler
        await client.subscribe('btcusdt@trade')        # subscribe a stream

        snap = client.rate_limit_snapshot()            # local, no network

    See :class:`~binance.core.client_base.BaseClient` for the full constructor
    keyword arguments.
    """

    # The Spot market this client speaks to (hosts / rules / endpoints).
    MARKET: ClassVar[MarketSpec] = SPOT_MARKET

    # The Spot processor set, injected into the HandlerContext.
    PROCESSORS = PROCESSORS
    EXCEPTION_PROCESSOR = ExceptionProcessor
    STREAM_ERROR_PROCESSOR = StreamErrorProcessor
