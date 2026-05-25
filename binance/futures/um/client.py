"""The USDⓈ-M Futures market client."""

from typing import ClassVar

from binance.core.client_base import BaseClient
from binance.core.market import MarketSpec
from binance.futures.um.endpoints import UMFuturesGetters
from binance.futures.um.spec import UM_MARKET
from binance.futures.um.streams import PROCESSORS
from binance.futures.um.processors import (
    ExceptionProcessor,
    StreamErrorProcessor,
)
from binance.futures.user_stream import FuturesUserStreamMixin


class UMFuturesClient(FuturesUserStreamMixin, BaseClient, UMFuturesGetters):  # type: ignore[misc]  # diamond mixin: compatible at runtime
    """Async Binance USDⓈ-M Futures market-data client.

    Binds the USDⓈ-M Futures :class:`~binance.core.market.MarketSpec` (hosts,
    rate-limit rules, stream processors) onto the shared
    :class:`~binance.core.client_base.BaseClient` and mixes in
    ``UMFuturesGetters``, the generated async REST methods for every USDⓈ-M
    Futures market-data endpoint -- open interest (current and historical),
    funding rate history, funding info, and mark price / premium index.

    This phase implements **read-only market-data only** (no order placement or
    account endpoints). An optional :class:`~binance.core.auth.Credentials`
    is accepted for symmetry with :class:`~binance.spot.client.SpotClient`
    but is not required for public market-data access.

    Construct with an optional :class:`~binance.core.auth.Credentials`::

        from binance import UMFuturesClient

        client = UMFuturesClient()

        oi = await client.get_open_interest(symbol='BTCUSDT')
        fr = await client.get_funding_rate(symbol='BTCUSDT', limit=10)
        mp = await client.get_premium_index(symbol='BTCUSDT')

        client.handler(on_mark_price)
        await client.subscribe(SubType.MARK_PRICE, 'btcusdt')

        snap = client.rate_limit_snapshot()  # local, no network

    See :class:`~binance.core.client_base.BaseClient` for the full constructor
    keyword arguments.
    """

    # The USDⓈ-M Futures market this client speaks to (hosts / rules / endpoints).
    MARKET: ClassVar[MarketSpec] = UM_MARKET

    # The USDⓈ-M Futures processor set, injected into the HandlerContext.
    PROCESSORS = PROCESSORS
    EXCEPTION_PROCESSOR = ExceptionProcessor
    STREAM_ERROR_PROCESSOR = StreamErrorProcessor
