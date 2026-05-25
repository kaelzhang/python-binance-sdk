"""The COIN-M Futures market client."""

from typing import ClassVar

from binance.core.client_base import BaseClient
from binance.core.market import MarketSpec
from binance.futures.cm.endpoints import CMFuturesGetters
from binance.futures.cm.spec import CM_MARKET
from binance.futures.cm.streams import PROCESSORS
from binance.futures.cm.processors import (
    ExceptionProcessor,
    StreamErrorProcessor,
)


class CMFuturesClient(BaseClient, CMFuturesGetters):  # type: ignore[misc]  # diamond mixin: compatible at runtime
    """Async Binance COIN-M (coin-margined) Futures market-data client.

    Binds the COIN-M Futures :class:`~binance.core.market.MarketSpec` (hosts,
    rate-limit rules, stream processors) onto the shared
    :class:`~binance.core.client_base.BaseClient` and mixes in
    ``CMFuturesGetters``, the generated async REST methods for every COIN-M
    Futures market-data endpoint -- open interest (current and historical),
    funding rate history, funding info, and mark price / premium index.

    This phase implements **read-only market-data only** (no order placement or
    account endpoints). An optional :class:`~binance.core.auth.Credentials`
    is accepted for symmetry with other clients but is not required for public
    market-data access.

    Key COIN-M differences from USDⓈ-M:
    - REST host: ``https://dapi.binance.com`` (dapi, not fapi).
    - Stream host: ``wss://dstream.binance.com`` (dstream, not fstream).
    - ``get_open_interest_hist`` uses ``pair`` + ``contractType`` (not ``symbol``).
    - ``get_premium_index`` always returns a list (even for a single symbol).
    - Mark Price stream has no ``mark_price_avg`` (``ap``) field.
    - Force Order stream has a ``pair`` (``ps``) field in the nested order object.

    Construct with an optional :class:`~binance.core.auth.Credentials`::

        from binance import CMFuturesClient

        client = CMFuturesClient()

        oi = await client.get_open_interest(symbol='BTCUSD_PERP')
        fr = await client.get_funding_rate(symbol='BTCUSD_PERP', limit=10)
        oih = await client.get_open_interest_hist(
            pair='BTCUSD', contractType='PERPETUAL', period='1h'
        )
        mp = await client.get_premium_index(symbol='BTCUSD_PERP')

        client.handler(on_mark_price)
        await client.subscribe(SubType.MARK_PRICE, 'btcusd_perp')

        snap = client.rate_limit_snapshot()  # local, no network

    See :class:`~binance.core.client_base.BaseClient` for the full constructor
    keyword arguments.
    """

    # The COIN-M Futures market this client speaks to (hosts / rules / endpoints).
    MARKET: ClassVar[MarketSpec] = CM_MARKET

    # The COIN-M Futures processor set, injected into the HandlerContext.
    PROCESSORS = PROCESSORS
    EXCEPTION_PROCESSOR = ExceptionProcessor
    STREAM_ERROR_PROCESSOR = StreamErrorProcessor
