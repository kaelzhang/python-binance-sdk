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
from binance.futures.user_stream import FuturesUserStreamMixin


class CMFuturesClient(FuturesUserStreamMixin, BaseClient, CMFuturesGetters):  # type: ignore[misc]  # diamond mixin: compatible at runtime
    """Async Binance COIN-M (coin-margined) Futures client.

    Binds the COIN-M Futures :class:`~binance.core.market.MarketSpec` (hosts,
    rate-limit rules, stream processors) onto the shared
    :class:`~binance.core.client_base.BaseClient` and mixes in
    ``CMFuturesGetters``, the generated async methods for every COIN-M Futures
    endpoint -- market-data, trading, account, and position management.

    Key COIN-M differences from USDⓈ-M:
    - REST host: ``https://dapi.binance.com`` (dapi, not fapi).
    - Stream host: ``wss://dstream.binance.com`` (dstream, not fstream).
    - WS-API host: ``wss://ws-dapi.binance.com/ws-dapi/v1``.
    - ``get_open_interest_hist`` uses ``pair`` + ``contractType`` (not ``symbol``).
    - ``get_premium_index`` always returns a list (even for a single symbol).
    - Mark Price stream has no ``mark_price_avg`` (``ap``) field.
    - Force Order stream has a ``pair`` (``ps``) field in the nested order object.
    - No ``multiAssetsMargin`` endpoint (USDⓈ-M only).
    - ``get_position_risk`` is on ``/dapi/v1/positionRisk`` (not ``/fapi/v3/``).
    - Rate limits: no 10-second ORDERS pool (only 1-minute ORDERS pool).
    - User-data stream events arrive on ``wss://dstream.binance.com/ws/<listenKey>``.

    Construct with a :class:`~binance.core.auth.Credentials` for trading::

        from binance import CMFuturesClient, Credentials, SubType

        client = CMFuturesClient(Credentials(api_key='...', api_secret='...'))

        # Trading
        order = await client.create_order(
            symbol='BTCUSD_PERP', side='BUY', type='LIMIT',
            timeInForce='GTC', quantity='1', price='30000')

        # Account
        balance = await client.get_balance()
        positions = await client.get_position_risk(pair='BTCUSD')

        # User-data stream
        client.handler(on_account_update)
        await client.subscribe(SubType.USER)

        # Market-data (no credentials needed)
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
