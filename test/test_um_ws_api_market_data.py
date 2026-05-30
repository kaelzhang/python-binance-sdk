"""Hermetic tests for USDⓈ-M Futures WS-API market-data endpoints.

Covers the three documented WS-API market-data methods on the UM ws-fapi
connection: ``depth`` (order book), ``ticker.price``, and ``ticker.book``.
All three are security NONE, accept ``symbol`` as an optional/required
param, and have dynamic weights documented on developers.binance.com.

Docs:
- https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-api/Order-Book
- https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-api/Symbol-Price-Ticker
- https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-api/Symbol-Order-Book-Ticker
"""

import pytest

from binance import UMFuturesClient
from binance.core.common.constants import SecurityType
from binance.core.rate_limit.types import RateLimitType
from binance.futures.um.endpoints import WS_API_ENDPOINTS
from test.test_ws_api import WSAPIServer


_PORT = 9101


def _make_client(server) -> UMFuturesClient:
    client = UMFuturesClient(ws_api_host=server.uri)
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    return [w for w in snap.windows if w.type == RateLimitType.REQUEST_WEIGHT][0].used


# ---------------------------------------------------------------------------
# get_orderbook_ws — depth, security NONE, dynamic weight by ``limit``.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_get_orderbook_ws_default_limit_weight_10():
    """``depth`` default limit (500) -> weight 10."""
    server = WSAPIServer(port=_PORT)
    server.on('depth', result={
        'lastUpdateId': 1, 'E': 1, 'T': 1, 'bids': [], 'asks': []
    })
    await server.run()
    try:
        client = _make_client(server)
        result = await client.get_orderbook_ws(symbol='BTCUSDT')
        assert result['lastUpdateId'] == 1
        sent = server.received[0]
        assert sent['method'] == 'depth'
        assert sent['params']['symbol'] == 'BTCUSDT'
        # No limit supplied -> docs default 500 -> weight 10
        assert _weight_used(client) == 10
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_um_get_orderbook_ws_limit_50_weight_2():
    server = WSAPIServer(port=_PORT)
    server.on('depth', result={
        'lastUpdateId': 2, 'E': 1, 'T': 1, 'bids': [], 'asks': []
    })
    await server.run()
    try:
        client = _make_client(server)
        await client.get_orderbook_ws(symbol='BTCUSDT', limit=50)
        sent = server.received[0]
        assert sent['method'] == 'depth'
        assert sent['params']['limit'] == 50
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_um_get_orderbook_ws_limit_1000_weight_20():
    server = WSAPIServer(port=_PORT)
    server.on('depth', result={
        'lastUpdateId': 3, 'E': 1, 'T': 1, 'bids': [], 'asks': []
    })
    await server.run()
    try:
        client = _make_client(server)
        await client.get_orderbook_ws(symbol='BTCUSDT', limit=1000)
        assert _weight_used(client) == 20
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_ticker_price_ws — ticker.price, security NONE, dynamic 1/2.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_get_ticker_price_ws_with_symbol_weight_1():
    """``ticker.price`` weight 1 with symbol per docs."""
    server = WSAPIServer(port=_PORT)
    server.on('ticker.price', result={'symbol': 'BTCUSDT', 'price': '50000', 'time': 1})
    await server.run()
    try:
        client = _make_client(server)
        await client.get_ticker_price_ws(symbol='BTCUSDT')
        sent = server.received[0]
        assert sent['method'] == 'ticker.price'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert _weight_used(client) == 1
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_um_get_ticker_price_ws_no_symbol_weight_2():
    """``ticker.price`` weight 2 when symbol is omitted."""
    server = WSAPIServer(port=_PORT)
    server.on('ticker.price', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_ticker_price_ws()
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# get_ticker_book_ws — ticker.book, security NONE, dynamic 2/5.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_get_ticker_book_ws_with_symbol_weight_2():
    """``ticker.book`` weight 2 with symbol per docs."""
    server = WSAPIServer(port=_PORT)
    server.on('ticker.book', result={
        'symbol': 'BTCUSDT', 'bidPrice': '50000', 'bidQty': '1',
        'askPrice': '50001', 'askQty': '1', 'time': 1, 'lastUpdateId': 1
    })
    await server.run()
    try:
        client = _make_client(server)
        await client.get_ticker_book_ws(symbol='BTCUSDT')
        sent = server.received[0]
        assert sent['method'] == 'ticker.book'
        assert sent['params']['symbol'] == 'BTCUSDT'
        assert _weight_used(client) == 2
    finally:
        await client.close()
        await server.shutdown()


@pytest.mark.asyncio
async def test_um_get_ticker_book_ws_no_symbol_weight_5():
    """``ticker.book`` weight 5 when symbol is omitted."""
    server = WSAPIServer(port=_PORT)
    server.on('ticker.book', result=[])
    await server.run()
    try:
        client = _make_client(server)
        await client.get_ticker_book_ws()
        assert _weight_used(client) == 5
    finally:
        await client.close()
        await server.shutdown()


# ---------------------------------------------------------------------------
# Registry shape — three new entries with correct security/weight.
# ---------------------------------------------------------------------------

def test_um_market_data_ws_api_registry_entries():
    by_name = {entry['name']: entry for entry in WS_API_ENDPOINTS}

    book = by_name['get_orderbook_ws']
    assert book['ws_method'] == 'depth'
    assert book['security_type'] == SecurityType.NONE
    assert book['transport'] == 'ws_api'
    assert callable(book['weight'])  # dynamic by limit

    price = by_name['get_ticker_price_ws']
    assert price['ws_method'] == 'ticker.price'
    assert price['security_type'] == SecurityType.NONE
    assert price['transport'] == 'ws_api'
    assert callable(price['weight'])  # dynamic by symbol presence

    bookticker = by_name['get_ticker_book_ws']
    assert bookticker['ws_method'] == 'ticker.book'
    assert bookticker['security_type'] == SecurityType.NONE
    assert bookticker['transport'] == 'ws_api'
    assert callable(bookticker['weight'])
