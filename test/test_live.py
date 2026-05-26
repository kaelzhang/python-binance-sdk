"""Opt-in live-API smoke tests.

These tests hit the real Binance endpoints and are skipped by default so the
hermetic, mock-driven default suite (``pytest test/``) stays untouched. Set
``BINANCE_LIVE=1`` (or ``true`` / ``yes``, case-insensitive) to enable.

Coverage:

* Per-market public connectivity probe (Spot / UM / CM): Spot uses the WS-API
  ``get_server_time`` call; UM and CM use REST ``get_premium_index`` because
  the futures clients do not expose a public WS-API ``time`` method (only
  signed trading / account WS-API methods are wired on futures).
* Stream-subscribe round-trip per market: subscribe to a real public stream
  and wait for the first message.
* One signed Spot endpoint (``get_account``) — read-only, no orders placed.
  Skipped when ``API_KEY`` / ``API_SECRET`` are absent from the environment
  / ``.env`` / ``.env.*`` files (resolved via ``get_api_credentials``).

The suite fails fast on unreachable hosts: stream waits are bounded by
``asyncio.wait_for(..., timeout=15)`` so a geo-blocked / region-restricted
network surfaces as a clear timeout instead of a 30-second pytest-timeout
kill. None of these tests place orders or mutate account state.
"""

import asyncio

import pytest

from binance import (
    CMFuturesClient,
    Credentials,
    MarkPriceHandlerBase,
    SpotClient,
    SubType,
    TickerHandlerBase,
    UMFuturesClient,
)

from .common import get_api_credentials, live


# ---------------------------------------------------------------------------
# Public connectivity smoke (no credentials required)
# ---------------------------------------------------------------------------

# Server-side ms-epoch values. Any value below 2023-11-14 (≈ this constant)
# almost certainly indicates a wrong code path, not a real reply.
_MIN_PLAUSIBLE_SERVER_TIME_MS = 1_700_000_000_000


@live
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_spot_ws_api_time():
    """Verify Spot WS-API connectivity by fetching server time."""
    client = SpotClient().start()
    try:
        result = await client.get_server_time()
    finally:
        await client.close()

    assert isinstance(result, dict)
    assert 'serverTime' in result
    assert isinstance(result['serverTime'], int)
    assert result['serverTime'] > _MIN_PLAUSIBLE_SERVER_TIME_MS


@live
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_um_rest_premium_index():
    """Verify UM Futures REST connectivity by fetching the premium index.

    The UM client does not expose a public WS-API ``time`` getter (only signed
    trading / account methods are wired on the futures WS-API). The closest
    public-data probe is ``get_premium_index``, which returns the current
    mark/index/funding data plus the server ``time`` for the symbol.
    """
    client = UMFuturesClient().start()
    try:
        result = await client.get_premium_index(symbol='BTCUSDT')
    finally:
        await client.close()

    assert isinstance(result, dict)
    assert result.get('symbol') == 'BTCUSDT'
    assert isinstance(result.get('time'), int)
    assert result['time'] > _MIN_PLAUSIBLE_SERVER_TIME_MS


@live
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_cm_rest_premium_index():
    """Verify CM Futures REST connectivity by fetching the premium index.

    Same rationale as the UM test: the CM client does not expose a public
    WS-API ``time`` getter, so REST ``get_premium_index`` is used.
    """
    client = CMFuturesClient().start()
    try:
        result = await client.get_premium_index(symbol='BTCUSD_PERP')
    finally:
        await client.close()

    # CM ``get_premium_index`` for a perpetual symbol returns a list (one
    # element per matching pair / contract type).
    if isinstance(result, list):
        assert result, 'premium index list is empty'
        item = result[0]
    else:
        item = result

    assert isinstance(item, dict)
    assert item.get('symbol') == 'BTCUSD_PERP'
    assert isinstance(item.get('time'), int)
    assert item['time'] > _MIN_PLAUSIBLE_SERVER_TIME_MS


# ---------------------------------------------------------------------------
# Stream subscribe + first-message smoke (no credentials required)
# ---------------------------------------------------------------------------

_STREAM_FIRST_MESSAGE_TIMEOUT = 15  # seconds — fail fast on unreachable hosts.


@live
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_spot_stream_ticker():
    """Subscribe to ``btcusdt@ticker`` and verify at least one message arrives."""
    received = asyncio.Event()
    received_msg: dict = {}

    class TickerHandler(TickerHandlerBase):
        def receive(self, payload):
            received_msg.update(payload)
            received.set()

    client = SpotClient().start()
    try:
        client.handler(TickerHandler())
        await client.subscribe(SubType.TICKER, 'BTCUSDT')
        await asyncio.wait_for(
            received.wait(), timeout=_STREAM_FIRST_MESSAGE_TIMEOUT
        )
    finally:
        await client.close()

    assert received_msg.get('e') == '24hrTicker'
    assert received_msg.get('s') == 'BTCUSDT'


@live
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_um_stream_mark_price():
    """Subscribe to UM ``btcusdt@markPrice`` and verify a message arrives."""
    received = asyncio.Event()
    received_msg: dict = {}

    class MarkPriceHandler(MarkPriceHandlerBase):
        def receive(self, payload):
            received_msg.update(payload)
            received.set()

    client = UMFuturesClient().start()
    try:
        client.handler(MarkPriceHandler())
        await client.subscribe(SubType.MARK_PRICE, 'BTCUSDT')
        await asyncio.wait_for(
            received.wait(), timeout=_STREAM_FIRST_MESSAGE_TIMEOUT
        )
    finally:
        await client.close()

    assert received_msg.get('e') == 'markPriceUpdate'
    assert received_msg.get('s') == 'BTCUSDT'


@live
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_cm_stream_mark_price():
    """Subscribe to CM ``btcusd_perp@markPrice`` and verify a message arrives."""
    received = asyncio.Event()
    received_msg: dict = {}

    class MarkPriceHandler(MarkPriceHandlerBase):
        def receive(self, payload):
            received_msg.update(payload)
            received.set()

    client = CMFuturesClient().start()
    try:
        client.handler(MarkPriceHandler())
        await client.subscribe(SubType.MARK_PRICE, 'BTCUSD_PERP')
        await asyncio.wait_for(
            received.wait(), timeout=_STREAM_FIRST_MESSAGE_TIMEOUT
        )
    finally:
        await client.close()

    assert received_msg.get('e') == 'markPriceUpdate'
    assert received_msg.get('s') == 'BTCUSD_PERP'


# ---------------------------------------------------------------------------
# Signed endpoint smoke (credentials-gated, no orders placed)
# ---------------------------------------------------------------------------


@live
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_spot_get_account_signed():
    """Verify Spot signed-endpoint path by calling ``get_account`` (read-only).

    Requires ``API_KEY`` / ``API_SECRET`` in ``.env*`` or the environment.
    Skipped if absent. DOES NOT place orders or modify account state.
    """
    api_key, api_secret = get_api_credentials()
    if not api_key or not api_secret:
        pytest.skip('no API credentials in environment / .env*')

    client = SpotClient(
        Credentials(api_key=api_key, api_secret=api_secret)
    ).start()
    try:
        result = await client.get_account()
    finally:
        await client.close()

    assert isinstance(result, dict)
    assert 'balances' in result
    assert isinstance(result['balances'], list)
