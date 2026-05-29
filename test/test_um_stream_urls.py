"""UM Futures stream URL migration (2026-04-23 decommission).

Verifies that the USDⓈ-M Futures client routes subscriptions to the new
per-category paths announced by Binance and decommissioned on 2026-04-23:

- ``wss://fstream.binance.com/public/stream?streams=...`` for ``@depth*``,
  ``@bookTicker``, ``!bookTicker`` and ``@rpiDepth*`` (high-frequency streams).
- ``wss://fstream.binance.com/market/stream?streams=...`` for every other
  market-data stream (aggTrade, markPrice, kline, ticker, miniTicker,
  forceOrder, compositeIndex, contractInfo, assetIndex, tradingSession).
- ``wss://fstream.binance.com/private/ws/<listenKey>`` for the dedicated
  user-data fstream connection (replaces the legacy ``/ws/<listenKey>``).

Spot and COIN-M are unaffected (they continue to use the legacy ``/stream``
and ``/ws/<listenKey>`` paths against their own hosts).

Docs:
- Important WebSocket Change Notice:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
- 2026-04-02 derivatives changelog:
  https://developers.binance.com/docs/derivatives/change-log
"""

import pytest

from binance import (
    UMFuturesClient,
    SpotClient,
    CMFuturesClient,
    Credentials,
    SubType,
    TimeFrame,
)
from binance.futures.um.spec import UM_MARKET
from binance.futures.cm.spec import CM_MARKET
from binance.spot.spec import SPOT_MARKET


# ---------------------------------------------------------------------------
# MarketSpec carries per-market stream path layout
# ---------------------------------------------------------------------------


def test_um_market_spec_data_stream_paths():
    """UM splits data streams across /public/stream + /market/stream."""
    assert UM_MARKET.data_stream_paths == ('/public/stream', '/market/stream')


def test_um_market_spec_user_stream_path_template():
    """UM user-data fstream uses /private/ws/{listen_key} (not legacy /ws/)."""
    assert UM_MARKET.user_stream_path_template == '/private/ws/{listen_key}'


def test_spot_market_spec_data_stream_paths():
    """Spot keeps the legacy single /stream path (unaffected by 2026-04-23)."""
    assert SPOT_MARKET.data_stream_paths == ('/stream',)


def test_spot_market_spec_user_stream_path_template():
    """Spot user-data flow does not use a path template (no per-listenKey stream)."""
    # Spot has no listenKey-based dedicated user stream; the field defaults to
    # the legacy /ws/ path for symmetry but is never read by Spot clients.
    assert SPOT_MARKET.user_stream_path_template == '/ws/{listen_key}'


def test_cm_market_spec_data_stream_paths():
    """COIN-M is NOT affected by the UM migration — keeps legacy /stream."""
    assert CM_MARKET.data_stream_paths == ('/stream',)


def test_cm_market_spec_user_stream_path_template():
    """COIN-M user-data fstream keeps the legacy /ws/<listenKey> path."""
    assert CM_MARKET.user_stream_path_template == '/ws/{listen_key}'


# ---------------------------------------------------------------------------
# Per-stream-name router
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('stream_name, expected_path', [
    # /public/stream — high-frequency
    ('btcusdt@depth', '/public/stream'),
    ('btcusdt@depth@500ms', '/public/stream'),
    ('btcusdt@depth5', '/public/stream'),
    ('btcusdt@depth10@100ms', '/public/stream'),
    ('btcusdt@bookTicker', '/public/stream'),
    ('!bookTicker', '/public/stream'),
    ('btcusdt@rpiDepth@500ms', '/public/stream'),
    # /market/stream — everything else
    ('btcusdt@aggTrade', '/market/stream'),
    ('btcusdt@markPrice', '/market/stream'),
    ('btcusdt@markPrice@1s', '/market/stream'),
    ('!markPrice@arr', '/market/stream'),
    ('!markPrice@arr@1s', '/market/stream'),
    ('btcusdt@kline_1m', '/market/stream'),
    ('btcusdt_perpetual@continuousKline_1m', '/market/stream'),
    ('btcusdt@miniTicker', '/market/stream'),
    ('!miniTicker@arr', '/market/stream'),
    ('btcusdt@ticker', '/market/stream'),
    ('!ticker@arr', '/market/stream'),
    ('btcusdt@forceOrder', '/market/stream'),
    ('!forceOrder@arr', '/market/stream'),
    ('btcusdt@compositeIndex', '/market/stream'),
    ('!contractInfo', '/market/stream'),
    ('btcusdt@assetIndex', '/market/stream'),
    ('!assetIndex@arr', '/market/stream'),
    ('tradingSession', '/market/stream'),
])
def test_um_data_stream_router(stream_name, expected_path):
    """UM router classifies each documented stream by suffix per the docs."""
    assert UM_MARKET.data_stream_router(stream_name) == expected_path


def test_spot_data_stream_router_returns_single_path():
    """Spot router always returns /stream (single connection)."""
    assert SPOT_MARKET.data_stream_router('btcusdt@depth') == '/stream'
    assert SPOT_MARKET.data_stream_router('btcusdt@kline_1m') == '/stream'


def test_cm_data_stream_router_returns_single_path():
    """COIN-M router always returns /stream."""
    assert CM_MARKET.data_stream_router('btcusdt@depth') == '/stream'
    assert CM_MARKET.data_stream_router('btcusdt@aggTrade') == '/stream'


# ---------------------------------------------------------------------------
# Subscribe path opens connections to the right URL(s)
# ---------------------------------------------------------------------------


class _RecordingStream:
    """Stream stub: records uri + every sent payload, returns canned values."""

    instances = []

    @classmethod
    def reset(cls):
        cls.instances = []

    def __init__(self, uri, *args, **kwargs):
        self.uri = uri
        self.sent = []
        self.closed = False
        _RecordingStream.instances.append(self)

    def connect(self):
        return self

    async def send(self, req):
        self.sent.append(req)
        if req.get('method') == 'LIST_SUBSCRIPTIONS':
            return []
        return None

    async def close(self, code=4999):
        self.closed = True

    async def recycle(self):
        pass


@pytest.fixture
def patch_stream(monkeypatch):
    _RecordingStream.reset()
    monkeypatch.setattr(
        'binance.core.transport.subscription.Stream', _RecordingStream)
    return _RecordingStream


@pytest.mark.asyncio
async def test_um_subscribe_depth_opens_public_stream(patch_stream):
    """Subscribing to @depth on UM opens a connection at /public/stream."""
    client = UMFuturesClient().start()
    await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')

    uris = {s.uri for s in _RecordingStream.instances}
    assert 'wss://fstream.binance.com/public/stream' in uris
    # Only the /public/stream connection was opened.
    public = [s for s in _RecordingStream.instances
              if s.uri == 'wss://fstream.binance.com/public/stream']
    assert len(public) == 1
    # The SUBSCRIBE frame named the depth stream.
    sub_msgs = [m for m in public[0].sent if m.get('method') == 'SUBSCRIBE']
    assert sub_msgs and 'btcusdt@depth' in sub_msgs[0]['params']

    await client.close()


@pytest.mark.asyncio
async def test_um_subscribe_book_ticker_opens_public_stream(patch_stream):
    """Subscribing to @bookTicker on UM goes to /public/stream."""
    client = UMFuturesClient().start()
    await client.subscribe(SubType.BOOK_TICKER, 'BTCUSDT')

    public = [s for s in _RecordingStream.instances
              if s.uri == 'wss://fstream.binance.com/public/stream']
    assert len(public) == 1
    sub_msgs = [m for m in public[0].sent if m.get('method') == 'SUBSCRIBE']
    assert sub_msgs and 'btcusdt@bookTicker' in sub_msgs[0]['params']
    await client.close()


@pytest.mark.asyncio
async def test_um_subscribe_agg_trade_opens_market_stream(patch_stream):
    """Subscribing to @aggTrade on UM opens a connection at /market/stream."""
    client = UMFuturesClient().start()
    await client.subscribe(SubType.AGG_TRADE, 'BTCUSDT')

    uris = {s.uri for s in _RecordingStream.instances}
    assert 'wss://fstream.binance.com/market/stream' in uris
    market = [s for s in _RecordingStream.instances
              if s.uri == 'wss://fstream.binance.com/market/stream']
    assert len(market) == 1
    sub_msgs = [m for m in market[0].sent if m.get('method') == 'SUBSCRIBE']
    assert sub_msgs and 'btcusdt@aggTrade' in sub_msgs[0]['params']
    await client.close()


@pytest.mark.asyncio
async def test_um_subscribe_kline_opens_market_stream(patch_stream):
    """Subscribing to @kline_<interval> on UM goes to /market/stream."""
    client = UMFuturesClient().start()
    await client.subscribe(SubType.KLINE, 'BTCUSDT', TimeFrame.m1)

    market = [s for s in _RecordingStream.instances
              if s.uri == 'wss://fstream.binance.com/market/stream']
    assert len(market) == 1
    sub_msgs = [m for m in market[0].sent if m.get('method') == 'SUBSCRIBE']
    assert sub_msgs and 'btcusdt@kline_1m' in sub_msgs[0]['params']
    await client.close()


@pytest.mark.asyncio
async def test_um_mixed_subscriptions_open_both_streams(patch_stream):
    """Mixing depth + kline opens BOTH /public/stream AND /market/stream."""
    client = UMFuturesClient().start()
    await client.subscribe(
        SubType.ORDER_BOOK, 'BTCUSDT',
    )
    await client.subscribe(
        SubType.KLINE, 'BTCUSDT', TimeFrame.m1,
    )

    uris = {s.uri for s in _RecordingStream.instances}
    assert 'wss://fstream.binance.com/public/stream' in uris
    assert 'wss://fstream.binance.com/market/stream' in uris

    public = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/public/stream')
    market = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/market/stream')

    public_subs = [m for m in public.sent if m.get('method') == 'SUBSCRIBE']
    market_subs = [m for m in market.sent if m.get('method') == 'SUBSCRIBE']
    assert public_subs and 'btcusdt@depth' in public_subs[0]['params']
    assert market_subs and 'btcusdt@kline_1m' in market_subs[0]['params']

    await client.close()


@pytest.mark.asyncio
async def test_spot_subscribe_still_uses_legacy_stream_path(patch_stream):
    """Spot is unaffected: /stream is still the data stream path."""
    client = SpotClient().start()
    await client.subscribe(SubType.TICKER, 'BTCUSDT')

    uris = {s.uri for s in _RecordingStream.instances}
    assert 'wss://stream.binance.com/stream' in uris
    # No /public/stream nor /market/stream on Spot.
    assert not any('/public/stream' in u for u in uris)
    assert not any('/market/stream' in u for u in uris)

    await client.close()


@pytest.mark.asyncio
async def test_cm_subscribe_still_uses_legacy_stream_path(patch_stream):
    """COIN-M is unaffected: dstream.binance.com/stream is still the data path."""
    client = CMFuturesClient(Credentials('k')).start()
    await client.subscribe(SubType.AGG_TRADE, 'BTCUSDT_PERP')

    uris = {s.uri for s in _RecordingStream.instances}
    assert 'wss://dstream.binance.com/stream' in uris
    assert not any('/public/stream' in u for u in uris)
    assert not any('/market/stream' in u for u in uris)

    await client.close()


# ---------------------------------------------------------------------------
# Unsubscribe path routes to the correct connection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_um_unsubscribe_routes_to_matching_stream(patch_stream):
    """UNSUBSCRIBE for a depth stream goes to /public/stream, not /market/."""
    client = UMFuturesClient().start()
    await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')
    await client.subscribe(SubType.KLINE, 'BTCUSDT', TimeFrame.m1)

    await client.unsubscribe(SubType.ORDER_BOOK, 'BTCUSDT')

    public = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/public/stream')
    market = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/market/stream')

    public_unsubs = [m for m in public.sent if m.get('method') == 'UNSUBSCRIBE']
    market_unsubs = [m for m in market.sent if m.get('method') == 'UNSUBSCRIBE']

    assert public_unsubs and 'btcusdt@depth' in public_unsubs[0]['params']
    assert not market_unsubs

    await client.close()


# ---------------------------------------------------------------------------
# Resubscribe on reconnect uses the correct per-path partitioning
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_um_resubscribe_partitions_by_path(patch_stream):
    """Resubscribe (e.g. after reconnect) partitions streams across both paths."""
    client = UMFuturesClient().start()
    # Build a recorded subscribed set that mixes a public and a market stream.
    await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')
    await client.subscribe(SubType.KLINE, 'BTCUSDT', TimeFrame.m1)

    # Reset recorded sends and trigger explicit resubscribe (e.g. on_connected).
    for s in _RecordingStream.instances:
        s.sent = []

    await client._resubscribe()

    public = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/public/stream')
    market = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/market/stream')

    pub_subs = [m for m in public.sent if m.get('method') == 'SUBSCRIBE']
    mkt_subs = [m for m in market.sent if m.get('method') == 'SUBSCRIBE']

    assert pub_subs and 'btcusdt@depth' in pub_subs[0]['params']
    assert mkt_subs and 'btcusdt@kline_1m' in mkt_subs[0]['params']

    await client.close()


@pytest.mark.asyncio
async def test_get_data_stream_default_path_when_unspecified(patch_stream):
    """_get_data_stream() with no path returns the market's default-path stream."""
    client = UMFuturesClient().start()
    s = client._get_data_stream()  # no path arg -> default '/public/stream'
    assert s.uri == 'wss://fstream.binance.com/public/stream'
    await client.close()


@pytest.mark.asyncio
async def test_resubscribe_path_callback_replays_only_matching_streams(patch_stream):
    """The per-path on_connected callback replays only subs whose router result matches."""
    client = UMFuturesClient().start()
    await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')
    await client.subscribe(SubType.KLINE, 'BTCUSDT', TimeFrame.m1)

    # Reset and invoke the public-path resubscribe callback (mimics reconnect).
    for s in _RecordingStream.instances:
        s.sent = []
    cb = client._build_data_resubscribe('/public/stream')
    await cb()

    public = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/public/stream')
    market = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/market/stream')

    pub_subs = [m for m in public.sent if m.get('method') == 'SUBSCRIBE']
    mkt_subs = [m for m in market.sent if m.get('method') == 'SUBSCRIBE']

    # Only the /public path got a SUBSCRIBE (kline did NOT also fire on /market).
    assert pub_subs and 'btcusdt@depth' in pub_subs[0]['params']
    assert not mkt_subs

    await client.close()


@pytest.mark.asyncio
async def test_resubscribe_path_with_no_market_subscriptions_is_noop(patch_stream):
    """Per-path resubscribe is a no-op when no market subs are tracked."""
    client = UMFuturesClient().start()
    # No subscriptions at all.
    await client._resubscribe_path('/public/stream')
    # No streams should have been opened.
    assert _RecordingStream.instances == []
    await client.close()


@pytest.mark.asyncio
async def test_resubscribe_path_with_no_matching_streams_is_noop(patch_stream):
    """Per-path resubscribe skips when no recorded stream routes to ``path``."""
    client = UMFuturesClient().start()
    await client.subscribe(SubType.KLINE, 'BTCUSDT', TimeFrame.m1)
    # Reset, then trigger the /public/stream callback — kline routes to /market,
    # so the public callback finds no matching streams and returns.
    for s in _RecordingStream.instances:
        s.sent = []
    await client._resubscribe_path('/public/stream')
    public_instances = [s for s in _RecordingStream.instances
                        if s.uri == 'wss://fstream.binance.com/public/stream']
    assert public_instances == []
    await client.close()


@pytest.mark.asyncio
async def test_resubscribe_path_failure_recycles_only_that_stream(patch_stream):
    """A failure during per-path resubscribe recycles only the matching stream."""
    import asyncio

    client = UMFuturesClient().start()
    await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')
    await client.subscribe(SubType.KLINE, 'BTCUSDT', TimeFrame.m1)

    public = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/public/stream')
    market = next(s for s in _RecordingStream.instances
                  if s.uri == 'wss://fstream.binance.com/market/stream')

    recycled_public = []
    recycled_market = []

    async def fail_send(_req):
        raise RuntimeError('send boom')

    public.send = fail_send  # type: ignore[method-assign]

    async def public_recycle():
        recycled_public.append(True)

    async def market_recycle():
        recycled_market.append(True)

    public.recycle = public_recycle  # type: ignore[method-assign]
    market.recycle = market_recycle  # type: ignore[method-assign]

    await client._resubscribe_path('/public/stream')
    await asyncio.sleep(0)

    # Only the public stream was recycled.
    assert recycled_public == [True]
    assert recycled_market == []

    await client.close()
