import pytest
import asyncio

from aioresponses import aioresponses
from stock_pandas import StockDataFrame

from binance import (
    Client,
    SubType,
    TimeFrame,

    TickerHandlerBase,
    KlineHandlerBase,
    AccountPositionHandlerBase,
    ExternalLockUpdateHandlerBase,
    EventStreamTerminatedHandlerBase,

    InvalidHandlerException,
    OrderBookHandlerBase,
    InvalidSubTypeParamException,
    InvalidSubParamsException,
    HandlerExceptionHandlerBase,
    UnsupportedSubTypeException
)
from binance.common.exceptions import TooManyStreamsException
from binance.common.utils import create_future


@pytest.fixture
def client():
    return Client('api_key').start()


TICKER_RES = dict(
    data=dict(
        e='24hrTicker',
        foo='bar'
    )
)


def install_fake_stream(monkeypatch):
    """Patch the manager's Stream so `_get_data_stream()` runs its real body
    (constructing a Stream, covering the manager wiring) but never opens a
    socket. `send()` returns a canned value for LIST_SUBSCRIPTIONS and `None`
    otherwise.
    """
    class FakeStream:
        def __init__(self, *args, **kwargs):
            pass

        def connect(self):
            return self

        async def send(self, req):
            if req.get('method') == 'LIST_SUBSCRIPTIONS':
                return ['btcusdt@depth']
            return None

        async def close(self, code=4999):
            pass

    monkeypatch.setattr('binance.subscribe.manager.Stream', FakeStream)
    return FakeStream


@pytest.mark.asyncio
async def test_ticker_handler(client):
    class TickerPrinter(TickerHandlerBase):
        DATA = None
        DF = None

        # sync receiver
        def receive(self, payload):
            TickerPrinter.DATA = payload
            TickerPrinter.DF = super().receive(payload)

    client.handler(TickerPrinter())

    await client._receive(TICKER_RES)

    assert TickerPrinter.DATA == TICKER_RES['data']
    assert isinstance(TickerPrinter.DF, StockDataFrame)
    await client.close()


@pytest.mark.asyncio
async def test_handler_exception_handler(client):
    exc = Exception()

    f = create_future()

    class TickerPrinter(TickerHandlerBase):
        def receive(self, payload):
            raise exc

    class ExceptionHandler(HandlerExceptionHandlerBase):
        def receive(self, e):
            f.set_result(e)

    client.handler(TickerPrinter(), ExceptionHandler())

    await client._receive(TICKER_RES)

    assert await f == exc
    await client.close()


def test_invalid_handler(client):
    with pytest.raises(InvalidHandlerException, match='invalid handler'):
        client.handler(1)


@pytest.mark.asyncio
async def test_invalid_subtype_symbol(client):
    with pytest.raises(InvalidSubTypeParamException, match='invalid param'):
        await client.subscribe(SubType.TICKER)

    with pytest.raises(InvalidSubTypeParamException, match='string expected'):
        await client.subscribe(SubType.TICKER, 1)

    with pytest.raises(InvalidSubTypeParamException, match='string expected'):
        await client.subscribe(SubType.TICKER, None)

    with pytest.raises(InvalidSubParamsException, match='invalid subscribe'):
        await client.subscribe(SubType.TICKER, 1, 2)

    with pytest.raises(UnsupportedSubTypeException, match='subtype "unknown"'):
        await client.subscribe('unknown')

    with pytest.raises(InvalidSubTypeParamException, match='symbol'):
        await client.subscribe(SubType.KLINE)

    with pytest.raises(InvalidSubTypeParamException, match='not specified'):
        await client.subscribe(SubType.KLINE, 'BTCUSDT')

    with pytest.raises(InvalidSubTypeParamException, match='TimeFrame'):
        await client.subscribe(SubType.KLINE, 'BTCUSDT', 1)


@pytest.mark.asyncio
async def test_client_handler(client, monkeypatch):
    install_fake_stream(monkeypatch)

    f = create_future()

    class TickerHandler(TickerHandlerBase):
        # async receiver
        async def receive(self, payload):
            f.set_result(payload)

    client.handler(TickerHandler())
    # Real subscribe path: builds params via the processor, constructs the
    # (faked) data stream and sends SUBSCRIBE -- all without a socket.
    await client.subscribe(SubType.TICKER, 'BTCUSDT')

    # Feed a 24hrTicker stream message hermetically.
    await client._receive({
        'data': {
            'e': '24hrTicker',
            's': 'BTCUSDT'
        }
    })

    payload = await f

    assert payload['e'] == '24hrTicker'
    assert payload['s'] == 'BTCUSDT'

    await client.close()


@pytest.mark.asyncio
async def test_client_kline_handler(client, monkeypatch):
    install_fake_stream(monkeypatch)

    f = create_future()

    class KlineHandler(KlineHandlerBase):
        # async receiver
        async def receive(self, payload):
            f.set_result(payload)

    client.handler(KlineHandler())
    # Real subscribe path for a 3-arg subtype (KLINE + symbol + TimeFrame).
    await client.subscribe(SubType.KLINE, 'BTCUSDT', TimeFrame.D1)

    # Feed a kline stream message hermetically.
    await client._receive({
        'data': {
            'e': 'kline',
            's': 'BTCUSDT'
        }
    })

    payload = await f

    assert payload['e'] == 'kline'
    assert payload['s'] == 'BTCUSDT'

    await client.close()


@pytest.mark.asyncio
async def test_user_handler_ws_api_event(client):
    f = create_future()

    class AccountPositionHandler(AccountPositionHandlerBase):
        def receive(self, payload):
            f.set_result(payload)

    client.handler(AccountPositionHandler())

    await client._receive({
        'subscriptionId': 0,
        'event': {
            'e': 'outboundAccountPosition',
            'foo': 'bar'
        }
    })

    payload = await f

    assert payload['e'] == 'outboundAccountPosition'
    assert payload['foo'] == 'bar'

    await client.close()


@pytest.mark.asyncio
async def test_user_handler_ws_api_external_lock_update_event(client):
    f = create_future()

    class ExternalLockUpdateHandler(ExternalLockUpdateHandlerBase):
        def receive(self, payload):
            f.set_result(payload)

    client.handler(ExternalLockUpdateHandler())

    await client._receive({
        'subscriptionId': 0,
        'event': {
            'e': 'externalLockUpdate',
            'foo': 'bar'
        }
    })

    payload = await f

    assert payload['e'] == 'externalLockUpdate'
    assert payload['foo'] == 'bar'

    await client.close()


@pytest.mark.asyncio
async def test_user_stream_auto_recover_on_event_stream_terminated(client):
    calls = []

    async def fake_subscribe_user_only(subscribe: bool, subscriptions):
        calls.append((subscribe, tuple(subscriptions)))

    class EventStreamTerminatedHandler(EventStreamTerminatedHandlerBase):
        def receive(self, payload):
            return payload

    client.handler(EventStreamTerminatedHandler())
    client._subscribe_user_only = fake_subscribe_user_only
    client._want_user_stream = True
    client._subscribed.add((SubType.USER,))

    await client._receive({
        'subscriptionId': 0,
        'event': {
            'e': 'eventStreamTerminated'
        }
    })

    assert calls == [(True, ((SubType.USER,),))]

    await client.close()


@pytest.mark.asyncio
async def test_user_stream_terminated_no_recover_when_unsubscribe_inflight(client):
    calls = []

    async def fake_subscribe_user_only(subscribe: bool, subscriptions):
        calls.append((subscribe, tuple(subscriptions)))

    class EventStreamTerminatedHandler(EventStreamTerminatedHandlerBase):
        def receive(self, payload):
            return payload

    client.handler(EventStreamTerminatedHandler())
    client._subscribe_user_only = fake_subscribe_user_only
    client._want_user_stream = True
    client._user_unsubscribe_inflight = True
    client._subscribed.add((SubType.USER,))

    await client._receive({
        'subscriptionId': 0,
        'event': {
            'e': 'eventStreamTerminated'
        }
    })

    assert calls == []

    await client.close()


@pytest.mark.asyncio
async def test_user_stream_auto_recover_without_user_handler(client):
    calls = []

    async def fake_subscribe_user_only(subscribe: bool, subscriptions):
        calls.append((subscribe, tuple(subscriptions)))

    client._subscribe_user_only = fake_subscribe_user_only
    client._want_user_stream = True
    client._subscribed.add((SubType.USER,))
    client._get_handler_ctx()

    await client._receive({
        'subscriptionId': 0,
        'event': {
            'e': 'eventStreamTerminated'
        }
    })

    assert calls == [(True, ((SubType.USER,),))]

    await client.close()


# Canned depth snapshot served instead of the live REST endpoint.
_SNAPSHOT_URL = (
    'https://api.binance.com/api/v3/depth?limit=1000&symbol=BTCUSDT'
)
_SNAPSHOT_ASKS = [[100, 10]]
_SNAPSHOT_BIDS = [[99, 5]]

# First depthUpdate: continuous with the snapshot (U <= lastUpdateId + 1).
_UPDATE_FIRST = dict(
    e='depthUpdate',
    s='BTCUSDT',
    U=11,
    u=12,
    a=[[101, 2]],
    b=[[98, 3]]
)

# Second depthUpdate: always mergeable (first id is tiny, last id is huge),
# so it merges and emits `updated()` regardless of the current last_update_id.
_UPDATE_NEXT = dict(
    e='depthUpdate',
    s='BTCUSDT',
    U=1,
    u=10_000_000,
    a=[[102, 4]],
    b=[[97, 6]]
)

# Stale depthUpdate: its last id is <= the current last id, so it is
# abandoned (covers the early-return branch in OrderBook._update).
_UPDATE_STALE = dict(
    e='depthUpdate',
    s='BTCUSDT',
    U=11,
    u=12,
    a=[],
    b=[]
)


async def run_orderbook_handler(client, monkeypatch, init_orderbook_first):
    install_fake_stream(monkeypatch)

    f = create_future()

    class OrderBookHandler(OrderBookHandlerBase):
        def receive(self, payload):
            f.set_result(super().receive(payload))

    handler = OrderBookHandler()

    with aioresponses() as m:
        # The depth snapshot may be fetched more than once (set_client +
        # any background refetch); serve the same canned snapshot repeatedly.
        m.get(
            _SNAPSHOT_URL,
            payload=dict(
                lastUpdateId=10,
                asks=_SNAPSHOT_ASKS,
                bids=_SNAPSHOT_BIDS
            ),
            status=200,
            repeat=True
        )

        if init_orderbook_first:
            # Created before the client is attached -> goes to the
            # uninit list and gets its client in set_client().
            orderbook = handler.orderbook('BTCUSDT')

        # When init_orderbook_first, this triggers the snapshot fetch.
        client.handler(handler)
        await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')

        # Feed the first depthUpdate hermetically. When the orderbook is
        # created lazily (not init_orderbook_first), this also triggers
        # the snapshot fetch and the payload is queued until it resolves.
        await client._receive({'data': _UPDATE_FIRST})

        info, [bids, asks] = await f
        assert isinstance(info, StockDataFrame)
        assert isinstance(bids, StockDataFrame)
        assert isinstance(asks, StockDataFrame)

        if not init_orderbook_first:
            orderbook = handler.orderbook('BTCUSDT')

        async def assert_no_change():
            asks = [*orderbook.asks]
            await asyncio.sleep(0.2)

            # should have no change
            assert asks == orderbook.asks

        # Drain any in-progress snapshot fetch so the orderbook is ready.
        if not orderbook.ready:  # type: ignore
            await orderbook.updated()  # type: ignore

        # Await the NEXT emit, triggered by feeding a mergeable update.
        # Grab the pending future BEFORE feeding the update: _emit_updated
        # resolves the current future and then swaps in a fresh one, so
        # awaiting `orderbook.updated()` *after* the emit would block forever.
        next_update = orderbook._updated_future  # type: ignore
        await client._receive({'data': _UPDATE_NEXT})
        await next_update

        # A stale update is abandoned without changing the book.
        await client._receive({'data': _UPDATE_STALE})

        assert len(orderbook.asks) != 0  # type: ignore
        assert len(orderbook.bids) != 0  # type: ignore

        assert await client.list_subscriptions() == ['btcusdt@depth']

        client.stop()
        await assert_no_change()

        client.start()

        await client.unsubscribe(SubType.ORDER_BOOK, 'BTCUSDT')

        await assert_no_change()

        await client.close()


@pytest.mark.asyncio
async def test_orderbook_handler_init_orderbook_ahead(client, monkeypatch):
    await run_orderbook_handler(client, monkeypatch, True)


@pytest.mark.asyncio
async def test_orderbook_handler_init_orderbook_after(client, monkeypatch):
    await run_orderbook_handler(client, monkeypatch, False)


def _ws_streams_used(client):
    for w in client.rate_limit_snapshot().windows:
        if w.type == 'ws_streams':
            return w.used
    return 0


@pytest.mark.asyncio
async def test_subscribe_tracks_stream_count_in_core(monkeypatch):
    from binance import Client
    client = Client()

    async def fake_send(_msg):
        return None

    fake_stream = type('S', (), {'send': staticmethod(fake_send)})()

    async def fake_params_two(subscribe, subscriptions):
        return ['a@trade', 'b@trade']

    monkeypatch.setattr(
        client._get_handler_ctx(), 'subscribe_params', fake_params_two)
    monkeypatch.setattr(client, '_get_data_stream', lambda: fake_stream)

    await client._subscribe_only(True, [('trade', 'A'), ('trade', 'B')])

    assert _ws_streams_used(client) == 2
    assert len(client._stream_names) == 2

    async def fake_params_one(subscribe, subscriptions):
        return ['a@trade']

    monkeypatch.setattr(
        client._get_handler_ctx(), 'subscribe_params', fake_params_one)

    await client._subscribe_only(False, [('trade', 'A')])

    assert _ws_streams_used(client) == 1
    assert client._stream_names == {'b@trade'}


@pytest.mark.asyncio
async def test_subscribe_rolls_back_reservation_on_send_failure(monkeypatch):
    from binance import Client
    client = Client()

    async def failing_send(_msg):
        raise RuntimeError('send failed')

    fake_stream = type('S', (), {'send': staticmethod(failing_send)})()

    async def fake_params(subscribe, subscriptions):
        return ['x@trade']

    monkeypatch.setattr(
        client._get_handler_ctx(), 'subscribe_params', fake_params)
    monkeypatch.setattr(client, '_get_data_stream', lambda: fake_stream)

    with pytest.raises(RuntimeError):
        await client._subscribe_only(True, [('trade', 'X')])

    assert _ws_streams_used(client) == 0
    assert client._stream_names == set()


@pytest.mark.asyncio
async def test_subscribe_rejects_more_than_1024_streams(monkeypatch):
    from binance import Client
    client = Client()

    async def fake_send(_msg):
        return None

    # Pretend 1024 market streams are already active
    client._stream_names = set(f's{i}@trade' for i in range(1024))

    async def fake_params(subscribe, subscriptions):
        return ['btcusdt@trade']

    monkeypatch.setattr(
        client._get_handler_ctx(), 'subscribe_params', fake_params)
    monkeypatch.setattr(client, '_get_data_stream', lambda: type(
        'S', (), {'send': staticmethod(fake_send)})())

    with pytest.raises(TooManyStreamsException):
        await client._subscribe_only(True, [('trade', 'BTCUSDT')])
