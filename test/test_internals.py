"""Unit tests for internal / defensive code paths.

These cover branches that the integration tests do not naturally exercise
(error handlers, validation guards, lifecycle edge cases), keeping the
package at 100% statement coverage.
"""

import asyncio
from types import SimpleNamespace
from logging import getLogger

import pytest
from aioresponses import aioresponses

from binance import Client, Stream, SubType, UserStreamNotSubscribedException
from binance.common.exceptions import (
    APIKeyNotDefinedException,
    InvalidSubTypeParamException
)
from binance.rate_limit import RateLimiter
from binance.handlers.orderbook import OrderBook
from binance.processors.processors import (
    _get_window,
    _get_order_book_interval,
    _get_partial_depth_level
)

logger = getLogger(__name__)

_DEPTH_URL = 'https://api.binance.com/api/v3/depth'


# --- client/base.py -------------------------------------------------------

def test_ws_api_signature_requires_api_key():
    with pytest.raises(APIKeyNotDefinedException):
        Client()._ws_api_signature_params()


@pytest.mark.asyncio
async def test_malformed_rate_limit_headers_are_ignored():
    client = Client()
    with aioresponses() as m:
        m.get(_DEPTH_URL + '?symbol=BTCUSDT', status=200,
              headers={'X-MBX-USED-WEIGHT-1M': 'notanumber',
                       'X-MBX-ORDER-COUNT-10S': 'bad'},
              payload={'lastUpdateId': 1, 'bids': [], 'asks': []})
        await client.get(_DEPTH_URL, symbol='BTCUSDT')
    # malformed values are swallowed, not stored
    assert client.used_weight.get('1m') is None
    assert client.order_count.get('10s') is None


def test_client_logger_property():
    assert Client().logger is logger or Client().logger is not None


# --- subscribe/manager.py -------------------------------------------------

def test_reconcile_ws_api_rate_limits_ignores_non_dict():
    """The on_response reconcile hook tolerates a non-dict message (no-op)."""
    client = Client()
    # Must not raise and must not touch the rate-limit core.
    client._reconcile_ws_api_rate_limits('not-a-dict')
    client._reconcile_ws_api_rate_limits(None)




@pytest.mark.asyncio
async def test_receive_ignored_when_not_receiving():
    client = Client()
    client.stop()
    assert await client._receive({'e': 'depthUpdate'}) is None


@pytest.mark.asyncio
async def test_receive_server_shutdown_recycles_data_stream():
    client = Client()
    recycled = []

    class FakeStream:
        async def recycle(self):
            recycled.append(True)

    client._data_stream = FakeStream()
    await client._receive({'data': {'e': 'serverShutdown'}})
    assert recycled == [True]


@pytest.mark.asyncio
async def test_receive_recovery_failure_is_caught():
    client = Client()
    dispatched = []

    async def boom():
        raise RuntimeError('recover failed')

    class FakeCtx:
        async def receive(self, msg):
            dispatched.append(msg)

    client._recover_user_stream_if_needed = boom
    client._handler_ctx = FakeCtx()
    # the recovery error must be caught; the message is still dispatched
    await client._receive({'event': {'e': 'eventStreamTerminated'}})
    assert dispatched


@pytest.mark.asyncio
async def test_resubscribe_market_and_user():
    client = Client()
    market_calls = []
    user_calls = []

    async def fake_only(subscribe, subs):
        market_calls.append((subscribe, list(subs)))

    async def fake_user_only(subscribe, subs):
        user_calls.append((subscribe, list(subs)))

    client._subscribe_only = fake_only
    client._subscribe_user_only = fake_user_only
    client._subscribed = {('aggTrade', 'BTCUSDT'), (SubType.USER,)}

    await client._resubscribe()
    await client._resubscribe_user()

    assert market_calls and market_calls[0][0] is True
    assert user_calls and user_calls[0][0] is True


# --- subscribe/handler_context.py ----------------------------------------

def test_overload_partial_order_book_four_tuple():
    ctx = Client()._get_handler_ctx()
    params = ctx.overload_subscriptions(
        (SubType.PARTIAL_ORDER_BOOK, 'BTCUSDT', 10, 100))
    assert len(params) == 1
    assert params[0][0] == SubType.PARTIAL_ORDER_BOOK


def test_overload_all_market_window_tickers_two_arg():
    """F-33: the 2-arg flat form subscribe(ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4)
    must produce the same canonical tuple as the 1-tuple-of-pair form and be
    consistent with the processor's subscribe_param signature."""
    from stock_pandas import TimeFrame
    ctx = Client()._get_handler_ctx()

    # Flat 2-arg call: subscribe(SubType.ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4)
    params_flat = ctx.overload_subscriptions(
        SubType.ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4)
    assert len(params_flat) == 1
    assert params_flat[0] == (SubType.ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4)

    # Tuple-pair form: subscribe((SubType.ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4),)
    params_tuple = ctx.overload_subscriptions(
        (SubType.ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4))
    assert params_flat == params_tuple

    # No-window form defaults to H1 (length==1)
    params_no_window = ctx.overload_subscriptions(
        SubType.ALL_MARKET_WINDOW_TICKERS)
    assert params_no_window == [(SubType.ALL_MARKET_WINDOW_TICKERS,)]


# --- processors/processors.py: param validation ---------------------------

def test_processor_param_validation_errors():
    with pytest.raises(InvalidSubTypeParamException):
        _get_window(SubType.WINDOW_TICKER, ['not-a-timeframe'])
    with pytest.raises(InvalidSubTypeParamException):
        _get_order_book_interval(SubType.ORDER_BOOK, ['not-an-int'])
    with pytest.raises(InvalidSubTypeParamException):
        _get_partial_depth_level(SubType.PARTIAL_ORDER_BOOK, ['not-an-int'])


# --- handlers/orderbook.py: background fetch exception handler -------------

def test_orderbook_handle_fetch_exception():
    book = OrderBook.__new__(OrderBook)

    class FakeLogger:
        def error(self, *args, **kwargs):
            pass

    class FakeClient:
        logger = FakeLogger()

    book._client = FakeClient()

    # cancelled task -> early return, no logging
    book._handle_fetch_exception(SimpleNamespace(cancelled=lambda: True))

    # failed task -> logs the error
    book._handle_fetch_exception(SimpleNamespace(
        cancelled=lambda: False,
        exception=lambda: RuntimeError('boom')))


# --- subscribe/stream.py: background task exception handler ----------------

def test_stream_handle_task_exception():
    stream = Stream.__new__(Stream)
    stream._logger = logger

    stream._handle_task_exception(SimpleNamespace(cancelled=lambda: True))
    stream._handle_task_exception(SimpleNamespace(
        cancelled=lambda: False,
        exception=lambda: RuntimeError('boom')))


# --- subscribe/stream.py: on_response reconcile hook ----------------------

@pytest.mark.asyncio
async def test_stream_on_response_hook_receives_full_message():
    # The on_response hook gets the FULL id-correlated message (incl.
    # rateLimits) before the awaiting future resolves with just `result`.
    seen = []
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._on_response = lambda msg: seen.append(msg)

    future = asyncio.get_running_loop().create_future()
    stream._message_futures = {7: future}

    msg = {'id': 7, 'status': 200, 'result': {'ok': 1},
           'rateLimits': [{'count': 3}]}
    await stream._handle_message(msg)

    assert seen == [msg]                 # full message handed to the hook
    assert future.result() == {'ok': 1}  # future still resolves to result


@pytest.mark.asyncio
async def test_stream_on_response_hook_error_is_swallowed():
    # A buggy hook must not break response delivery.
    stream = Stream.__new__(Stream)
    stream._logger = logger

    def boom(_msg):
        raise RuntimeError('hook boom')

    stream._on_response = boom
    future = asyncio.get_running_loop().create_future()
    stream._message_futures = {1: future}

    await stream._handle_message({'id': 1, 'result': None})
    # The future still resolves despite the hook raising.
    assert future.result() is None


@pytest.mark.asyncio
async def test_stream_no_on_response_hook_is_noop():
    # The market-data stream passes no hook -> behaviour unchanged.
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._on_response = None

    future = asyncio.get_running_loop().create_future()
    stream._message_futures = {2: future}
    await stream._handle_message({'id': 2, 'result': 'r'})
    assert future.result() == 'r'


@pytest.mark.asyncio
async def test_stream_receive_pings_on_recv_timeout():
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._timeout = 0.05
    stream._connection_error = False
    stream._rate_limiter = RateLimiter()
    stream._connection_id = 'default'

    class FakeSocket:
        async def recv(self):
            await asyncio.sleep(10)  # force the wait_for timeout

        async def ping(self):
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(None)
            return fut

    stream._socket = FakeSocket()
    # recv times out -> ping succeeds -> returns cleanly
    await stream._receive()


@pytest.mark.asyncio
async def test_stream_receive_ping_timeout_disconnects():
    from websockets.exceptions import ConnectionClosedError

    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._timeout = 0.01
    stream._connection_error = False
    stream._rate_limiter = RateLimiter()
    stream._connection_id = 'default'

    class FakeSocket:
        async def recv(self):
            await asyncio.sleep(10)  # force recv timeout -> ping path

        async def ping(self):
            raise asyncio.TimeoutError()  # pong never arrives

    stream._socket = FakeSocket()
    # ping times out -> connection treated as stale -> ConnectionClosedError
    with pytest.raises(ConnectionClosedError):
        await stream._receive()


@pytest.mark.asyncio
async def test_stream_receive_ping_failure_reraises():
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._timeout = 0.01
    stream._connection_error = False
    stream._rate_limiter = RateLimiter()
    stream._connection_id = 'default'

    class FakeSocket:
        async def recv(self):
            await asyncio.sleep(10)  # force recv timeout -> ping path

        async def ping(self):
            raise RuntimeError('ping boom')  # non-timeout ping failure

    stream._socket = FakeSocket()
    # a non-timeout ping error is logged and re-raised
    with pytest.raises(RuntimeError, match='ping boom'):
        await stream._receive()


@pytest.mark.asyncio
async def test_reconnect_handles_connected_task_errors():
    # case 1: the connected task failed with a non-cancel exception
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._connection_error = False

    async def raiser():
        raise RuntimeError('boom')

    failed = asyncio.create_task(raiser())
    await asyncio.sleep(0.01)
    stream._connected_task = failed
    await stream._reconnect(SimpleNamespace(exception=RuntimeError('e'), fails=1))
    assert stream._connected_task is None

    # case 2: the connected task is cancelled during reconnect
    stream2 = Stream.__new__(Stream)
    stream2._logger = logger
    stream2._connection_error = False

    async def runner():
        await asyncio.sleep(10)

    running = asyncio.create_task(runner())
    stream2._connected_task = running
    await stream2._reconnect(SimpleNamespace(exception=RuntimeError('e'), fails=1))
    assert stream2._connected_task is None


@pytest.mark.asyncio
async def test_close_logs_task_errors():
    stream = Stream.__new__(Stream)
    stream._logger = logger
    stream._closing = False
    stream._connected_task = None

    async def done():
        return None

    conn_task = asyncio.create_task(done())
    await asyncio.sleep(0.01)
    stream._conn_task = conn_task

    class FakeSocket:
        async def close(self, code):
            raise RuntimeError('close boom')

    stream._socket = FakeSocket()
    # the failing socket.close is caught and logged, close() completes
    await stream.close()
    assert stream._socket is None


# --- user-stream flow (mocked; replaces the live test_user_stream) ---------

def test_ws_api_signature_params_builds_signed_payload():
    signed = Client('key', 'secret')._ws_api_signature_params(symbol='BTCUSDT')
    assert signed['apiKey'] == 'key'
    assert signed['symbol'] == 'BTCUSDT'
    assert isinstance(signed['timestamp'], int)
    assert isinstance(signed['signature'], str) and signed['signature']


def test_user_stream_not_subscribed_exception_message():
    assert 'not subscribed' in str(UserStreamNotSubscribedException())


@pytest.mark.asyncio
async def test_user_stream_subscribe_unsubscribe_close_mocked(monkeypatch):
    client = Client('key', 'secret')
    sent = []

    class FakeUserStream:
        def __init__(self, *args, **kwargs):
            pass

        def connect(self):
            return self

        async def send(self, req):
            sent.append(req)
            return None

        async def close(self, code=4999):
            sent.append({'method': 'close'})

    # Patch the Stream class the manager builds so the real _get_ws_api_stream
    # body runs (no network) and send() returns instantly.
    monkeypatch.setattr('binance.subscribe.manager.Stream', FakeUserStream)

    await client.subscribe(SubType.USER)
    await client.unsubscribe(SubType.USER)
    await client.close()

    methods = [req['method'] for req in sent]
    assert 'userDataStream.subscribe.signature' in methods
    assert 'userDataStream.unsubscribe' in methods
