"""Tests for the USDⓈ-M Futures user-data-stream handlers and processor.

Covers:
- Each event type (ACCOUNT_UPDATE, ORDER_TRADE_UPDATE, MARGIN_CALL,
  ACCOUNT_CONFIG_UPDATE, listenKeyExpired, eventStreamTerminated) is routed to
  the correct handler base via client._receive().
- FuturesUserProcessor routes by 'e' key (event type) within both WS-API
  'event' envelope and data-stream 'data' envelope.
- subscribe_param returns signed WS-API params on subscribe and {} on
  unsubscribe (mirrors the Spot UserProcessor contract).
- Layering: all handler bases are importable from the top-level 'binance'
  package.
- The futures user processor sends the same WS-API subscribe method as Spot
  (userDataStream.subscribe.signature / userDataStream.unsubscribe) when
  subscribe(SubType.USER) is called on a UMFuturesClient.

SAFETY: MOCK-only — no live API calls.
"""

import pytest

from binance import (
    UMFuturesClient,
    Credentials,
    SubType,
    # Public handler bases importable from binance root:
    FuturesAccountUpdateHandlerBase,
    FuturesOrderUpdateHandlerBase,
    FuturesMarginCallHandlerBase,
    FuturesAccountConfigUpdateHandlerBase,
    FuturesListenKeyExpiredHandlerBase,
    FuturesEventStreamTerminatedHandlerBase,
)
from binance.core.common.utils import create_future


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return UMFuturesClient(Credentials('api_key', 'api_secret')).start()


# ---------------------------------------------------------------------------
# Canonical payloads (confirmed from Binance USDⓈ-M docs, 2026-05-25)
# ---------------------------------------------------------------------------

ACCOUNT_UPDATE_PAYLOAD = {
    'e': 'ACCOUNT_UPDATE',
    'E': 1564745798939,
    'T': 1564745798938,
    'a': {
        'm': 'ORDER',
        'B': [
            {
                'a': 'USDT',
                'wb': '122624.12345678',
                'cw': '100.12345678',
                'bc': '50.12345678',
            }
        ],
        'P': [
            {
                's': 'BTCUSDT',
                'pa': '0',
                'ep': '0.00000',
                'bep': '0',
                'cr': '200',
                'up': '0',
                'mt': 'isolated',
                'iw': '0.00000000',
                'ps': 'BOTH',
            }
        ],
    },
}

ORDER_TRADE_UPDATE_PAYLOAD = {
    'e': 'ORDER_TRADE_UPDATE',
    'E': 1568879465651,
    'T': 1568879465650,
    'o': {
        's': 'BTCUSDT',
        'c': 'abc123',
        'S': 'SELL',
        'o': 'LIMIT',
        'f': 'GTC',
        'q': '0.001',
        'p': '9910',
        'ap': '9910',
        'sp': '0',
        'x': 'TRADE',
        'X': 'FILLED',
        'i': 8886774,
        'l': '0.001',
        'z': '0.001',
        'L': '9910',
        'N': 'BNB',
        'n': '0.01',
        'T': 1568879465651,
        't': 1,
        'b': '0',
        'a': '0',
        'm': False,
        'R': False,
        'wt': 'CONTRACT_PRICE',
        'ot': 'LIMIT',
        'ps': 'BOTH',
        'cp': False,
        'rp': '0',
        'pP': False,
        'V': 'NONE',
        'pm': 'NONE',
        'gtd': 0,
    },
}

MARGIN_CALL_PAYLOAD = {
    'e': 'MARGIN_CALL',
    'E': 1587727187525,
    'cw': '3.16812045',
    'p': [
        {
            's': 'ETHUSDT',
            'ps': 'LONG',
            'pa': '1.327',
            'mt': 'CROSSED',
            'iw': '0',
            'mp': '187.17127',
            'up': '-1.166074',
            'mm': '1.614445',
        }
    ],
}

ACCOUNT_CONFIG_UPDATE_LEVERAGE_PAYLOAD = {
    'e': 'ACCOUNT_CONFIG_UPDATE',
    'E': 1611646737479,
    'T': 1611646737476,
    'ac': {
        's': 'BTCUSDT',
        'l': 25,
    },
}

ACCOUNT_CONFIG_UPDATE_MULTIASSETS_PAYLOAD = {
    'e': 'ACCOUNT_CONFIG_UPDATE',
    'E': 1611646737479,
    'T': 1611646737476,
    'ai': {
        'j': True,
    },
}

LISTEN_KEY_EXPIRED_PAYLOAD = {
    'e': 'listenKeyExpired',
    'E': 1736996475556,
    'listenKey': 'WsCMN0a4KHUPTQuX6IUnqEZfB1inxmv1qR4kbf1Luabcd',
}

EVENT_STREAM_TERMINATED_PAYLOAD = {
    'e': 'eventStreamTerminated',
    'E': 1700000000000,
}


# ---------------------------------------------------------------------------
# Helper: drive a payload through client._receive and capture what the handler
# receives.  Mirrors test_handlers.py `run_handler` but for futures clients.
# ---------------------------------------------------------------------------

async def run_futures_handler(client, HandlerBase, payload, envelope='event'):
    """Drive *payload* through client._receive and return what the handler sees.

    Args:
        client: a started UMFuturesClient
        HandlerBase: the handler base class to instantiate
        payload: the inner event dict (with 'e' key)
        envelope: 'event' wraps in {'event': payload} (WS-API form);
                  'data' wraps in {'data': payload, 'stream': 'fake'}
    """
    future = create_future()

    class Handler(HandlerBase):
        def receive(self, p):
            p = super().receive(p)
            if not future.done():
                future.set_result(p)

    client.start()
    client.handler(Handler())

    if envelope == 'event':
        msg = {'subscriptionId': 0, 'event': payload}
    else:
        msg = {'data': payload, 'stream': 'fake'}

    await client._receive(msg)

    return await future


# ---------------------------------------------------------------------------
# Routing tests: each event type → correct handler
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_account_update_routed_ws_api_envelope(client):
    """ACCOUNT_UPDATE in WS-API 'event' envelope routes to FuturesAccountUpdateHandlerBase."""
    received = await run_futures_handler(
        client, FuturesAccountUpdateHandlerBase, ACCOUNT_UPDATE_PAYLOAD, envelope='event'
    )
    assert received['e'] == 'ACCOUNT_UPDATE'
    assert received['a']['m'] == 'ORDER'
    assert received['a']['B'][0]['a'] == 'USDT'
    assert received['a']['P'][0]['s'] == 'BTCUSDT'


@pytest.mark.asyncio
async def test_account_update_routed_data_envelope(client):
    """ACCOUNT_UPDATE in data-stream 'data' envelope routes to FuturesAccountUpdateHandlerBase."""
    received = await run_futures_handler(
        client, FuturesAccountUpdateHandlerBase, ACCOUNT_UPDATE_PAYLOAD, envelope='data'
    )
    assert received['e'] == 'ACCOUNT_UPDATE'
    assert received['a']['m'] == 'ORDER'


@pytest.mark.asyncio
async def test_order_trade_update_routed(client):
    """ORDER_TRADE_UPDATE routes to FuturesOrderUpdateHandlerBase; raw dict returned."""
    received = await run_futures_handler(
        client, FuturesOrderUpdateHandlerBase, ORDER_TRADE_UPDATE_PAYLOAD
    )
    assert received['e'] == 'ORDER_TRADE_UPDATE'
    assert received['o']['s'] == 'BTCUSDT'
    assert received['o']['S'] == 'SELL'
    assert received['o']['X'] == 'FILLED'


@pytest.mark.asyncio
async def test_margin_call_routed(client):
    """MARGIN_CALL routes to FuturesMarginCallHandlerBase."""
    received = await run_futures_handler(
        client, FuturesMarginCallHandlerBase, MARGIN_CALL_PAYLOAD
    )
    assert received['e'] == 'MARGIN_CALL'
    assert received['p'][0]['s'] == 'ETHUSDT'
    assert received['p'][0]['mt'] == 'CROSSED'


@pytest.mark.asyncio
async def test_account_config_update_leverage_routed(client):
    """ACCOUNT_CONFIG_UPDATE (leverage variant) routes to FuturesAccountConfigUpdateHandlerBase."""
    received = await run_futures_handler(
        client, FuturesAccountConfigUpdateHandlerBase,
        ACCOUNT_CONFIG_UPDATE_LEVERAGE_PAYLOAD
    )
    assert received['e'] == 'ACCOUNT_CONFIG_UPDATE'
    assert received['ac']['s'] == 'BTCUSDT'
    assert received['ac']['l'] == 25


@pytest.mark.asyncio
async def test_account_config_update_multiassets_routed(client):
    """ACCOUNT_CONFIG_UPDATE (multi-assets variant) routes to FuturesAccountConfigUpdateHandlerBase."""
    received = await run_futures_handler(
        client, FuturesAccountConfigUpdateHandlerBase,
        ACCOUNT_CONFIG_UPDATE_MULTIASSETS_PAYLOAD
    )
    assert received['e'] == 'ACCOUNT_CONFIG_UPDATE'
    assert received['ai']['j'] is True


@pytest.mark.asyncio
async def test_listen_key_expired_routed(client):
    """listenKeyExpired routes to FuturesListenKeyExpiredHandlerBase."""
    received = await run_futures_handler(
        client, FuturesListenKeyExpiredHandlerBase, LISTEN_KEY_EXPIRED_PAYLOAD
    )
    assert received['e'] == 'listenKeyExpired'
    assert 'listenKey' in received


@pytest.mark.asyncio
async def test_event_stream_terminated_routed(client):
    """eventStreamTerminated routes to FuturesEventStreamTerminatedHandlerBase."""
    received = await run_futures_handler(
        client, FuturesEventStreamTerminatedHandlerBase, EVENT_STREAM_TERMINATED_PAYLOAD
    )
    assert received['e'] == 'eventStreamTerminated'


# ---------------------------------------------------------------------------
# Unrelated payloads are NOT delivered to futures user handlers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unrelated_payload_not_delivered(client):
    """A non-user-stream payload must not reach a futures user handler."""
    delivered = []

    class Handler(FuturesAccountUpdateHandlerBase):
        def receive(self, p):
            delivered.append(p)

    client.start()
    client.handler(Handler())

    # A spot ticker payload — not a futures user event.
    await client._receive({'data': {'e': '24hrTicker', 's': 'BTCUSDT'}})

    assert delivered == []


# ---------------------------------------------------------------------------
# subscribe_param contract (mirrors Spot UserProcessor behaviour)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_futures_user_processor_subscribe_param_returns_signed_params(client):
    """subscribe_param(True, ...) returns a dict with apiKey and signature."""
    from binance.futures.user_processor import FuturesUserProcessor

    proc = FuturesUserProcessor(client)
    params = await proc.subscribe_param(True, SubType.USER)

    assert isinstance(params, dict)
    assert 'apiKey' in params
    assert 'signature' in params
    assert 'timestamp' in params
    assert proc._subscribed is True


@pytest.mark.asyncio
async def test_futures_user_processor_unsubscribe_param_returns_empty(client):
    """subscribe_param(False, ...) after subscribe returns {}."""
    from binance.futures.user_processor import FuturesUserProcessor

    proc = FuturesUserProcessor(client)
    await proc.subscribe_param(True, SubType.USER)  # subscribe first
    params = await proc.subscribe_param(False, SubType.USER)

    assert params == {}
    assert proc._subscribed is False


@pytest.mark.asyncio
async def test_futures_user_processor_unsubscribe_before_subscribe_raises(client):
    """subscribe_param(False, ...) without prior subscribe raises UserStreamNotSubscribedException."""
    from binance.futures.user_processor import FuturesUserProcessor
    from binance import UserStreamNotSubscribedException

    proc = FuturesUserProcessor(client)

    with pytest.raises(UserStreamNotSubscribedException):
        await proc.subscribe_param(False, SubType.USER)


# ---------------------------------------------------------------------------
# subscribe(SubType.USER) sends userDataStream.subscribe.signature over WS-API
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_um_subscribe_user_sends_correct_ws_api_method(monkeypatch):
    """subscribe(SubType.USER) on UMFuturesClient sends userDataStream.subscribe.signature."""
    client = UMFuturesClient(Credentials('api_key', 'api_secret'))
    sent = []

    class FakeStream:
        def __init__(self, *args, **kwargs):
            pass

        def connect(self):
            return self

        async def send(self, req):
            sent.append(req)
            return None

        async def close(self, code=4999):
            sent.append({'method': 'close'})

    monkeypatch.setattr('binance.core.transport.subscription.Stream', FakeStream)

    await client.subscribe(SubType.USER)
    await client.unsubscribe(SubType.USER)
    await client.close()

    methods = [req.get('method') for req in sent]
    assert 'userDataStream.subscribe.signature' in methods
    assert 'userDataStream.unsubscribe' in methods


# ---------------------------------------------------------------------------
# Public API: all handler bases importable from top-level 'binance' package
# ---------------------------------------------------------------------------

def test_handler_bases_importable_from_binance():
    """All futures user handler bases are importable from the binance root package."""
    import binance

    assert hasattr(binance, 'FuturesAccountUpdateHandlerBase')
    assert hasattr(binance, 'FuturesOrderUpdateHandlerBase')
    assert hasattr(binance, 'FuturesMarginCallHandlerBase')
    assert hasattr(binance, 'FuturesAccountConfigUpdateHandlerBase')
    assert hasattr(binance, 'FuturesListenKeyExpiredHandlerBase')
    assert hasattr(binance, 'FuturesEventStreamTerminatedHandlerBase')


def test_handler_bases_are_handler_subclasses():
    """All futures user handler bases are subclasses of core Handler."""
    from binance.core.handlers.base import Handler

    assert issubclass(FuturesAccountUpdateHandlerBase, Handler)
    assert issubclass(FuturesOrderUpdateHandlerBase, Handler)
    assert issubclass(FuturesMarginCallHandlerBase, Handler)
    assert issubclass(FuturesAccountConfigUpdateHandlerBase, Handler)
    assert issubclass(FuturesListenKeyExpiredHandlerBase, Handler)
    assert issubclass(FuturesEventStreamTerminatedHandlerBase, Handler)


# ---------------------------------------------------------------------------
# Layering: futures handlers must NOT import from binance.spot
# ---------------------------------------------------------------------------

def test_futures_user_handlers_no_spot_import():
    """futures/user_handlers.py must not import from binance.spot."""
    import importlib
    import sys

    # Reload the module fresh to check its actual imports
    mod_name = 'binance.futures.user_handlers'
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    mod = importlib.import_module(mod_name)

    # binance.spot may already be loaded via other imports; what matters is
    # that user_handlers itself does not depend on spot.
    assert not any('spot' in str(dep) for dep in getattr(mod, '__dict__', {}).values()
                   if hasattr(dep, '__module__') and dep.__module__ is not None
                   and 'spot' in dep.__module__)


def test_futures_user_processor_no_spot_import():
    """futures/user_processor.py must not import from binance.spot."""
    import importlib
    import sys

    mod_name = 'binance.futures.user_processor'
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    mod = importlib.import_module(mod_name)

    # No binance.spot dependency in the module's own namespace
    assert not any(
        hasattr(v, '__module__') and v.__module__ is not None and 'spot' in v.__module__
        for v in mod.__dict__.values()
    )


# ---------------------------------------------------------------------------
# FuturesUserProcessor.is_message_type correctness
# ---------------------------------------------------------------------------

def test_is_message_type_event_envelope():
    """is_message_type matches WS-API 'event' envelope for ACCOUNT_UPDATE."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'subscriptionId': 0, 'event': {'e': 'ACCOUNT_UPDATE', 'T': 1}}
    matched, payload = proc.is_message_type(msg)

    assert matched is True
    assert payload['e'] == 'ACCOUNT_UPDATE'


def test_is_message_type_data_envelope():
    """is_message_type matches data-stream 'data' envelope for ORDER_TRADE_UPDATE."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'data': {'e': 'ORDER_TRADE_UPDATE', 'T': 1}, 'stream': 'fake'}
    matched, payload = proc.is_message_type(msg)

    assert matched is True
    assert payload['e'] == 'ORDER_TRADE_UPDATE'


def test_is_message_type_no_match():
    """is_message_type returns (False, None) for non-user-stream payloads."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'data': {'e': 'markPriceUpdate', 's': 'BTCUSDT'}}
    matched, payload = proc.is_message_type(msg)

    assert matched is False
    assert payload is None
