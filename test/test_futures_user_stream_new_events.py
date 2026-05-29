"""Tests for the futures user-data-stream event handlers.

Covers:
- TRADE_LITE (UM only) → FuturesTradeLiteHandlerBase
- STRATEGY_UPDATE (UM + CM) → FuturesStrategyUpdateHandlerBase
- GRID_UPDATE (UM + CM) → FuturesGridUpdateHandlerBase
- ALGO_UPDATE (UM only) → FuturesAlgoUpdateHandlerBase
- CONDITIONAL_ORDER_TRIGGER_REJECT — DEPRECATED & REMOVED (2025-12-10):
  conditional orders were migrated to the Algo Service; rejection reasons
  are now delivered inside ALGO_UPDATE's ``o.rm`` reject_message field.
  See https://developers.binance.com/docs/derivatives/change-log

Each event is driven through client._receive() on the appropriate mock client and
assertions confirm correct routing and field access.

SAFETY: MOCK-only — no live API calls.

Payload schemas confirmed from official Binance docs (2026-05-25):
- TRADE_LITE: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Trade-Lite
- STRATEGY_UPDATE: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-STRATEGY-UPDATE
- GRID_UPDATE: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-GRID-UPDATE
- ALGO_UPDATE: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Algo-Order-Update
"""

import pytest

from binance import (
    UMFuturesClient,
    CMFuturesClient,
    Credentials,
    FuturesTradeLiteHandlerBase,
    FuturesStrategyUpdateHandlerBase,
    FuturesGridUpdateHandlerBase,
    FuturesAlgoUpdateHandlerBase,
)
from binance.core.common.utils import create_future


# ---------------------------------------------------------------------------
# Canonical payloads (confirmed from official Binance docs, 2026-05-25)
# ---------------------------------------------------------------------------

TRADE_LITE_PAYLOAD = {
    'e': 'TRADE_LITE',
    'E': 1669262908218,
    'T': 1669262908216,
    's': 'BTCUSDT',
    'q': '0.001',
    'p': '9910',
    'm': False,
    'c': 'abc123',
    'S': 'SELL',
    'L': '9910',
    'l': '0.001',
    't': 1,
    'i': 8886774,
}

STRATEGY_UPDATE_PAYLOAD = {
    'e': 'STRATEGY_UPDATE',
    'T': 1669261797627,
    'E': 1669261797628,
    'su': {
        'si': 176054594,
        'st': 'GRID',
        'ss': 'NEW',
        's': 'BTCUSDT',
        'ut': 1669261797627,
        'c': 8007,
    },
}

GRID_UPDATE_PAYLOAD = {
    'e': 'GRID_UPDATE',
    'T': 1669262908216,
    'E': 1669262908218,
    'gu': {
        'si': 176057039,
        'st': 'GRID',
        'ss': 'WORKING',
        's': 'BTCUSDT',
        'r': '-0.00300716',
        'up': '16720',
        'uq': '-0.001',
        'uf': '-0.00300716',
        'mp': '0.0',
        'ut': 1669262908197,
    },
}

ALGO_UPDATE_PAYLOAD = {
    'e': 'ALGO_UPDATE',
    'T': 1750515742297,
    'E': 1750515742303,
    'o': {
        'caid': 'Q5xaq5EGKgXXa0fD7fs0Ip',
        'aid': 2148719,
        'at': 'CONDITIONAL',
        'o': 'TAKE_PROFIT',
        's': 'BNBUSDT',
        'S': 'SELL',
        'ps': 'BOTH',
        'f': 'GTC',
        'q': '0.01',
        'X': 'CANCELED',
        'ai': '',
        'ap': '0.00000',
        'aq': '0.00000',
        'act': '0',
        'tp': '750',
        'p': '750',
        'V': 'EXPIRE_MAKER',
        'wt': 'CONTRACT_PRICE',
        'pm': 'NONE',
        'cp': False,
        'pP': False,
        'R': False,
        'tt': 0,
        'gtd': 0,
        'rm': 'Reduce Only reject',
    },
}


# ---------------------------------------------------------------------------
# Helper: drive a payload through client._receive and capture what the handler
# receives.  Mirrors run_futures_handler in test_futures_user_stream.py.
# ---------------------------------------------------------------------------

async def _run_handler(client, HandlerBase, payload, envelope='event'):
    """Drive *payload* through client._receive and return what the handler sees.

    Args:
        client: a started futures client (UM or CM)
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
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def um_client():
    return UMFuturesClient(Credentials('api_key', 'api_secret')).start()


@pytest.fixture
def cm_client():
    return CMFuturesClient(Credentials('api_key', 'api_secret')).start()


# ---------------------------------------------------------------------------
# TRADE_LITE — UM only; confirm event type + all top-level fields
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trade_lite_routed_on_um_event_envelope(um_client):
    """TRADE_LITE in WS-API 'event' envelope routes to FuturesTradeLiteHandlerBase on UM."""
    received = await _run_handler(
        um_client, FuturesTradeLiteHandlerBase, TRADE_LITE_PAYLOAD, envelope='event'
    )
    assert received['e'] == 'TRADE_LITE'
    assert received['s'] == 'BTCUSDT'
    assert received['S'] == 'SELL'
    assert received['L'] == '9910'
    assert received['l'] == '0.001'
    assert received['t'] == 1
    assert received['i'] == 8886774
    assert received['m'] is False
    assert received['c'] == 'abc123'


@pytest.mark.asyncio
async def test_trade_lite_routed_on_um_data_envelope(um_client):
    """TRADE_LITE in data-stream 'data' envelope routes to FuturesTradeLiteHandlerBase on UM."""
    received = await _run_handler(
        um_client, FuturesTradeLiteHandlerBase, TRADE_LITE_PAYLOAD, envelope='data'
    )
    assert received['e'] == 'TRADE_LITE'
    assert received['s'] == 'BTCUSDT'
    assert received['q'] == '0.001'
    assert received['p'] == '9910'


# ---------------------------------------------------------------------------
# STRATEGY_UPDATE — UM + CM; confirm nested 'su' object
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_strategy_update_routed_on_um(um_client):
    """STRATEGY_UPDATE routes to FuturesStrategyUpdateHandlerBase on UM client."""
    received = await _run_handler(
        um_client, FuturesStrategyUpdateHandlerBase, STRATEGY_UPDATE_PAYLOAD
    )
    assert received['e'] == 'STRATEGY_UPDATE'
    assert received['su']['si'] == 176054594
    assert received['su']['st'] == 'GRID'
    assert received['su']['ss'] == 'NEW'
    assert received['su']['s'] == 'BTCUSDT'
    assert received['su']['c'] == 8007


@pytest.mark.asyncio
async def test_strategy_update_routed_on_cm(cm_client):
    """STRATEGY_UPDATE routes to FuturesStrategyUpdateHandlerBase on CM client."""
    received = await _run_handler(
        cm_client, FuturesStrategyUpdateHandlerBase, STRATEGY_UPDATE_PAYLOAD
    )
    assert received['e'] == 'STRATEGY_UPDATE'
    assert received['su']['si'] == 176054594
    assert received['su']['ss'] == 'NEW'


# ---------------------------------------------------------------------------
# GRID_UPDATE — UM + CM; confirm nested 'gu' object
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_grid_update_routed_on_um(um_client):
    """GRID_UPDATE routes to FuturesGridUpdateHandlerBase on UM client."""
    received = await _run_handler(
        um_client, FuturesGridUpdateHandlerBase, GRID_UPDATE_PAYLOAD
    )
    assert received['e'] == 'GRID_UPDATE'
    assert received['gu']['si'] == 176057039
    assert received['gu']['st'] == 'GRID'
    assert received['gu']['ss'] == 'WORKING'
    assert received['gu']['s'] == 'BTCUSDT'
    assert received['gu']['r'] == '-0.00300716'
    assert received['gu']['up'] == '16720'
    assert received['gu']['uq'] == '-0.001'
    assert received['gu']['uf'] == '-0.00300716'
    assert received['gu']['mp'] == '0.0'


@pytest.mark.asyncio
async def test_grid_update_routed_on_cm(cm_client):
    """GRID_UPDATE routes to FuturesGridUpdateHandlerBase on CM client."""
    received = await _run_handler(
        cm_client, FuturesGridUpdateHandlerBase, GRID_UPDATE_PAYLOAD
    )
    assert received['e'] == 'GRID_UPDATE'
    assert received['gu']['si'] == 176057039
    assert received['gu']['ss'] == 'WORKING'


# ---------------------------------------------------------------------------
# CONDITIONAL_ORDER_TRIGGER_REJECT — REMOVED (2025-12-10)
#
# Binance migrated conditional orders to the Algo Service.  Conditional order
# rejection reasons are now delivered inside ``ALGO_UPDATE``'s ``o.rm`` field
# (reject_message).  The SDK drops the legacy handler entirely — there is no
# backward-compatibility shim and the SDK will not accept the old class name.
#
# Source: https://developers.binance.com/docs/derivatives/change-log
# Replacement source: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Algo-Order-Update
# ---------------------------------------------------------------------------


def test_conditional_order_trigger_reject_handler_no_longer_importable_from_binance_root():
    """FuturesConditionalOrderTriggerRejectHandlerBase MUST NOT be importable from binance root."""
    import binance

    assert not hasattr(binance, 'FuturesConditionalOrderTriggerRejectHandlerBase')


def test_conditional_order_trigger_reject_handler_no_longer_importable_from_user_handlers():
    """FuturesConditionalOrderTriggerRejectHandlerBase MUST NOT exist on the user_handlers module."""
    from binance.futures import user_handlers

    assert not hasattr(user_handlers, 'FuturesConditionalOrderTriggerRejectHandlerBase')


def test_conditional_order_trigger_reject_handler_explicit_import_raises_importerror():
    """An explicit ``from binance.futures.user_handlers import ...`` of the removed class MUST raise ImportError."""
    with pytest.raises(ImportError):
        from binance.futures.user_handlers import (  # noqa: F401
            FuturesConditionalOrderTriggerRejectHandlerBase,
        )


def test_conditional_order_trigger_reject_not_in_processor_payload_types():
    """FuturesUserProcessor.PAYLOAD_TYPES MUST NOT carry 'CONDITIONAL_ORDER_TRIGGER_REJECT'."""
    from binance.futures.user_processor import FuturesUserProcessor

    assert 'CONDITIONAL_ORDER_TRIGGER_REJECT' not in FuturesUserProcessor.PAYLOAD_TYPES


# ---------------------------------------------------------------------------
# ALGO_UPDATE — UM only; confirm nested 'o' object and key fields
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_algo_update_routed_on_um_event_envelope(um_client):
    """ALGO_UPDATE in 'event' envelope routes to FuturesAlgoUpdateHandlerBase on UM."""
    received = await _run_handler(
        um_client, FuturesAlgoUpdateHandlerBase, ALGO_UPDATE_PAYLOAD, envelope='event'
    )
    assert received['e'] == 'ALGO_UPDATE'
    assert received['o']['caid'] == 'Q5xaq5EGKgXXa0fD7fs0Ip'
    assert received['o']['aid'] == 2148719
    assert received['o']['at'] == 'CONDITIONAL'
    assert received['o']['s'] == 'BNBUSDT'
    assert received['o']['S'] == 'SELL'
    assert received['o']['X'] == 'CANCELED'
    assert received['o']['rm'] == 'Reduce Only reject'


@pytest.mark.asyncio
async def test_algo_update_routed_on_um_data_envelope(um_client):
    """ALGO_UPDATE in 'data' envelope routes to FuturesAlgoUpdateHandlerBase on UM."""
    received = await _run_handler(
        um_client, FuturesAlgoUpdateHandlerBase, ALGO_UPDATE_PAYLOAD, envelope='data'
    )
    assert received['e'] == 'ALGO_UPDATE'
    assert received['o']['aid'] == 2148719
    assert received['o']['X'] == 'CANCELED'


# ---------------------------------------------------------------------------
# ALGO_UPDATE docstring must describe ai/act/tt per Binance docs:
#   ai  = "Order ID in matching engine (string; empty when not triggered)"
#   act = "Actual order type in matching engine"
#   tt  = "Trigger time" (ms timestamp)
# https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Algo-Order-Update
# ---------------------------------------------------------------------------


def test_algo_update_docstring_describes_ai_as_order_id_matching_engine():
    """FuturesAlgoUpdateHandlerBase docstring must call ``ai`` the matching-engine order id, NOT 'algo_info'."""
    doc = FuturesAlgoUpdateHandlerBase.__doc__ or ''
    # Stale mislabel must be gone.
    assert 'algo_info' not in doc
    # Docs-correct meaning must be present.
    assert 'order_id_matching_engine' in doc


def test_algo_update_docstring_describes_act_as_actual_order_type():
    """FuturesAlgoUpdateHandlerBase docstring must call ``act`` the actual order type, NOT 'algo_cancel_type'."""
    doc = FuturesAlgoUpdateHandlerBase.__doc__ or ''
    assert 'algo_cancel_type' not in doc
    assert 'actual_order_type' in doc


def test_algo_update_docstring_describes_tt_as_trigger_time():
    """FuturesAlgoUpdateHandlerBase docstring must call ``tt`` the trigger time, NOT 'trailing_type'."""
    doc = FuturesAlgoUpdateHandlerBase.__doc__ or ''
    assert 'trailing_type' not in doc
    assert 'trigger_time' in doc


# ---------------------------------------------------------------------------
# Public API: all 5 new handler bases importable from top-level 'binance' package
# ---------------------------------------------------------------------------

def test_new_handler_bases_importable_from_binance():
    """All current futures user handler bases are importable from the binance root package."""
    import binance

    assert hasattr(binance, 'FuturesTradeLiteHandlerBase')
    assert hasattr(binance, 'FuturesStrategyUpdateHandlerBase')
    assert hasattr(binance, 'FuturesGridUpdateHandlerBase')
    assert hasattr(binance, 'FuturesAlgoUpdateHandlerBase')


def test_new_handler_bases_are_handler_subclasses():
    """All current handler bases are subclasses of core Handler."""
    from binance.core.handlers.base import Handler

    assert issubclass(FuturesTradeLiteHandlerBase, Handler)
    assert issubclass(FuturesStrategyUpdateHandlerBase, Handler)
    assert issubclass(FuturesGridUpdateHandlerBase, Handler)
    assert issubclass(FuturesAlgoUpdateHandlerBase, Handler)


# ---------------------------------------------------------------------------
# FuturesUserProcessor routing table covers the current event types
# ---------------------------------------------------------------------------

def test_processor_payload_types_include_new_events():
    """FuturesUserProcessor.PAYLOAD_TYPES must contain the current event type strings."""
    from binance.futures.user_processor import FuturesUserProcessor

    types = FuturesUserProcessor.PAYLOAD_TYPES
    assert 'TRADE_LITE' in types
    assert 'STRATEGY_UPDATE' in types
    assert 'GRID_UPDATE' in types
    assert 'ALGO_UPDATE' in types


def test_processor_handlers_include_new_handler_bases():
    """FuturesUserProcessor.HANDLERS must contain the current handler base classes.

    Import both modules fresh inside the test to avoid class-identity mismatches
    caused by module-reload tests in test_futures_user_stream.py.
    """
    import importlib
    import sys

    for mod_name in ('binance.futures.user_handlers', 'binance.futures.user_processor'):
        sys.modules.pop(mod_name, None)

    user_handlers = importlib.import_module('binance.futures.user_handlers')
    user_processor = importlib.import_module('binance.futures.user_processor')

    handlers = user_processor.FuturesUserProcessor.HANDLERS
    assert user_handlers.FuturesTradeLiteHandlerBase in handlers
    assert user_handlers.FuturesStrategyUpdateHandlerBase in handlers
    assert user_handlers.FuturesGridUpdateHandlerBase in handlers
    assert user_handlers.FuturesAlgoUpdateHandlerBase in handlers


def test_processor_payload_types_and_handlers_aligned():
    """PAYLOAD_TYPES and HANDLERS must be the same length (one-to-one mapping)."""
    from binance.futures.user_processor import FuturesUserProcessor

    assert len(FuturesUserProcessor.PAYLOAD_TYPES) == len(FuturesUserProcessor.HANDLERS)


# ---------------------------------------------------------------------------
# is_message_type matches new event types in both envelopes
# ---------------------------------------------------------------------------

def test_is_message_type_trade_lite_event_envelope():
    """is_message_type matches TRADE_LITE in WS-API 'event' envelope."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'subscriptionId': 0, 'event': {'e': 'TRADE_LITE', 'E': 1}}
    matched, payload = proc.is_message_type(msg)

    assert matched is True
    assert payload['e'] == 'TRADE_LITE'


def test_is_message_type_strategy_update_data_envelope():
    """is_message_type matches STRATEGY_UPDATE in data-stream 'data' envelope."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = CMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'data': {'e': 'STRATEGY_UPDATE', 'T': 1}, 'stream': 'fake'}
    matched, payload = proc.is_message_type(msg)

    assert matched is True
    assert payload['e'] == 'STRATEGY_UPDATE'


def test_is_message_type_grid_update_data_envelope():
    """is_message_type matches GRID_UPDATE in data-stream 'data' envelope."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = CMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'data': {'e': 'GRID_UPDATE', 'T': 1}, 'stream': 'fake'}
    matched, payload = proc.is_message_type(msg)

    assert matched is True
    assert payload['e'] == 'GRID_UPDATE'


def test_is_message_type_conditional_order_trigger_reject_no_longer_matches():
    """is_message_type MUST NOT match CONDITIONAL_ORDER_TRIGGER_REJECT (event removed 2025-12-10)."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'data': {'e': 'CONDITIONAL_ORDER_TRIGGER_REJECT', 'E': 1}, 'stream': 'fake'}
    matched, payload = proc.is_message_type(msg)

    assert matched is False
    assert payload is None


def test_is_message_type_algo_update():
    """is_message_type matches ALGO_UPDATE."""
    from binance.futures.user_processor import FuturesUserProcessor

    client = UMFuturesClient(Credentials('key', 'secret'))
    proc = FuturesUserProcessor(client)

    msg = {'data': {'e': 'ALGO_UPDATE', 'T': 1}, 'stream': 'fake'}
    matched, payload = proc.is_message_type(msg)

    assert matched is True
    assert payload['e'] == 'ALGO_UPDATE'


# ---------------------------------------------------------------------------
# Unrelated payloads are NOT delivered to new handlers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unrelated_payload_not_delivered_to_trade_lite_handler(um_client):
    """A non-TRADE_LITE payload must not reach FuturesTradeLiteHandlerBase."""
    delivered = []

    class Handler(FuturesTradeLiteHandlerBase):
        def receive(self, p):
            delivered.append(p)

    um_client.start()
    um_client.handler(Handler())

    await um_client._receive({'data': {'e': 'markPriceUpdate', 's': 'BTCUSDT'}})

    assert delivered == []


@pytest.mark.asyncio
async def test_unrelated_payload_not_delivered_to_algo_update_handler(um_client):
    """A non-ALGO_UPDATE payload must not reach FuturesAlgoUpdateHandlerBase."""
    delivered = []

    class Handler(FuturesAlgoUpdateHandlerBase):
        def receive(self, p):
            delivered.append(p)

    um_client.start()
    um_client.handler(Handler())

    await um_client._receive({'data': {'e': 'ORDER_TRADE_UPDATE', 'T': 1}})

    assert delivered == []
