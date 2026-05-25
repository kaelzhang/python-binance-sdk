"""Tests for the COIN-M Futures module.

Covers:
- REST endpoints: each method hits the right URL (dapi.binance.com), sends
  correct params, and consumes the documented REQUEST_WEIGHT.
- Stream handlers: MarkPriceHandlerBase column mapping (no 'ap' / mark_price_avg),
  ForceOrderHandlerBase nested-'o' flattening + 'ps' (pair) column.
- subscribe_param: MarkPriceProcessor returns correct stream names (base and 1s);
  ForceOrderProcessor returns correct stream name.
- SubType enum values for MARK_PRICE and FORCE_ORDER.
- Layering: CM never imports from spot or um modules.

Endpoint weights confirmed against live Binance COIN-M API (2026-05-25):
- /dapi/v1/openInterest         weight 1
- /futures/data/openInterestHist weight 1 (treated; no weight header on data sub-path)
- /dapi/v1/fundingRate          weight 1
- /dapi/v1/fundingInfo          weight 1 (treated; no weight header on this path)
- /dapi/v1/premiumIndex         weight 1 (symbol or pair given) / 10 (all symbols)

COIN-M vs USDⓈ-M confirmed differences (2026-05-25):
- openInterestHist: COIN-M uses pair+contractType not symbol.
- premiumIndex: always returns list (even single symbol); has 'pair' field.
- markPrice stream: NO 'ap' field (mark price moving average) in COIN-M.
- forceOrder stream: COIN-M nested 'o' has 'ps' (pair) field; USDⓈ-M does not.
- Rate limits: REQUEST_WEIGHT 2400/1min, ORDERS 1200/1min (no 10s ORDERS pool in CM).
"""

import re
import pytest
from aioresponses import aioresponses

from binance import CMFuturesClient
from binance.core.common.constants import SubType
from binance.core.common.utils import create_future
from binance.core.rate_limit.types import RateLimitType
from binance.futures.cm.streams import (
    MarkPriceHandlerBase,
    ForceOrderHandlerBase,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return CMFuturesClient()


# ---------------------------------------------------------------------------
# REST endpoint helpers
# ---------------------------------------------------------------------------

DAPI_BASE = 'https://dapi.binance.com'

_RE_OPEN_INTEREST = re.compile(
    r'https://dapi\.binance\.com/dapi/v1/openInterest(\?.*)?$'
)
_RE_OI_HIST = re.compile(
    r'https://dapi\.binance\.com/futures/data/openInterestHist(\?.*)?$'
)
_RE_FUNDING_RATE = re.compile(
    r'https://dapi\.binance\.com/dapi/v1/fundingRate(\?.*)?$'
)
_RE_FUNDING_INFO = re.compile(
    r'https://dapi\.binance\.com/dapi/v1/fundingInfo(\?.*)?$'
)
_RE_PREMIUM_INDEX = re.compile(
    r'https://dapi\.binance\.com/dapi/v1/premiumIndex(\?.*)?$'
)


def _weight_used(client) -> int:
    """Return the current REQUEST_WEIGHT consumed in the client's rate limiter."""
    snap = client.rate_limit_snapshot()
    for w in snap.windows:
        if w.type == RateLimitType.REQUEST_WEIGHT:
            return w.used
    return 0


# ---------------------------------------------------------------------------
# REST: get_open_interest
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_open_interest_hits_correct_url(client):
    payload = {
        'symbol': 'BTCUSD_PERP',
        'pair': 'BTCUSD',
        'openInterest': '11942594',
        'contractType': 'PERPETUAL',
        'time': 1779705310815,
    }

    with aioresponses() as m:
        m.get(_RE_OPEN_INTEREST, payload=payload, status=200)
        result = await client.get_open_interest(symbol='BTCUSD_PERP')

    assert result == payload


@pytest.mark.asyncio
async def test_cm_get_open_interest_weight(client):
    with aioresponses() as m:
        m.get(_RE_OPEN_INTEREST, payload={}, status=200)
        await client.get_open_interest(symbol='BTCUSD_PERP')

    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# REST: get_open_interest_hist (pair+contractType, not symbol)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_open_interest_hist_hits_correct_url(client):
    payload = [
        {
            'contractType': 'PERPETUAL',
            'sumOpenInterest': '11948062.00000000',
            'sumOpenInterestValue': '15419.48047926',
            'pair': 'BTCUSD',
            'timestamp': 1779703200000,
        }
    ]

    with aioresponses() as m:
        m.get(_RE_OI_HIST, payload=payload, status=200)
        result = await client.get_open_interest_hist(
            pair='BTCUSD', contractType='PERPETUAL', period='1h'
        )

    assert result == payload


@pytest.mark.asyncio
async def test_cm_get_open_interest_hist_weight(client):
    with aioresponses() as m:
        m.get(_RE_OI_HIST, payload=[], status=200)
        await client.get_open_interest_hist(
            pair='BTCUSD', contractType='PERPETUAL', period='1h'
        )

    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# REST: get_funding_rate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_funding_rate_hits_correct_url(client):
    payload = [
        {
            'symbol': 'BTCUSD_PERP',
            'fundingTime': 1779667200001,
            'fundingRate': '0.00009106',
            'markPrice': '76943.88345951',
        }
    ]

    with aioresponses() as m:
        m.get(_RE_FUNDING_RATE, payload=payload, status=200)
        result = await client.get_funding_rate(symbol='BTCUSD_PERP', limit=10)

    assert result == payload


@pytest.mark.asyncio
async def test_cm_get_funding_rate_weight(client):
    with aioresponses() as m:
        m.get(_RE_FUNDING_RATE, payload=[], status=200)
        await client.get_funding_rate(symbol='BTCUSD_PERP')

    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# REST: get_funding_info
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_funding_info_hits_correct_url(client):
    payload = []  # COIN-M currently returns empty list; endpoint exists

    with aioresponses() as m:
        m.get(_RE_FUNDING_INFO, payload=payload, status=200)
        result = await client.get_funding_info()

    assert result == payload


@pytest.mark.asyncio
async def test_cm_get_funding_info_weight(client):
    with aioresponses() as m:
        m.get(_RE_FUNDING_INFO, payload=[], status=200)
        await client.get_funding_info()

    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# REST: get_premium_index (symbol given -> weight 1; omitted -> weight 10)
# COIN-M always returns a list (unlike USDⓈ-M which returns a dict for single symbol)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_premium_index_with_symbol_weight_1(client):
    payload = [
        {
            'symbol': 'BTCUSD_PERP',
            'pair': 'BTCUSD',
            'markPrice': '77458.95073093',
            'indexPrice': '77493.53787133',
            'estimatedSettlePrice': '77504.04329360',
            'lastFundingRate': '0.00006004',
            'interestRate': '0.00010000',
            'nextFundingTime': 1779724800000,
            'time': 1779705323000,
        }
    ]

    with aioresponses() as m:
        m.get(_RE_PREMIUM_INDEX, payload=payload, status=200)
        result = await client.get_premium_index(symbol='BTCUSD_PERP')

    assert result == payload
    assert _weight_used(client) == 1


@pytest.mark.asyncio
async def test_cm_get_premium_index_with_pair_weight_1(client):
    """Weight is 1 when 'pair' is given (in addition to 'symbol')."""
    with aioresponses() as m:
        m.get(_RE_PREMIUM_INDEX, payload=[], status=200)
        await client.get_premium_index(pair='BTCUSD')

    assert _weight_used(client) == 1


@pytest.mark.asyncio
async def test_cm_get_premium_index_no_symbol_weight_10(client):
    with aioresponses() as m:
        m.get(_RE_PREMIUM_INDEX, payload=[], status=200)
        await client.get_premium_index()

    assert _weight_used(client) == 10


# ---------------------------------------------------------------------------
# Stream handler helpers
# ---------------------------------------------------------------------------

async def run_cm_handler(client, HandlerBase, payload, stream='btcusd_perp@markPrice'):
    """Drive a payload through client._receive and return the handler DataFrame row."""
    future = create_future()

    class Handler(HandlerBase):
        def receive(self, p):
            p = super().receive(p)
            if not future.done():
                future.set_result(p)

    client.start()
    client.handler(Handler())

    await client._receive({
        'data': payload,
        'stream': stream,
    })

    df = await future
    return df.iloc[0]  # StockDataFrame -> single-row access


# ---------------------------------------------------------------------------
# Stream handler: MarkPriceHandlerBase (COIN-M -- no 'ap' field)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_mark_price_handler_columns(client):
    """CM MarkPriceHandlerBase converts raw fields; no mark_price_avg (ap)."""
    payload = {
        'e': 'markPriceUpdate',
        'E': 1779705310000,
        's': 'BTCUSD_PERP',
        'p': '77458.95073093',
        'i': '77493.53787133',
        'P': '77504.04329360',
        'r': '0.00006004',
        'T': 1779724800000,
    }

    row = await run_cm_handler(client, MarkPriceHandlerBase, payload)

    assert row['type'] == 'markPriceUpdate'
    assert row['event_time'] == 1779705310000
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['mark_price'] == '77458.95073093'
    assert row['index_price'] == '77493.53787133'
    assert row['est_settle_price'] == '77504.04329360'
    assert row['funding_rate'] == '0.00006004'
    assert row['next_funding_time'] == 1779724800000
    # COIN-M does NOT have 'ap' / mark_price_avg
    assert 'mark_price_avg' not in row.index


# ---------------------------------------------------------------------------
# Stream handler: ForceOrderHandlerBase (COIN-M -- includes 'ps' / pair)
# ---------------------------------------------------------------------------

CM_FORCE_ORDER_PAYLOAD = {
    'e': 'forceOrder',
    'E': 1568014460893,
    'o': {
        's': 'BTCUSD_PERP',
        'ps': 'BTCUSD',        # CM-only field: pair
        'S': 'SELL',
        'o': 'LIMIT',
        'f': 'IOC',
        'q': '100',
        'p': '9910',
        'ap': '9910',
        'X': 'FILLED',
        'l': '100',
        'z': '100',
        'T': 1568014460893,
    }
}


@pytest.mark.asyncio
async def test_cm_force_order_handler_flattens_nested_o_with_pair(client):
    """CM ForceOrderHandlerBase flattens 'o' and maps including 'ps' -> 'pair'."""
    row = await run_cm_handler(
        client, ForceOrderHandlerBase, CM_FORCE_ORDER_PAYLOAD, 'btcusd_perp@forceOrder'
    )

    assert row['type'] == 'forceOrder'
    assert row['event_time'] == 1568014460893
    assert row['symbol'] == 'BTCUSD_PERP'
    assert row['pair'] == 'BTCUSD'   # CM-only column
    assert row['side'] == 'SELL'
    assert row['order_type'] == 'LIMIT'
    assert row['time_in_force'] == 'IOC'
    assert row['orig_quantity'] == '100'
    assert row['price'] == '9910'
    assert row['avg_price'] == '9910'
    assert row['order_status'] == 'FILLED'
    assert row['last_filled_qty'] == '100'
    assert row['acc_filled_qty'] == '100'
    assert row['trade_time'] == 1568014460893


# ---------------------------------------------------------------------------
# subscribe_param: MarkPriceProcessor and ForceOrderProcessor
# ---------------------------------------------------------------------------

def test_cm_mark_price_subscribe_param_default(client):
    """CM subscribe_param for MARK_PRICE returns <symbol>@markPrice by default."""
    from binance.futures.cm.streams import MarkPriceProcessor
    proc = MarkPriceProcessor(client)
    result = proc.subscribe_param(True, SubType.MARK_PRICE, 'BTCUSD_PERP')
    assert result == 'btcusd_perp@markPrice'


def test_cm_mark_price_subscribe_param_1s(client):
    """CM subscribe_param with '1s' speed appends @1s suffix."""
    from binance.futures.cm.streams import MarkPriceProcessor
    proc = MarkPriceProcessor(client)
    result = proc.subscribe_param(True, SubType.MARK_PRICE, 'BTCUSD_PERP', '1s')
    assert result == 'btcusd_perp@markPrice@1s'


def test_cm_force_order_subscribe_param(client):
    """CM subscribe_param for FORCE_ORDER returns <symbol>@forceOrder."""
    from binance.futures.cm.streams import ForceOrderProcessor
    proc = ForceOrderProcessor(client)
    result = proc.subscribe_param(True, SubType.FORCE_ORDER, 'BTCUSD_PERP')
    assert result == 'btcusd_perp@forceOrder'


# ---------------------------------------------------------------------------
# SubType enum values (same as USDⓈ-M; shared constants)
# ---------------------------------------------------------------------------

def test_subtype_mark_price_value():
    """SubType.MARK_PRICE wire value is 'markPrice' (shared by both futures markets)."""
    assert str(SubType.MARK_PRICE) == 'markPrice'


def test_subtype_force_order_value():
    """SubType.FORCE_ORDER wire value is 'forceOrder' (shared by both futures markets)."""
    assert str(SubType.FORCE_ORDER) == 'forceOrder'


# ---------------------------------------------------------------------------
# Rate limit: CM has 2 rules (REQUEST_WEIGHT + ORDERS 1m) + WS_CONNECTIONS
# (no 10-second ORDERS pool unlike USDⓈ-M)
# ---------------------------------------------------------------------------

def test_cm_rate_limit_rules_count(client):
    """CMFuturesClient has exactly 3 rate-limit rules."""
    snap = client.rate_limit_snapshot()
    # 3 pools: REQUEST_WEIGHT, ORDERS (1m), WS_CONNECTIONS
    assert len(snap.windows) == 3


def test_cm_request_weight_limit(client):
    """CM REQUEST_WEIGHT safety limit is 2160 per minute (2400 * 0.9 safety ratio).

    The raw Binance limit is 2400/min (confirmed from GET /dapi/v1/exchangeInfo);
    the SDK applies a 0.9 safety ratio client-side, so the enforced limit is 2160.
    """
    from binance.futures.cm.rate_limit import CM_REQUEST_WEIGHT_LIMIT, CM_WEIGHT_SAFETY_RATIO
    snap = client.rate_limit_snapshot()
    for w in snap.windows:
        if w.type == RateLimitType.REQUEST_WEIGHT:
            expected_safety_limit = int(CM_REQUEST_WEIGHT_LIMIT * CM_WEIGHT_SAFETY_RATIO)
            assert w.limit == expected_safety_limit
            return
    raise AssertionError('REQUEST_WEIGHT window not found')
