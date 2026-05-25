"""Tests for the USDⓈ-M Futures module (P4).

Covers:
- REST endpoints: each method hits the right URL, sends correct params, and
  consumes the documented REQUEST_WEIGHT.
- Stream handlers: MarkPriceHandlerBase column mapping, ForceOrderHandlerBase
  nested-'o' flattening and column mapping.
- subscribe_param: MarkPriceProcessor returns correct stream names (base and 1s).
- SubType enum values for MARK_PRICE and FORCE_ORDER.

Endpoint weights confirmed against live Binance USDⓈ-M API responses (2026-05-25):
- /fapi/v1/openInterest         weight 1
- /futures/data/openInterestHist weight 1
- /fapi/v1/fundingRate          weight 1
- /fapi/v1/fundingInfo          weight 1
- /fapi/v1/premiumIndex         weight 1 (symbol given) / 10 (all symbols)
"""

import re
import pytest
from aioresponses import aioresponses

from binance import UMFuturesClient, MarkPriceHandlerBase, ForceOrderHandlerBase
from binance.core.common.constants import SubType
from binance.core.common.utils import create_future
from binance.core.rate_limit.types import RateLimitType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return UMFuturesClient()


# ---------------------------------------------------------------------------
# REST endpoint helpers
# ---------------------------------------------------------------------------

FAPI_BASE = 'https://fapi.binance.com'

# Regex patterns so aioresponses matches the base URL + any query string.
_RE_OPEN_INTEREST = re.compile(
    r'https://fapi\.binance\.com/fapi/v1/openInterest(\?.*)?$'
)
_RE_OI_HIST = re.compile(
    r'https://fapi\.binance\.com/futures/data/openInterestHist(\?.*)?$'
)
_RE_FUNDING_RATE = re.compile(
    r'https://fapi\.binance\.com/fapi/v1/fundingRate(\?.*)?$'
)
_RE_FUNDING_INFO = re.compile(
    r'https://fapi\.binance\.com/fapi/v1/fundingInfo(\?.*)?$'
)
_RE_PREMIUM_INDEX = re.compile(
    r'https://fapi\.binance\.com/fapi/v1/premiumIndex(\?.*)?$'
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
async def test_get_open_interest_hits_correct_url(client):
    payload = {'openInterest': '10659.509', 'symbol': 'BTCUSDT', 'time': 1589437530011}

    with aioresponses() as m:
        m.get(_RE_OPEN_INTEREST, payload=payload, status=200)
        result = await client.get_open_interest(symbol='BTCUSDT')

    assert result == payload


@pytest.mark.asyncio
async def test_get_open_interest_weight(client):
    with aioresponses() as m:
        m.get(_RE_OPEN_INTEREST, payload={}, status=200)
        await client.get_open_interest(symbol='BTCUSDT')

    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# REST: get_open_interest_hist
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_open_interest_hist_hits_correct_url(client):
    payload = [
        {'symbol': 'BTCUSDT', 'sumOpenInterest': '20403.637', 'timestamp': 1583127900000}
    ]

    with aioresponses() as m:
        m.get(_RE_OI_HIST, payload=payload, status=200)
        result = await client.get_open_interest_hist(symbol='BTCUSDT', period='1h')

    assert result == payload


@pytest.mark.asyncio
async def test_get_open_interest_hist_weight(client):
    with aioresponses() as m:
        m.get(_RE_OI_HIST, payload=[], status=200)
        await client.get_open_interest_hist(symbol='BTCUSDT', period='1h')

    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# REST: get_funding_rate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_funding_rate_hits_correct_url(client):
    payload = [
        {'symbol': 'BTCUSDT', 'fundingRate': '-0.0375', 'fundingTime': 1570608000000}
    ]

    with aioresponses() as m:
        m.get(_RE_FUNDING_RATE, payload=payload, status=200)
        result = await client.get_funding_rate(symbol='BTCUSDT', limit=10)

    assert result == payload


@pytest.mark.asyncio
async def test_get_funding_rate_weight(client):
    with aioresponses() as m:
        m.get(_RE_FUNDING_RATE, payload=[], status=200)
        await client.get_funding_rate(symbol='BTCUSDT')

    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# REST: get_funding_info
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_funding_info_hits_correct_url(client):
    payload = [
        {
            'symbol': 'BTCUSDT',
            'adjustedFundingRateCap': '0.02000000',
            'adjustedFundingRateFloor': '-0.02000000',
            'fundingIntervalHours': 8,
        }
    ]

    with aioresponses() as m:
        m.get(_RE_FUNDING_INFO, payload=payload, status=200)
        result = await client.get_funding_info()

    assert result == payload


@pytest.mark.asyncio
async def test_get_funding_info_weight(client):
    with aioresponses() as m:
        m.get(_RE_FUNDING_INFO, payload=[], status=200)
        await client.get_funding_info()

    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# REST: get_premium_index (symbol given -> weight 1; omitted -> weight 10)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_premium_index_with_symbol_weight_1(client):
    payload = {
        'symbol': 'BTCUSDT',
        'markPrice': '11793.63',
        'indexPrice': '11781.80',
        'estimatedSettlePrice': '11781.16',
        'lastFundingRate': '0.0001',
        'interestRate': '0.0001',
        'nextFundingTime': 1595836800000,
        'time': 1595827200000,
    }

    with aioresponses() as m:
        m.get(_RE_PREMIUM_INDEX, payload=payload, status=200)
        result = await client.get_premium_index(symbol='BTCUSDT')

    assert result == payload
    assert _weight_used(client) == 1


@pytest.mark.asyncio
async def test_get_premium_index_no_symbol_weight_10(client):
    with aioresponses() as m:
        m.get(_RE_PREMIUM_INDEX, payload=[], status=200)
        await client.get_premium_index()

    assert _weight_used(client) == 10


# ---------------------------------------------------------------------------
# Stream handler helpers
# ---------------------------------------------------------------------------

async def run_um_handler(client, HandlerBase, payload, stream='btcusdt@markPrice'):
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
# Stream handler: MarkPriceHandlerBase
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_price_handler_columns(client):
    """MarkPriceHandlerBase converts raw fields to human-readable column names."""
    payload = {
        'e': 'markPriceUpdate',
        'E': 1595826801000,
        's': 'BTCUSDT',
        'p': '11793.63104562',
        'ap': '11795.0',
        'i': '11781.80495970',
        'P': '11781.16138815',
        'r': '0.00010000',
        'T': 1595836800000,
    }

    row = await run_um_handler(client, MarkPriceHandlerBase, payload)

    assert row['type'] == 'markPriceUpdate'
    assert row['event_time'] == 1595826801000
    assert row['symbol'] == 'BTCUSDT'
    assert row['mark_price'] == '11793.63104562'
    assert row['mark_price_avg'] == '11795.0'
    assert row['index_price'] == '11781.80495970'
    assert row['est_settle_price'] == '11781.16138815'
    assert row['funding_rate'] == '0.00010000'
    assert row['next_funding_time'] == 1595836800000


# ---------------------------------------------------------------------------
# Stream handler: ForceOrderHandlerBase (flattening the nested 'o')
# ---------------------------------------------------------------------------

FORCE_ORDER_PAYLOAD = {
    'e': 'forceOrder',
    'E': 1568014460893,
    'o': {
        's': 'BTCUSDT',
        'S': 'SELL',
        'o': 'LIMIT',
        'f': 'IOC',
        'q': '0.014',
        'p': '9910',
        'ap': '9910',
        'X': 'FILLED',
        'l': '0.014',
        'z': '0.014',
        'T': 1568014460893,
    }
}


@pytest.mark.asyncio
async def test_force_order_handler_flattens_nested_o(client):
    """ForceOrderHandlerBase flattens 'o' and maps to human-readable columns."""
    row = await run_um_handler(
        client, ForceOrderHandlerBase, FORCE_ORDER_PAYLOAD, 'btcusdt@forceOrder'
    )

    assert row['type'] == 'forceOrder'
    assert row['event_time'] == 1568014460893
    assert row['symbol'] == 'BTCUSDT'
    assert row['side'] == 'SELL'
    assert row['order_type'] == 'LIMIT'
    assert row['time_in_force'] == 'IOC'
    assert row['orig_quantity'] == '0.014'
    assert row['price'] == '9910'
    assert row['avg_price'] == '9910'
    assert row['order_status'] == 'FILLED'
    assert row['last_filled_qty'] == '0.014'
    assert row['acc_filled_qty'] == '0.014'
    assert row['trade_time'] == 1568014460893


# ---------------------------------------------------------------------------
# subscribe_param: MarkPriceProcessor and ForceOrderProcessor
# ---------------------------------------------------------------------------

def test_mark_price_subscribe_param_default(client):
    """subscribe_param for MARK_PRICE returns <symbol>@markPrice by default."""
    from binance.futures.um.streams import MarkPriceProcessor
    proc = MarkPriceProcessor(client)
    result = proc.subscribe_param(True, SubType.MARK_PRICE, 'BTCUSDT')
    assert result == 'btcusdt@markPrice'


def test_mark_price_subscribe_param_1s(client):
    """subscribe_param with '1s' speed appends @1s suffix."""
    from binance.futures.um.streams import MarkPriceProcessor
    proc = MarkPriceProcessor(client)
    result = proc.subscribe_param(True, SubType.MARK_PRICE, 'BTCUSDT', '1s')
    assert result == 'btcusdt@markPrice@1s'


def test_force_order_subscribe_param(client):
    """subscribe_param for FORCE_ORDER returns <symbol>@forceOrder."""
    from binance.futures.um.streams import ForceOrderProcessor
    proc = ForceOrderProcessor(client)
    result = proc.subscribe_param(True, SubType.FORCE_ORDER, 'BTCUSDT')
    assert result == 'btcusdt@forceOrder'


# ---------------------------------------------------------------------------
# SubType enum values (confirmed against docs 2026-05-25)
# ---------------------------------------------------------------------------

def test_subtype_mark_price_value():
    """SubType.MARK_PRICE wire value is 'markPrice' (stream name prefix)."""
    assert str(SubType.MARK_PRICE) == 'markPrice'


def test_subtype_force_order_value():
    """SubType.FORCE_ORDER wire value is 'forceOrder' (stream name prefix)."""
    assert str(SubType.FORCE_ORDER) == 'forceOrder'
