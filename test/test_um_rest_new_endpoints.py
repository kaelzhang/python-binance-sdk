"""Hermetic tests for newly-added USDⓈ-M Futures REST endpoints (Round-8 K4 C3).

Endpoints (all verified via developers.binance.com):
- ``accountConfig`` / ``symbolConfig`` — USER_DATA, weight 5
- ``apiTradingStatus`` — USER_DATA, weight 1 (with ``symbol``) / 10 (without)
- ``feeBurn`` GET — USER_DATA, weight 30
- ``feeBurn`` POST — TRADE, weight 1
- ``tradingSchedule`` — NONE, weight 5
- ``symbolAdlRisk`` — NONE, weight 1
- ``insuranceBalance`` — NONE, weight 1
- ``constituents`` — NONE, weight 2
- ``rpiDepth`` REST — NONE, weight 20 (fixed; limit hardcoded to 1000 per docs)
- ``assetIndex`` REST — NONE, weight 1 (with ``symbol``) / 10 (without)
"""

import re
import pytest
from aioresponses import aioresponses

from binance import UMFuturesClient, Credentials
from binance.core.common.constants import SecurityType, RequestMethod
from binance.core.rate_limit.types import RateLimitType
from binance.futures.um.endpoints import REST_ENDPOINTS


FAPI = 'https://fapi.binance.com'


def _signed_client():
    client = UMFuturesClient(Credentials(api_key='K', api_secret='S'))
    client._time_synced = True
    return client


def _public_client():
    client = UMFuturesClient()
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    for w in snap.windows:
        if w.type == RateLimitType.REQUEST_WEIGHT:
            return w.used
    return 0


def _re(path: str) -> re.Pattern:
    escaped = re.escape(FAPI + path)
    return re.compile(rf'{escaped}(\?.*)?$')


# ---------------------------------------------------------------------------
# accountConfig — USER_DATA, weight 5
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Config
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_account_config_get_correct_url_and_weight():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/accountConfig'), payload={'feeTier': 0}, status=200)
        result = await client.get_account_config()
    assert result == {'feeTier': 0}
    assert _weight_used(client) == 5


# ---------------------------------------------------------------------------
# symbolConfig — USER_DATA, weight 5
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Symbol-Config
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_symbol_config_get_correct_url_and_weight():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/symbolConfig'), payload=[{'symbol': 'BTCUSDT'}], status=200)
        result = await client.get_symbol_config(symbol='BTCUSDT')
    assert result == [{'symbol': 'BTCUSDT'}]
    assert _weight_used(client) == 5


# ---------------------------------------------------------------------------
# apiTradingStatus — USER_DATA, weight 1 with symbol / 10 without
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Futures-Trading-Quantitative-Rules-Indicators
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_api_trading_status_with_symbol_weight_1():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/apiTradingStatus'), payload={'isLocked': False}, status=200)
        await client.get_api_trading_status(symbol='BTCUSDT')
    assert _weight_used(client) == 1


@pytest.mark.asyncio
async def test_get_api_trading_status_no_symbol_weight_10():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/apiTradingStatus'), payload={}, status=200)
        await client.get_api_trading_status()
    assert _weight_used(client) == 10


# ---------------------------------------------------------------------------
# feeBurn GET — USER_DATA, weight 30
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Get-BNB-Burn-Status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_fee_burn_status_get_correct_url_and_weight():
    client = _signed_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/feeBurn'), payload={'feeBurn': True}, status=200)
        result = await client.get_fee_burn_status()
    assert result == {'feeBurn': True}
    assert _weight_used(client) == 30


# ---------------------------------------------------------------------------
# feeBurn POST — TRADE, weight 1
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Toggle-BNB-Burn-On-Futures-Trade
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_fee_burn_post_correct_url_and_weight():
    client = _signed_client()
    with aioresponses() as m:
        m.post(_re('/fapi/v1/feeBurn'), payload={'code': 200, 'msg': 'success'}, status=200)
        result = await client.set_fee_burn(feeBurn='true')
    assert result['code'] == 200
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# tradingSchedule — NONE, weight 5
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Trading-Schedule
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_trading_schedule_get_correct_url_and_weight():
    client = _public_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/tradingSchedule'), payload=[], status=200)
        result = await client.get_trading_schedule()
    assert result == []
    assert _weight_used(client) == 5


# ---------------------------------------------------------------------------
# symbolAdlRisk — NONE, weight 1
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/ADL-Risk
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_symbol_adl_risk_get_correct_url_and_weight():
    client = _public_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/symbolAdlRisk'), payload=[{'symbol': 'BTCUSDT', 'adlRisk': 'low'}], status=200)
        result = await client.get_symbol_adl_risk()
    assert result == [{'symbol': 'BTCUSDT', 'adlRisk': 'low'}]
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# insuranceBalance — NONE, weight 1
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Insurance-Fund
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_insurance_balance_get_correct_url_and_weight():
    client = _public_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/insuranceBalance'), payload=[], status=200)
        result = await client.get_insurance_balance()
    assert result == []
    assert _weight_used(client) == 1


# ---------------------------------------------------------------------------
# constituents — NONE, weight 2
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Index-Constituents
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_constituents_get_correct_url_and_weight():
    client = _public_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/constituents'), payload={'symbol': 'BTCUSDT', 'constituents': []}, status=200)
        result = await client.get_constituents(symbol='BTCUSDT')
    assert 'symbol' in result
    assert _weight_used(client) == 2


# ---------------------------------------------------------------------------
# rpiDepth REST — NONE, weight 20 (fixed; only valid limit is 1000)
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book-RPI
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_rpi_depth_get_correct_url_and_weight():
    client = _public_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/rpiDepth'), payload={'lastUpdateId': 1, 'bids': [], 'asks': []}, status=200)
        result = await client.get_rpi_depth(symbol='BTCUSDT')
    assert 'lastUpdateId' in result
    assert _weight_used(client) == 20


# ---------------------------------------------------------------------------
# assetIndex REST — NONE, weight 1 with symbol / 10 without
# Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Multi-Assets-Mode-Asset-Index
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_asset_index_with_symbol_weight_1():
    client = _public_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/assetIndex'), payload={'symbol': 'BTCUSDT'}, status=200)
        await client.get_asset_index(symbol='BTCUSDT')
    assert _weight_used(client) == 1


@pytest.mark.asyncio
async def test_get_asset_index_no_symbol_weight_10():
    client = _public_client()
    with aioresponses() as m:
        m.get(_re('/fapi/v1/assetIndex'), payload=[], status=200)
        await client.get_asset_index()
    assert _weight_used(client) == 10


# ---------------------------------------------------------------------------
# Registry shape — all new entries are wired in.
# ---------------------------------------------------------------------------

def test_um_new_endpoints_registry_shape():
    by_name = {e['name']: e for e in REST_ENDPOINTS}

    # account/config + symbol/config — USER_DATA, 5
    cfg = by_name['get_account_config']
    assert cfg['rest_url'].endswith('/fapi/v1/accountConfig')
    assert cfg['security_type'] == SecurityType.USER_DATA
    assert cfg['weight'] == 5

    sym = by_name['get_symbol_config']
    assert sym['rest_url'].endswith('/fapi/v1/symbolConfig')
    assert sym['security_type'] == SecurityType.USER_DATA
    assert sym['weight'] == 5

    # apiTradingStatus — USER_DATA, dynamic 1/10
    ats = by_name['get_api_trading_status']
    assert ats['rest_url'].endswith('/fapi/v1/apiTradingStatus')
    assert ats['security_type'] == SecurityType.USER_DATA
    assert callable(ats['weight'])

    # feeBurn GET — USER_DATA, 30
    fb_get = by_name['get_fee_burn_status']
    assert fb_get['rest_url'].endswith('/fapi/v1/feeBurn')
    assert fb_get['security_type'] == SecurityType.USER_DATA
    assert fb_get['weight'] == 30
    assert fb_get.get('method', RequestMethod.GET) == RequestMethod.GET

    # feeBurn POST — TRADE, 1
    fb_post = by_name['set_fee_burn']
    assert fb_post['rest_url'].endswith('/fapi/v1/feeBurn')
    assert fb_post['security_type'] == SecurityType.TRADE
    assert fb_post['weight'] == 1
    assert fb_post['method'] == RequestMethod.POST

    # tradingSchedule — NONE, 5
    ts = by_name['get_trading_schedule']
    assert ts['rest_url'].endswith('/fapi/v1/tradingSchedule')
    assert ts['security_type'] == SecurityType.NONE
    assert ts['weight'] == 5

    # symbolAdlRisk — NONE, 1
    adl = by_name['get_symbol_adl_risk']
    assert adl['rest_url'].endswith('/fapi/v1/symbolAdlRisk')
    assert adl['security_type'] == SecurityType.NONE
    assert adl['weight'] == 1

    # insuranceBalance — NONE, 1
    ins = by_name['get_insurance_balance']
    assert ins['rest_url'].endswith('/fapi/v1/insuranceBalance')
    assert ins['security_type'] == SecurityType.NONE
    assert ins['weight'] == 1

    # constituents — NONE, 2
    con = by_name['get_constituents']
    assert con['rest_url'].endswith('/fapi/v1/constituents')
    assert con['security_type'] == SecurityType.NONE
    assert con['weight'] == 2

    # rpiDepth — NONE, 20 (fixed)
    rpi = by_name['get_rpi_depth']
    assert rpi['rest_url'].endswith('/fapi/v1/rpiDepth')
    assert rpi['security_type'] == SecurityType.NONE
    assert rpi['weight'] == 20

    # assetIndex — NONE, dynamic 1/10
    ai = by_name['get_asset_index']
    assert ai['rest_url'].endswith('/fapi/v1/assetIndex')
    assert ai['security_type'] == SecurityType.NONE
    assert callable(ai['weight'])
