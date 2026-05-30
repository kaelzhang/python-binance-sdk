"""Hermetic tests for newly-added COIN-M Futures REST endpoints (Round-8 K4 C4).

Only one CM-documented endpoint missing on the SDK lacked a WS-API
equivalent: ``constituents``. Other CM-side candidates listed in the
Round-8 spec (tradingSchedule, insuranceBalance, apiTradingStatus,
accountConfig, symbolConfig, feeBurn, async download endpoints,
historicalTrades, etc.) are NOT documented under
``developers.binance.com/docs/derivatives/coin-margined-futures/*/rest-api``;
they are UM-only surfaces.

Docs:
- https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Index-Constituents
"""

import re
import pytest
from aioresponses import aioresponses

from binance import CMFuturesClient
from binance.core.common.constants import SecurityType
from binance.core.rate_limit.types import RateLimitType
from binance.futures.cm.endpoints import REST_ENDPOINTS


DAPI = 'https://dapi.binance.com'


def _public_client():
    client = CMFuturesClient()
    client._time_synced = True
    return client


def _weight_used(client) -> int:
    snap = client.rate_limit_snapshot()
    for w in snap.windows:
        if w.type == RateLimitType.REQUEST_WEIGHT:
            return w.used
    return 0


def _re(path: str) -> re.Pattern:
    escaped = re.escape(DAPI + path)
    return re.compile(rf'{escaped}(\?.*)?$')


# ---------------------------------------------------------------------------
# constituents — NONE, weight 1, symbol required
# Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Index-Constituents
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cm_get_constituents_get_correct_url_and_weight():
    client = _public_client()
    with aioresponses() as m:
        m.get(_re('/dapi/v1/constituents'),
              payload={'symbol': 'BTCUSD_PERP', 'time': 1, 'constituents': []},
              status=200)
        result = await client.get_constituents(symbol='BTCUSD_PERP')
    assert 'symbol' in result
    assert _weight_used(client) == 1


def test_cm_constituents_registry_shape():
    by_name = {e['name']: e for e in REST_ENDPOINTS}
    con = by_name['get_constituents']
    assert con['rest_url'].endswith('/dapi/v1/constituents')
    assert con['security_type'] == SecurityType.NONE
    assert con['weight'] == 1
