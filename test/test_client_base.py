import hashlib
import hmac
import re
import pytest
from aioresponses import aioresponses

from binance import (
    Client,
    StatusException
)
from binance.common.constants import SecurityType

# TODO:
# global request_params
# keyword argument request_params

URL = 'https://api.binance.com/api/v3/foo'
URL2 = URL + '/bar'


def redirect(m):
    m.get(
        URL,
        status=307,
        headers={
            'Location': URL2
        }
    )


def test_generate_signature_url_encodes_params():
    client = Client('api_key', 'api_secret')

    params = {
        'symbol': 'BTCUSDT',
        'newClientOrderId': 'id=1&tag=foo bar'
    }

    payload = (
        'newClientOrderId=id%3D1%26tag%3Dfoo%20bar'
        '&symbol=BTCUSDT'
    )

    expected = hmac.new(
        b'api_secret',
        payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    assert client._generate_signature(params) == expected


@pytest.mark.asyncio
async def test_global_request_params():
    payload = {
        'foo': 'bar'
    }

    client = Client(
        request_params={
            'allow_redirects': True
        }
    )

    with aioresponses() as m:
        redirect(m)

        m.get(
            URL2,
            payload=payload,
            status=200
        )

        res = await client.get(URL)

        assert res == payload


@pytest.mark.asyncio
async def test_request_params():
    client = Client()

    with aioresponses() as m:
        redirect(m)

        with pytest.raises(
            StatusException,
            match='status 307'
        ):
            await client.get(
                URL,
                request_params={
                    'allow_redirects': False
                }
            )


@pytest.mark.asyncio
async def test_force_params():
    payload = {
        'foo': 'bar'
    }

    client = Client()

    with aioresponses() as m:
        m.post(
            URL + '?foo=bar',
            payload=payload,
            status=200
        )

        res = await client.post(
            URL,
            foo='bar',
            force_params=True
        )

        assert res == payload


# ---------------------------------------------------------------------------
# The generic REST request escape hatch (`client.get`/`post`/...) is retained
# while the data/account/trading endpoints travel over the WebSocket API.
# These exercise the remaining REST `_request` / `_handle_response` plumbing
# (signed signing path, lazy time-sync, exchangeInfo cap config, -1021 re-arm)
# directly against a raw URL, since no public method routes to REST anymore.
# ---------------------------------------------------------------------------

_TIME_URL = 'https://api.binance.com/api/v3/time'
_ACCOUNT_URL = 'https://api.binance.com/api/v3/account'
_ACCOUNT_URL_RE = re.compile(r'https://api\.binance\.com/api/v3/account(\?.*)?$')
_EXCHANGE_INFO_URL = 'https://api.binance.com/api/v3/exchangeInfo'


@pytest.mark.asyncio
async def test_signed_rest_escape_hatch_signs_and_lazy_syncs():
    """A signed REST GET signs the query (apiKey header + signature) and lazily
    syncs the server-time offset before the first signed request."""
    client = Client(api_key='k', api_secret='s')
    assert client._time_synced is False

    with aioresponses() as m:
        # The lazy time-sync hits GET /api/v3/time.
        m.get(_TIME_URL, payload={'serverTime': 1_700_000_000_000})
        m.get(_ACCOUNT_URL_RE, payload={'canTrade': True})

        result = await client.get(
            _ACCOUNT_URL,
            security_type=SecurityType.USER_DATA,
            recvWindow=5000,
        )

    assert result == {'canTrade': True}
    # The lazy sync ran on the REST path.
    assert client._time_synced is True


@pytest.mark.asyncio
async def test_rest_escape_hatch_exchange_info_configures_pool_caps():
    """A REST response carrying a `rateLimits` array reconfigures pool caps."""
    client = Client()
    with aioresponses() as m:
        m.get(_EXCHANGE_INFO_URL, status=200, payload={
            'rateLimits': [
                {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE',
                 'intervalNum': 1, 'limit': 12000},
            ],
            'symbols': []
        })
        await client.get(_EXCHANGE_INFO_URL)

    snap = client.rate_limit_snapshot()
    weight = [w for w in snap.windows if w.type == 'request_weight'][0]
    # configured cap 12000 * 0.9 safety ratio = 10800 effective
    assert weight.limit == 10800


@pytest.mark.asyncio
async def test_rest_escape_hatch_1021_rearms_time_sync():
    """A -1021 REST response re-arms the lazy time-sync (sets _time_synced=False)."""
    client = Client(api_key='k', api_secret='s')
    client._time_synced = True   # pretend we already synced

    with aioresponses() as m:
        m.get(_ACCOUNT_URL_RE, status=400, payload={
            'code': -1021,
            'msg': 'Timestamp for this request is outside of the recvWindow.'
        })
        with pytest.raises(StatusException) as exc_info:
            await client.get(
                'https://api.binance.com/api/v3/account',
                security_type=SecurityType.USER_DATA,
            )

    assert exc_info.value.code == -1021
    assert client._time_synced is False
