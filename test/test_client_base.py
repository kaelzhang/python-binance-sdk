import hashlib
import hmac
import re
import pytest
from aioresponses import aioresponses

from binance import (
    Client,
    StatusException
)
from binance.core.common.constants import SecurityType
from binance.client.base import _reject_float_params, encode_params, sort_params
from binance.core.rate_limit import RateLimiter
from binance.core.rate_limit.types import RateLimitType

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
    weight = [w for w in snap.windows if w.type == RateLimitType.REQUEST_WEIGHT][0]
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


# ---------------------------------------------------------------------------
# F-07 — float param rejection on REST path
# ---------------------------------------------------------------------------

def test_reject_float_params_rejects_nested_floats():
    from binance.client.base import _reject_float_params

    # top-level float
    with pytest.raises(ValueError, match='float'):
        _reject_float_params({'price': 1.0})

    # float nested inside a list value
    with pytest.raises(ValueError, match='float'):
        _reject_float_params({'symbols': ['BTCUSDT', 2.0]})

    # float nested inside a dict value
    with pytest.raises(ValueError, match='float'):
        _reject_float_params({'outer': {'inner': 3.0}})

    # clean nested structures (str/int/bool/nested containers) must NOT raise
    _reject_float_params({'a': '1', 'b': 2, 'c': True, 'd': ['x', {'e': 'y'}]})


def test_reject_float_params_raises_with_message():
    """_reject_float_params raises ValueError naming the offending key."""
    with pytest.raises(ValueError, match="price.*float.*pass a string"):
        _reject_float_params({'symbol': 'BTCUSDT', 'price': 0.01})


def test_reject_float_params_allows_int_str_bool():
    """int, str, and bool values are all accepted."""
    _reject_float_params({'qty': 1, 'price': '0.01', 'test': True})


@pytest.mark.asyncio
async def test_rest_float_param_rejected():
    """A float param in a GET REST call raises ValueError before any network call."""
    client = Client()
    with pytest.raises(ValueError, match="float"):
        await client.get(URL, price=0.01)


@pytest.mark.asyncio
async def test_rest_post_float_param_rejected():
    """A float param in a POST REST call raises ValueError before any network call."""
    client = Client()
    with pytest.raises(ValueError, match="float"):
        await client.post(URL, qty=0.1)


# ---------------------------------------------------------------------------
# F-35 — contract test: signed string == wire string (GET and POST)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_f35_get_signed_equals_wire():
    """F-47: for a signed GET the query string on the wire is byte-identical to
    what was signed.  The param value contains '/' and a space to exercise
    percent-encoding paths that previously diverged between signing and sending."""
    client = Client(api_key='k', api_secret='secret')
    # Pre-arm time-sync so the request doesn't try to open a WS connection.
    client._time_synced = True
    client._time_offset = 0

    captured_url = []

    with aioresponses() as m:
        # Match any URL for this host.
        pattern = re.compile(r'https://api\.binance\.com/.*')
        m.get(pattern, payload={'ok': True})

        # Intercept _build_rest_request to capture the URL object it produces.
        original_build = client._build_rest_request

        def capturing_build(method, uri, need_signed, api_key, **data):
            result_uri, req_kwargs = original_build(
                method, uri, need_signed, api_key, **data)
            if 'url' in req_kwargs:
                captured_url.append(str(req_kwargs['url']))
            return result_uri, req_kwargs

        client._build_rest_request = capturing_build
        await client.get(
            'https://api.binance.com/api/v3/account',
            security_type=SecurityType.USER_DATA,
            newClientOrderId='a/b c',
            recvWindow=5000,
        )

    # Reconstruct the expected signed payload from the captured query.
    assert captured_url, "URL was not captured"
    query_part = captured_url[0].split('?', 1)[1]

    # Parse the params back out and verify the signature.
    pairs = {}
    for part in query_part.split('&'):
        k, v = part.split('=', 1)
        pairs[k] = v

    # The signing input must be everything except the signature itself,
    # assembled as 'key=value&...' in the SAME percent-encoded form.
    sig = pairs.pop('signature')
    # Rebuild the signing input from the wire pairs (already encoded).
    # Signature is at the end; the input is everything before &signature=.
    signing_input = '&'.join(f'{k}={v}' for k, v in sorted(pairs.items()))
    # The wire string is percent-encoded; signing was over the same encoded string.
    expected_sig = hmac.new(
        b'secret', signing_input.encode(), hashlib.sha256
    ).hexdigest()
    assert sig == expected_sig, (
        f"signed string != wire string.\n"
        f"  signing_input: {signing_input!r}\n"
        f"  expected sig:  {expected_sig!r}\n"
        f"  got sig:       {sig!r}"
    )


@pytest.mark.asyncio
async def test_f35_post_signed_body_equals_signed():
    """F-47: for a signed POST the body on the wire is byte-identical to what
    was signed.  The param value contains '/' and a space."""
    client = Client(api_key='k', api_secret='secret')
    client._time_synced = True
    client._time_offset = 0

    captured_body = []

    with aioresponses() as m:
        pattern = re.compile(r'https://api\.binance\.com/.*')
        m.post(pattern, payload={'ok': True})

        original_build = client._build_rest_request

        def capturing_build(method, uri, need_signed, api_key, **data):
            result_uri, req_kwargs = original_build(
                method, uri, need_signed, api_key, **data)
            if 'data' in req_kwargs:
                captured_body.append(req_kwargs['data'])
            return result_uri, req_kwargs

        client._build_rest_request = capturing_build
        await client.post(
            'https://api.binance.com/api/v3/order',
            security_type=SecurityType.TRADE,
            newClientOrderId='a/b c',
            side='BUY',
        )

    assert captured_body, "body was not captured"
    body = captured_body[0]

    # Parse pairs from the body.
    pairs = {}
    for part in body.split('&'):
        k, v = part.split('=', 1)
        pairs[k] = v

    sig = pairs.pop('signature')
    # Signing input is exactly the body minus the trailing &signature=...
    # = everything before the last &signature= segment
    signing_input = body[:body.rfind('&signature=')]
    expected_sig = hmac.new(
        b'secret', signing_input.encode(), hashlib.sha256
    ).hexdigest()
    assert sig == expected_sig, (
        f"signed body != wire body.\n"
        f"  signing_input: {signing_input!r}\n"
        f"  expected sig:  {expected_sig!r}\n"
        f"  got sig:       {sig!r}"
    )


# ---------------------------------------------------------------------------
# F-12 / F-42 — shared ClientSession + configurable timeout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_session_reused_across_requests():
    """The same ClientSession instance is reused for multiple REST calls."""
    client = Client(request_timeout=5)
    session1_id = None
    session2_id = None

    with aioresponses() as m:
        m.get(URL, payload={'ok': 1})
        m.get(URL, payload={'ok': 2})
        await client.get(URL)
        session1_id = id(client._rest_session)
        await client.get(URL)
        session2_id = id(client._rest_session)

    assert session1_id == session2_id, "Session was not reused"
    await client.close()


@pytest.mark.asyncio
async def test_close_closes_rest_session():
    """client.close() closes the shared REST ClientSession."""
    client = Client()
    with aioresponses() as m:
        m.get(URL, payload={'ok': True})
        await client.get(URL)
    assert client._rest_session is not None
    assert not client._rest_session.closed
    await client.close()
    assert client._rest_session is None


@pytest.mark.asyncio
async def test_close_when_session_never_opened():
    """client.close() is safe even if no REST request was ever made."""
    client = Client()
    assert client._rest_session is None
    await client.close()  # must not raise


def test_request_timeout_stored():
    """Client(request_timeout=...) stores the value for session creation."""
    client = Client(request_timeout=42)
    assert client._request_timeout == 42.0


# ---------------------------------------------------------------------------
# F-49 — shared RateLimiter injection
# ---------------------------------------------------------------------------

def test_shared_rate_limiter_injection():
    """Client(rate_limiter=...) uses the injected limiter rather than building one."""
    shared = RateLimiter(enabled=False)
    client = Client(rate_limiter=shared)
    assert client._rate_limiter is shared


def test_default_rate_limiter_created_without_injection():
    """Without rate_limiter=, a fresh RateLimiter is created."""
    client = Client(rate_limit_guard=True)
    assert isinstance(client._rate_limiter, RateLimiter)


@pytest.mark.asyncio
async def test_shared_rate_limiter_shared_between_clients():
    """Two clients sharing a RateLimiter see each other's REST weight usage."""
    shared = RateLimiter(enabled=False)
    client_a = Client(rate_limiter=shared)
    client_b = Client(rate_limiter=shared)
    assert client_a._rate_limiter is client_b._rate_limiter

    with aioresponses() as m:
        m.get(URL, payload={'ok': True},
              headers={'X-MBX-Used-Weight-1m': '5'})
        await client_a.get(URL)

    # client_b's limiter was updated by client_a's response headers.
    client_a._rate_limiter.sync_from_headers(
        client_a._used_weight, client_a._order_count)
    snap = shared.snapshot()
    weight_windows = [w for w in snap.windows if w.type == RateLimitType.REQUEST_WEIGHT]
    assert weight_windows[0].used == 5


# ---------------------------------------------------------------------------
# POST body (no force_params) — exercises _build_rest_request body path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_post_body_sends_encoded_form():
    """A POST with no force_params sends params as a url-encoded body."""
    client = Client()
    with aioresponses() as m:
        m.post(URL, payload={'ok': True})
        result = await client.post(URL, symbol='BTCUSDT', side='BUY')
    assert result == {'ok': True}


@pytest.mark.asyncio
async def test_put_and_delete_methods():
    """PUT and DELETE methods are dispatched correctly."""
    client = Client()
    with aioresponses() as m:
        m.put(URL, payload={'updated': True})
        res = await client.put(URL)
    assert res == {'updated': True}

    with aioresponses() as m:
        m.delete(URL, payload={'deleted': True})
        res = await client.delete(URL)
    assert res == {'deleted': True}


# ---------------------------------------------------------------------------
# F-75 — bool params must serialize as JSON `true`/`false` (not Python True/False)
# ---------------------------------------------------------------------------

def test_encode_params_bool_true_is_lowercase():
    """encode_params must produce `true` (not `True`) for Python True values."""
    result = encode_params({'computeCommissionRates': True})
    assert 'computeCommissionRates=true' in result
    assert 'True' not in result


def test_encode_params_bool_false_is_lowercase():
    """encode_params must produce `false` (not `False`) for Python False values."""
    result = encode_params({'computeCommissionRates': False})
    assert 'computeCommissionRates=false' in result
    assert 'False' not in result


def test_sort_params_bool_serialization():
    """sort_params must emit lowercase true/false for bool values."""
    pairs = dict(sort_params({'flag': True, 'off': False, 'num': 1}))
    assert pairs['flag'] == 'true'
    assert pairs['off'] == 'false'
    assert pairs['num'] == '1'


def test_ws_api_query_bool_is_lowercase():
    """_ws_api_query must emit lowercase true/false for bool params (F-75).

    Correct bool serialization is critical for WS-API signed requests:
    the signature is over `_ws_api_query` output; the wire JSON serializes
    bool as JSON `true`/`false`. Both must agree.
    """
    client = Client(api_key='k', api_secret='s')
    query = client._ws_api_query({'computeCommissionRates': True,
                                   'symbol': 'BTCUSDT'})
    assert 'computeCommissionRates=true' in query
    assert 'True' not in query

    query_false = client._ws_api_query({'computeCommissionRates': False})
    assert 'computeCommissionRates=false' in query_false
    assert 'False' not in query_false


def test_int_and_str_params_unaffected_by_bool_fix():
    """Non-bool types (int, str) are still stringified via str()."""
    result = encode_params({'limit': 100, 'symbol': 'BTCUSDT'})
    assert 'limit=100' in result
    assert 'symbol=BTCUSDT' in result
