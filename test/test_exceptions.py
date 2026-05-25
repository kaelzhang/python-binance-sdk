import pytest
from aioresponses import aioresponses

from binance import (
    Client,
    SubType,
    SecurityType,
    UserStreamNotSubscribedException,
    InvalidResponseException,
    StatusException,
    APIKeyNotDefinedException,
    APISecretNotDefinedException
)


@pytest.mark.asyncio
async def test_no_secret():
    client = Client('api_key')

    for method in ['get', 'post', 'delete', 'put']:
        with pytest.raises(APISecretNotDefinedException, match='api_secret'):
            await getattr(client, method)('/foo', security_type=SecurityType.USER_DATA)


@pytest.mark.asyncio
async def test_no_key():
    client = Client()

    with pytest.raises(APIKeyNotDefinedException, match='api_key'):
        await client.get('/foo', security_type=SecurityType.USER_DATA)


# The data endpoints now travel over the WebSocket API, but the generic REST
# request plumbing (`client.get`/`_request`/`_handle_response`) still backs the
# escape hatch -- exercise its error paths directly against a raw URL.
_TIME_URL = 'https://api.binance.com/api/v3/time'


@pytest.mark.asyncio
async def test_invalid_json():
    """Test Invalid response Exception"""

    with pytest.raises(InvalidResponseException, match='invalid response'):
        with aioresponses() as m:
            m.get(_TIME_URL, body='<head></html>')

            client = Client('api_key')
            await client.get(_TIME_URL)


@pytest.mark.asyncio
async def test_api_exception():
    """Test Status Exception"""
    with pytest.raises(StatusException, match='status'):
        with aioresponses() as m:
            json_obj = {"code": 1002, "msg": "Invalid API call"}
            m.get(_TIME_URL, payload=json_obj, status=400)

            client = Client('api_key')
            await client.get(_TIME_URL)


@pytest.mark.asyncio
async def test_api_exception_5xx_server_error():
    """A 5xx response yields a StatusException with the generic server-error message."""
    with pytest.raises(StatusException, match='Binance server error'):
        with aioresponses() as m:
            m.get(_TIME_URL, body='<html>502 Bad Gateway</html>', status=502)

            client = Client('api_key')
            await client.get(_TIME_URL)


@pytest.mark.asyncio
async def test_api_exception_invalid_json():
    """
    Test Status Exception, StatusException comes before InvalidResponseException
    """

    with pytest.raises(StatusException):
        with aioresponses() as m:
            not_json_str = "<html><body>Error</body></html>"
            m.get(_TIME_URL, body=not_json_str, status=400)

            client = Client('api_key')
            await client.get(_TIME_URL)


@pytest.mark.asyncio
async def test_user_steam_not_subscribed():
    with pytest.raises(UserStreamNotSubscribedException):
        client = Client()
        await client.unsubscribe(SubType.USER)


@pytest.mark.asyncio
async def test_user_stream_no_secret():
    client = Client('api_key')

    with pytest.raises(APISecretNotDefinedException, match='api_secret'):
        await client.subscribe(SubType.USER)


def test_rate_limit_exception_carries_retry_after():
    from binance.common.exceptions import RateLimitException, IPBannedException

    class _Resp:
        url = 'https://api.binance.com/api/v3/order'
        status = 429
    exc = RateLimitException(_Resp(), '{"code":-1003,"msg":"Too many requests"}', retry_after=120)
    assert exc.retry_after == 120
    assert exc.status == 429
    assert '429' in str(exc)

    banned = IPBannedException(_Resp(), '{"code":-1003,"msg":"banned"}', retry_after=3000)
    assert banned.retry_after == 3000
    assert '418' in str(banned) or 'banned' in str(banned).lower()


def test_too_many_streams_and_stream_rate_limit_exceptions():
    from binance.common.exceptions import (
        TooManyStreamsException,
        StreamRateLimitException,
        StreamSubscribeException
    )

    too_many = TooManyStreamsException(2000, 1024)
    assert too_many.requested == 2000
    assert too_many.limit == 1024
    assert '1024' in str(too_many)

    rate_limited = StreamRateLimitException(
        -1003, 'Too much request weight used', retry_after=88)
    # backward-compat: must be catchable as StreamSubscribeException
    assert isinstance(rate_limited, StreamSubscribeException)
    assert rate_limited.code == -1003
    assert rate_limited.retry_after == 88
    assert '-1003' in str(rate_limited)


def test_rate_limit_reached_exception_message():
    from binance.common.exceptions import RateLimitReachedException
    from binance.rate_limit.types import RateLimitScope, RateLimitType
    exc = RateLimitReachedException(RateLimitScope.ACCOUNT, RateLimitType.ORDERS, '10s', 7)
    assert exc.scope == 'account'
    assert exc.limit_type == 'orders'
    assert exc.interval == '10s'
    assert exc.retry_after == 7
    assert '10s' in str(exc) and 'orders' in str(exc)
    assert isinstance(exc.scope, RateLimitScope)
    assert isinstance(exc.limit_type, RateLimitType)
    assert 'account' in str(exc) and 'orders' in str(exc)


def test_status_exception_redacts_signature_and_api_key():
    """StatusException.__str__ must not expose signature/apiKey values."""
    import yarl
    from binance.common.exceptions import StatusException

    class _Resp:
        url = yarl.URL(
            'https://api.binance.com/api/v3/order'
            '?symbol=BTCUSDT&signature=supersecrethmac&apiKey=myapikey'
        )
        status = 400

    exc = StatusException(_Resp(), '{"code":-1121,"msg":"Invalid symbol"}')
    s = str(exc)
    assert 'supersecrethmac' not in s
    assert 'myapikey' not in s
    assert 'BTCUSDT' in s
    assert '400' in s


def test_rate_limit_exception_redacts_signature_and_api_key():
    """RateLimitException.__str__ must not expose signature/apiKey values."""
    import yarl
    from binance.common.exceptions import RateLimitException

    class _Resp:
        url = yarl.URL(
            'https://api.binance.com/api/v3/order'
            '?symbol=BTCUSDT&signature=supersecrethmac&apiKey=myapikey'
        )
        status = 429

    exc = RateLimitException(_Resp(), '{"code":-1003,"msg":"Too many requests"}', retry_after=60)
    s = str(exc)
    assert 'supersecrethmac' not in s
    assert 'myapikey' not in s
    assert '429' in s


def test_ip_banned_exception_redacts_signature_and_api_key():
    """IPBannedException.__str__ must not expose signature/apiKey values."""
    import yarl
    from binance.common.exceptions import IPBannedException

    class _Resp:
        url = yarl.URL(
            'https://api.binance.com/api/v3/order'
            '?symbol=BTCUSDT&signature=supersecrethmac&apiKey=myapikey'
        )
        status = 418

    exc = IPBannedException(_Resp(), '{"code":-1003,"msg":"banned"}', retry_after=900)
    s = str(exc)
    assert 'supersecrethmac' not in s
    assert 'myapikey' not in s
    assert '418' in s or 'banned' in s.lower()


def test_redact_url_no_sensitive_params():
    """_redact_url returns the URL unchanged when no sensitive params are present."""
    from binance.common.exceptions import _redact_url
    url = 'https://api.binance.com/api/v3/time'
    assert _redact_url(url) == url


def test_redact_url_replaces_both_params():
    """_redact_url replaces both signature and apiKey values with ***."""
    from binance.common.exceptions import _redact_url
    url = 'https://api.binance.com/api/v3/order?symbol=X&signature=HMAC123&apiKey=KEY456'
    result = _redact_url(url)
    assert 'HMAC123' not in result
    assert 'KEY456' not in result
    assert 'symbol=X' in result
    assert 'signature=***' in result
    assert 'apiKey=***' in result
