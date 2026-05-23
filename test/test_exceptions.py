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


@pytest.mark.asyncio
async def test_invalid_json():
    """Test Invalid response Exception"""

    with pytest.raises(InvalidResponseException, match='invalid response'):
        with aioresponses() as m:
            m.get('https://api.binance.com/api/v3/time', body='<head></html>')

            client = Client('api_key')
            await client.get_server_time()


@pytest.mark.asyncio
async def test_api_exception():
    """Test Status Exception"""
    with pytest.raises(StatusException, match='status'):
        with aioresponses() as m:
            json_obj = {"code": 1002, "msg": "Invalid API call"}
            m.get('https://api.binance.com/api/v3/time', payload=json_obj, status=400)

            client = Client('api_key')
            await client.get_server_time()


@pytest.mark.asyncio
async def test_api_exception_invalid_json():
    """
    Test Status Exception, StatusException comes before InvalidResponseException
    """

    with pytest.raises(StatusException):
        with aioresponses() as m:
            not_json_str = "<html><body>Error</body></html>"
            m.get('https://api.binance.com/api/v3/time', body=not_json_str, status=400)

            client = Client('api_key')
            await client.get_server_time()


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
    exc = RateLimitReachedException('account', 'orders', '10s', 7)
    assert exc.scope == 'account'
    assert exc.limit_type == 'orders'
    assert exc.interval == '10s'
    assert exc.retry_after == 7
    assert '10s' in str(exc) and 'orders' in str(exc)
