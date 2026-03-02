import hashlib
import hmac
import pytest
from aioresponses import aioresponses

from binance import (
    Client,
    StatusException
)

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
