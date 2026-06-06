import pytest

from binance import Credentials, SpotClient, SubType


class _FakeWsApiStream:
    def __init__(self):
        self.sent = []

    async def send(self, req):
        self.sent.append(req)
        if req['method'] == 'userDataStream.subscribe.signature':
            return {'subscriptionId': len(self.sent)}
        return None


@pytest.mark.asyncio
async def test_repeated_spot_user_subscribe_reuses_existing_subscription(
    monkeypatch,
):
    client = SpotClient(
        Credentials(api_key='TESTAPIKEY', api_secret='TESTSECRET'),
        ws_api_host='ws://unused',
    ).start()
    stream = _FakeWsApiStream()
    monkeypatch.setattr(client, '_get_ws_api_stream', lambda: stream)

    try:
        await client.subscribe(SubType.USER)
        first_subscription_id = client._user_subscription_id

        await client.subscribe(SubType.USER)

        methods = [req['method'] for req in stream.sent]
        assert methods == ['userDataStream.subscribe.signature']
        assert client._user_subscription_id == first_subscription_id
    finally:
        await client.close()
