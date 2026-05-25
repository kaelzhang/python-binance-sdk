from binance import SpotClient, Credentials


def test_init_client():
    """create a client with key and secret"""
    SpotClient(Credentials('key', 'secret'))


def test_init_client_key():
    """create a client only with key"""
    SpotClient(Credentials('key'))


def test_no_api_key():
    """create a client with no args"""
    SpotClient()
