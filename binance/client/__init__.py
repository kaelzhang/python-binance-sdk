from logging import getLogger, Logger

from binance.apis import RestAPIGetters

from aioretry import RetryPolicy

from binance.subscribe.manager import SubscriptionManager
from binance.common.constants import (
    REST_API_HOST,
    STREAM_HOST,
    WS_API_HOST,
    DEFAULT_RETRY_POLICY, DEFAULT_STREAM_TIMEOUT
)
from binance.rate_limit import RateLimiter, RateLimitSnapshot
from binance.common.types import Timeout

from .base import ClientBase


class Client(
    ClientBase,
    RestAPIGetters,
    SubscriptionManager
):
    """Async Binance REST + WebSocket client — the primary public entry point.

    Combines three building blocks via multiple inheritance:

    - ``ClientBase``: holds API credentials, signs and sends aiohttp requests,
      captures rate-limit response headers, and drives the ``RateLimiter``.
    - ``RestAPIGetters``: generated async methods for every ``/api/`` REST
      endpoint (e.g. ``ping``, ``get_orderbook``, ``create_order``).
    - ``SubscriptionManager``: manages WebSocket market-data and user-data
      stream connections via ``subscribe()`` / ``unsubscribe()``.

    Typical usage::

        client = Client(api_key='KEY', api_secret='SECRET')

        # REST call — awaitable coroutine
        info = await client.get_exchange_info()

        # Subscribe to a trade stream and attach a handler
        client.handler(on_trade)
        await client.subscribe('btcusdt@trade')

        # Inspect rate-limit usage without a network round-trip
        snap = client.rate_limit_snapshot()

    See ``__init__`` for the full list of constructor keyword arguments and
    ``rate_limit_snapshot`` for monitoring rate-limit budgets.
    """
    def __init__(
        self,
        api_key=None,
        api_secret=None,
        private_key=None,
        private_key_pass=None,
        request_params=None,
        # so that you can change api_host for CN network
        api_host: str = REST_API_HOST,
        # website_host=WEBSITE_HOST,
        stream_host: str = STREAM_HOST,
        ws_api_host: str = WS_API_HOST,
        stream_retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        stream_timeout: Timeout = DEFAULT_STREAM_TIMEOUT,
        rate_limit_guard: bool = True,
        logger: Logger = getLogger(__name__)
    ):
        """Binance API Client constructor

        Args:
            api_key (str): API Key
            api_secret (str): API Secret used for HMAC-SHA256 signing (deprecated by Binance).
            private_key (str or bytes, optional): Ed25519 or RSA private key for asymmetric
                request signing.  Can be the PEM content (``str`` or ``bytes``) or a file path
                to a PEM file.  When supplied, the private key is used for signing instead of
                ``api_secret``.  Binance recommends Ed25519 (fastest) or RSA over the
                deprecated HMAC API keys.
            private_key_pass (str or bytes, optional): Password to decrypt an encrypted PEM
                private key.  Pass ``None`` (default) for unencrypted keys.
            requests_params (:obj:`dict`, optional): Dictionary of requests params to use for all calls
            rate_limit_guard (:obj:`bool`, optional): when True, proactively throttle REST requests with a client-side weight/raw/order budget to stay under the per-IP and per-account caps. When False, usage is still tracked (so monitoring works) but requests are never delayed. Defaults to True.
        """

        self._api_key = None
        self._api_secret = None
        self._private_key = None

        self._used_weight = {}
        self._order_count = {}
        self._rate_limiter = RateLimiter(enabled=bool(rate_limit_guard))
        self._time_offset = 0
        self._time_synced = False

        self.key(api_key)
        self.secret(api_secret)
        self._load_private_key(private_key, private_key_pass)

        self._request_params = request_params
        self._api_host = api_host

        self._stream_host = stream_host
        self._ws_api_host = ws_api_host
        self._stream_retry_policy = stream_retry_policy
        self._stream_timeout = stream_timeout

        self._receiving = True
        self._handler_ctx = None
        self._data_stream = None
        self._user_stream = None
        self._subscribed = set()
        self._stream_names = set()
        self._want_user_stream = False
        self._user_unsubscribe_inflight = False
        self._user_recovering = False
        self._logger = logger

    @property
    def logger(self) -> Logger:
        """The ``logging.Logger`` instance used by this client.

        Defaults to the logger named after the ``binance.client`` module.
        Pass a custom ``Logger`` to the constructor to route log output
        through a different handler or level.
        """
        return self._logger

    def rate_limit_snapshot(self) -> RateLimitSnapshot:
        """Return a point-in-time RateLimitSnapshot of all rate-limit pools.

        Read-only and local (no network); safe to poll from a monitoring loop.
        """
        return self._rate_limiter.snapshot()

    def key(self, key):
        """Defines or changes api key. This method is unnecessary if we only request APIs of `SecurityType.NONE`

        Args:
            key (str): api key

        Returns:
            self
        """

        if key:
            self._api_key = key
        return self

    def secret(self, secret):
        """Defines or changes api secret, especially when we have not define api secret in Client constructor.

        `secret` is not always required for using binance-sdk.

        Args:
            secret (str): api secret

        Returns:
            self
        """

        if secret:
            self._api_secret = secret
        return self
