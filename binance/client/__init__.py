from logging import getLogger, Logger
from typing import Optional

from binance.apis import WsApiGetters

from aioretry import RetryPolicy

from binance.subscribe.manager import SubscriptionManager
from binance.common.constants import (
    REST_API_HOST,
    STREAM_HOST,
    WS_API_HOST,
    WS_API_TIME_UNIT_QUERY,
    WS_API_TIME_UNIT_MICROSECOND,
    WS_API_TIME_UNIT_MILLISECOND,
    DEFAULT_RETRY_POLICY, DEFAULT_STREAM_TIMEOUT
)
from binance.rate_limit import RateLimiter, RateLimitSnapshot
from binance.common.types import Timeout

from .base import ClientBase


def _apply_time_unit(ws_api_host: str, time_unit) -> str:
    """Append ``?timeUnit=...`` to the WS-API host URL when opting into microseconds.

    F-13: the WS-API exposes a per-connection ``timeUnit`` option. Setting it on
    the connection URL makes EVERY timestamp on that connection (response fields
    and any server-side time handling) use the chosen unit. ``None`` (default)
    leaves the URL untouched, keeping Binance's millisecond default.

    Args:
        ws_api_host: The base ``wss://.../ws-api/v3`` URL.
        time_unit: ``None``/``'millisecond'`` (default ms, no change) or
            ``'microsecond'`` (case-insensitive) to request microseconds.

    Raises:
        ValueError: If ``time_unit`` is not a recognised value.
    """
    if time_unit is None:
        return ws_api_host

    normalized = str(time_unit).upper()

    if normalized == WS_API_TIME_UNIT_MILLISECOND:
        # Explicit millisecond is the server default -> no query needed.
        return ws_api_host

    if normalized != WS_API_TIME_UNIT_MICROSECOND:
        raise ValueError(
            "time_unit must be None, 'millisecond', or 'microsecond', "
            f'got {time_unit!r}'
        )

    separator = '&' if '?' in ws_api_host else '?'
    return (
        f'{ws_api_host}{separator}'
        f'{WS_API_TIME_UNIT_QUERY}={WS_API_TIME_UNIT_MICROSECOND}'
    )


class Client(
    ClientBase,
    WsApiGetters,
    SubscriptionManager
):
    """Async Binance REST + WebSocket client — the primary public entry point.

    Combines three building blocks via multiple inheritance:

    - ``ClientBase``: holds API credentials, signs requests, drives the
      ``RateLimiter``, and provides the generic REST escape hatch
      (``get``/``post``/...) plus ``sync_time()``.
    - ``WsApiGetters``: generated async methods for every request/response
      endpoint -- general (``get_server_time``, ``get_exchange_info``),
      market-data (``get_orderbook``, ``get_klines``, ``get_ticker``, ...),
      account (``get_account``, ``get_commission``, ...) and trading
      (``create_order``, ``cancel_order``, ``create_oco``, ...) -- each an
      id-correlated request on the shared WS-API connection.
    - ``SubscriptionManager``: manages WebSocket market-data and user-data
      stream connections via ``subscribe()`` / ``unsubscribe()``, and owns the
      shared WS-API request connection used by ``WsApiGetters``.

    Typical usage::

        client = Client(api_key='KEY', api_secret='SECRET')

        # WebSocket-API call — awaitable coroutine
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
        time_unit=None,
        stream_retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        stream_timeout: Timeout = DEFAULT_STREAM_TIMEOUT,
        rate_limit_guard: bool = True,
        rate_limiter: Optional[RateLimiter] = None,
        request_timeout: float = 10,
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
            time_unit (:obj:`str`, optional): WebSocket-API timestamp unit. ``None`` (default) or ``'millisecond'`` keeps Binance's millisecond default; ``'microsecond'`` (case-insensitive) opts the whole WS-API connection into microsecond-precision timestamps by appending ``?timeUnit=MICROSECOND`` to the connection URL.
            rate_limit_guard (:obj:`bool`, optional): when True, proactively throttle REST requests
                with a client-side weight/raw/order budget to stay under the per-IP and per-account
                caps. When False, usage is still tracked (so monitoring works) but requests are never
                delayed. Ignored when ``rate_limiter`` is supplied. Defaults to True.
            rate_limiter (:obj:`RateLimiter`, optional): inject a shared :class:`~binance.rate_limit.RateLimiter`
                instance so multiple ``Client`` objects on the same IP share one IP-level pool (F-49).
                When ``None`` (default) a private limiter is built from ``rate_limit_guard``.
            request_timeout (:obj:`float`, optional): total seconds before an aiohttp REST request
                is abandoned. Defaults to 10.
        """

        self._api_key = None
        self._api_secret = None
        self._private_key = None

        self._used_weight = {}
        self._order_count = {}
        if rate_limiter is not None:
            self._rate_limiter = rate_limiter
        else:
            self._rate_limiter = RateLimiter(enabled=bool(rate_limit_guard))
        self._time_offset = 0
        self._time_synced = False
        # Shared REST session — lazily opened on first REST call (F-12 / F-42).
        self._rest_session = None
        self._request_timeout = float(request_timeout)

        self.key(api_key)
        self.secret(api_secret)
        self._load_private_key(private_key, private_key_pass)

        self._request_params = request_params
        self._api_host = api_host

        self._stream_host = stream_host
        self._ws_api_host = _apply_time_unit(ws_api_host, time_unit)
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
        # Whether the shared WS-API connection has an authenticated session
        # (after a successful Ed25519 `session.logon`). Reset on every
        # (re)connect since the session is not persistent across reconnects.
        self._ws_api_authenticated = False
        self._logger = logger

    @property
    def logger(self) -> Logger:
        """The ``logging.Logger`` instance used by this client.

        Defaults to the logger named after the ``binance.client`` module.
        Pass a custom ``Logger`` to the constructor to route log output
        through a different handler or level.
        """
        return self._logger

    async def close(self, code: int = 4999) -> None:
        """Close all stream connections and the shared REST session.

        Extends :meth:`~binance.subscribe.manager.SubscriptionManager.close`
        to also close the shared :class:`~aiohttp.ClientSession` used by the
        generic REST escape hatch (F-12 / F-42).
        """
        await super().close(code)
        await self._close_rest_session()

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
