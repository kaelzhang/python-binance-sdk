"""The shared, market-agnostic client base.

:class:`BaseClient` assembles the REST + WebSocket-API transports and the
stream :class:`~binance.core.transport.subscription.SubscriptionManager`, and
provides the construction / lifecycle boilerplate (``start`` / ``close`` /
``rate_limit_snapshot`` / ``_sync_time``) common to every market client. Market
modules subclass it, bind their :class:`~binance.core.market.MarketSpec`, and
install their endpoint methods.
"""

import time
from logging import getLogger, Logger
from typing import Any, ClassVar, Optional

from aioretry import RetryPolicy

from binance.core.auth import Credentials
from binance.core.common.constants import (
    DEFAULT_RETRY_POLICY,
    DEFAULT_STREAM_TIMEOUT,
)
from binance.core.common.types import Timeout
from binance.core.market import MarketSpec
from binance.core.rate_limit import RateLimiter, RateLimitSnapshot
from binance.core.transport.rest import RestTransport
from binance.core.transport.control import ControlTaskSupervisor
from binance.core.transport.subscription import SubscriptionManager
from binance.core.transport.ws_api import WsApiTransport, _apply_time_unit


class BaseClient(  # type: ignore[misc]  # diamond mixin: _ws_api_request is a Callable hint in getters and an actual method in SubscriptionManager; compatible at runtime
    RestTransport,
    WsApiTransport,
    SubscriptionManager
):
    """Async Binance client base shared by every market (Spot / Futures).

    Combines the building blocks via multiple inheritance:

    - :class:`~binance.core.transport.rest.RestTransport`: holds the shared REST
      session, signs requests, drives the ``RateLimiter``, and provides the
      generic REST escape hatch (``get``/``post``/...).
    - :class:`~binance.core.transport.ws_api.WsApiTransport`: builds the WS-API
      signed-request payload.
    - :class:`~binance.core.transport.subscription.SubscriptionManager`: manages
      WebSocket market-data and user-data stream connections via ``subscribe()``
      / ``unsubscribe()``, and owns the shared WS-API request connection used by
      the market's endpoint methods.

    Market-specific endpoint methods (``get_klines`` / ``create_order`` / ...)
    are installed onto the concrete subclass by its market module.

    Subclasses MUST bind :attr:`MARKET` to their
    :class:`~binance.core.market.MarketSpec`; the hosts and default rate-limit
    rules are taken from it.
    """

    # Bound by each concrete market client (SpotClient / UMFuturesClient).
    MARKET: ClassVar[MarketSpec]

    def __init__(
        self,
        credentials: Optional[Credentials] = None,
        *,
        request_params=None,
        # Host overrides default to the market's hosts; override for the CN
        # network or to point tests at a local mock server.
        rest_host: Optional[str] = None,
        stream_host: Optional[str] = None,
        ws_api_host: Optional[str] = None,
        time_unit=None,
        stream_retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        stream_timeout: Timeout = DEFAULT_STREAM_TIMEOUT,
        rate_limit_guard: bool = True,
        rate_limiter: Optional[RateLimiter] = None,
        request_timeout: float = 10,
        recv_window: Optional[int] = None,
        logger: Logger = getLogger(__name__)
    ):
        """Construct a market client.

        Args:
            credentials (:obj:`Credentials`, optional): API credentials and
                signing material. A single :class:`Credentials` instance may be
                shared across multiple market clients. ``None`` (default) creates
                an unauthenticated client (public endpoints only).
            request_params (:obj:`dict`, optional): Dictionary of request params
                applied to every REST call.
            time_unit (:obj:`str`, optional): WebSocket-API timestamp unit.
                ``None`` (default) or ``'millisecond'`` keeps Binance's
                millisecond default; ``'microsecond'`` (case-insensitive) opts
                the whole WS-API connection into microsecond-precision timestamps
                by appending ``?timeUnit=MICROSECOND`` to the connection URL.
            rate_limit_guard (:obj:`bool`, optional): when True, proactively
                throttle REST requests with a client-side weight/raw/order budget
                to stay under the per-IP and per-account caps. When False, usage
                is still tracked (so monitoring works) but requests are never
                delayed. Ignored when ``rate_limiter`` is supplied. Defaults to
                True.
            rate_limiter (:obj:`RateLimiter`, optional): inject a shared
                :class:`~binance.core.rate_limit.RateLimiter` instance so
                multiple clients on the same IP share one IP-level pool (F-49).
                When ``None`` (default) a private limiter is built from
                ``rate_limit_guard``. Never share a limiter across markets.
            request_timeout (:obj:`float`, optional): total seconds before an
                aiohttp REST request is abandoned. Defaults to 10.
            recv_window (:obj:`int`, optional): default ``recvWindow`` (ms)
                included in every signed WS-API request. Clamped to at most
                60000; injected automatically unless the caller already passes
                ``recvWindow`` explicitly. ``None`` (default) lets Binance use
                its server-side default (5000 ms).
        """

        market = self.MARKET

        self._credentials = credentials or Credentials()

        self._used_weight = {}
        self._order_count = {}
        if rate_limiter is not None:
            self._rate_limiter = rate_limiter
        else:
            self._rate_limiter = RateLimiter(
                rules=market.rules,
                enabled=bool(rate_limit_guard),
                ws_message_rule=market.ws_message_rule,
            )
        self._time_offset = 0
        self._time_synced = False
        # Shared REST session — lazily opened on first REST call (F-12 / F-42).
        self._rest_session = None
        self._request_timeout = float(request_timeout)

        self._request_params = request_params
        self._rest_host = rest_host if rest_host is not None else market.rest_host
        # F-48: client-level recvWindow default (None = use server default).
        self._recv_window = (
            min(int(recv_window), 60000) if recv_window is not None else None
        )

        self._stream_host = (
            stream_host if stream_host is not None else market.stream_host
        )
        resolved_ws_api_host = (
            ws_api_host if ws_api_host is not None else market.ws_api_host
        )
        self._ws_api_host = _apply_time_unit(resolved_ws_api_host, time_unit)
        self._stream_retry_policy = stream_retry_policy
        self._stream_timeout = stream_timeout

        self._receiving = True
        self._handler_ctx = None
        # Per-path data stream connections keyed by the path string returned
        # by the market's ``data_stream_router``.  Lazily opened on the first
        # subscription whose stream name routes to that path.
        self._data_streams = {}
        # Wire the market's stream-routing config so the subscription manager
        # partitions stream names across the right per-path connections.
        self._data_stream_router = market.data_stream_router
        self._default_data_stream_path = market.data_stream_paths[0]
        self._user_stream = None
        self._subscribed = set()
        self._stream_names = set()
        self._want_user_stream = False
        self._user_unsubscribe_inflight = False
        self._control_tasks = ControlTaskSupervisor(logger)
        # Captured ``subscriptionId`` from the Spot
        # ``userDataStream.subscribe.signature`` response (2025-08-12 Spot
        # CHANGELOG). Cleared on unsubscribe.  Other markets leave it None.
        self._user_subscription_id: Optional[Any] = None
        # Whether the shared WS-API connection has an authenticated session
        # (after a successful Ed25519 `session.logon`). Reset on every
        # (re)connect since the session is not persistent across reconnects.
        self._ws_api_authenticated = False
        self._logger = logger

    @property
    def logger(self) -> Logger:
        """The ``logging.Logger`` instance used by this client.

        Defaults to the logger named after the ``binance.core.client_base``
        module. Pass a custom ``Logger`` to the constructor to route log output
        through a different handler or level.
        """
        return self._logger

    @property
    def user_subscription_id(self) -> Optional[Any]:
        """The ``subscriptionId`` returned by the most recent
        ``userDataStream.subscribe.signature`` response (Spot WS-API).

        Set on every successful user-stream subscribe and cleared on
        unsubscribe. ``None`` when the client has not subscribed to its
        user-data stream, when the response did not include a
        ``subscriptionId``, or for markets that do not surface one (UM/CM
        futures use a different user-stream flow entirely).

        Docs:
          https://developers.binance.com/docs/binance-spot-api-docs/CHANGELOG (2025-08-12)
          https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests
        """
        return self._user_subscription_id

    async def _sync_time(self) -> int:
        """Internal: sync the local clock offset against Binance server time.

        Issues the WebSocket-API ``time`` request (``get_server_time``) and
        stores ``server_time - local_time`` (ms) as an offset that is added to
        the ``timestamp`` of every signed request, preventing ``-1021``
        (timestamp outside recvWindow) rejections from a drifting local clock.
        Called automatically before the first signed request and re-armed
        whenever a ``-1021`` is seen. This is fully internal — users do not
        need to call or manage it. Returns the new offset in milliseconds.

        ``time`` is a public (``NONE``) request, so this never re-triggers the
        signed-request time-sync arming (no recursion).
        """
        # get_server_time is installed on the concrete market client.
        res = await self.get_server_time()  # type: ignore[attr-defined]
        self._time_offset = int(res['serverTime']) - int(time.time() * 1000)
        self._time_synced = True
        return self._time_offset

    async def close(self, code: int = 4999) -> None:
        """Close all stream connections and the shared REST session.

        Extends
        :meth:`~binance.core.transport.subscription.SubscriptionManager.close`
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
