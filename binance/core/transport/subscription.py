import asyncio
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Iterable,
    Set,
    Tuple,
    Optional,
    cast
)
from logging import Logger

from aioretry import RetryPolicy

from binance.core.auth import Credentials
from binance.core.common.constants import (
    DEFAULT_STREAM_CLOSE_CODE,
    EVENT_SERVER_SHUTDOWN,
    EVENT_STREAM_TERMINATED,
    SecurityType,
    SubType,
    STREAM_KEY_RATE_LIMITS,
    STREAM_KEY_RESULT,
    WS_API_METHOD_SESSION_LOGON,
    ERROR_CODE_UNAUTHORIZED,
    ERROR_CODE_INVALID_TIMESTAMP
)
from binance.core.common.exceptions import (
    APIKeyNotDefinedException,
    APISecretNotDefinedException,
    InvalidHandlerException,
    StreamSubscribeException
)
from binance.core.common.types import (
    StreamError,
    StreamName,
    StreamErrorPhase,
    Timeout
)
from binance.core.common.utils import (
    format_msg,
    repr_exception
)
from binance.core.transport.rest import _reject_float_params
from binance.core.rate_limit import ConnectionLease, RateLimiter

from .stream import Stream
from .control import ControlTaskSupervisor
from .data_streams import DataStreamRegistry, data_connection_id
from binance.core.handlers.context import HandlerContext

# pylint: disable=no-member


def _extract_event_type(msg):
    """Return the Binance event type ('e') from any documented message shape."""
    if not isinstance(msg, dict):
        return None
    for container_key in ('data', 'event'):
        container = msg.get(container_key)
        if isinstance(container, dict) and 'e' in container:
            return container['e']
    return msg.get('e')


# Spot WS-API user-data stream subscribe/unsubscribe weight.  Both
# ``userDataStream.subscribe.signature`` and ``userDataStream.unsubscribe``
# are documented as **weight 2** per
# https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests
_USER_DATA_STREAM_SUB_WEIGHT = 2


_data_connection_id = data_connection_id


class SubscriptionManager:
    """Internal mixin merged into ``Client`` that manages data and user WebSocket stream lifecycles."""

    # Per-path data stream connections.  Keyed by the path string returned by
    # the market's ``data_stream_router`` (e.g. ``'/stream'`` for Spot/CM,
    # ``'/public/stream'`` or ``'/market/stream'`` for UM).  Lazily opened on
    # the first subscription whose stream name routes to that path.
    _data_stream_registry: DataStreamRegistry
    # The shared WS-API request/response connection (wss://ws-api...). It
    # carries BOTH the user-data stream subscription and every `_ws_api_request`
    # (former REST) call -- one connection, lazily opened.
    _user_stream: Optional[Stream]
    _user_connection_lease: Optional[ConnectionLease]
    _subscribed: Set[tuple]
    _stream_names: Set[str]
    _subscription_lock: Optional[asyncio.Lock]
    _stream_host: str
    _ws_api_host: str
    _stream_retry_policy: RetryPolicy
    _stream_timeout: Timeout
    _rate_limiter: RateLimiter
    _logger: Logger
    _want_user_stream: bool
    _user_unsubscribe_inflight: bool
    # Captured response ``subscriptionId`` from
    # ``userDataStream.subscribe.signature`` (2025-08-12 Spot CHANGELOG).
    _user_subscription_id: Optional[Any]
    _ws_api_authenticated: bool
    _control_tasks: ControlTaskSupervisor
    # The market's data-stream router (set from ``MarketSpec``).  Maps a wire
    # stream name to the path that should carry it.
    _data_stream_router: Callable[[str], str]
    # The default (first) data-stream path.  Used for plain ``list_subscriptions``
    # which does not target a specific stream name.
    _default_data_stream_path: str
    # Credentials / signing live on ClientBase; declared here for the WS-API
    # request path that runs on the merged Client via these mixin attributes.
    _credentials: Credentials
    _time_offset: int
    _time_synced: bool
    _recv_window: Optional[int]
    _handler_ctx: Optional[HandlerContext]
    _sync_time: Callable[[], Awaitable]
    # Cross-mixin method defined on ClientBase but used by SubscriptionManager
    _ws_api_signature_params: Callable[..., dict]

    def start(self):
        """Starts receiving messages.

        By calling this method, the client will not actually start the stream connection.

        Returns:
            self
        """

        self._receiving = True
        return self

    def stop(self):
        """Stops receiving messages.

        By calling this method, the client only ignores all incomming stream message, and will not close the stream connection.

        Returns:
            self
        """

        self._receiving = False
        return self

    async def close(
        self,
        code: int = DEFAULT_STREAM_CLOSE_CODE
    ) -> None:
        """Closes stream connection, clear all stream subscriptions and clear all handlers.

        Args:
            code (:obj:`int`, optional): the close code for python library websockets. Defaults to 4999, and it should be in the range 4000 - 4999
        """

        self._receiving = False
        self._want_user_stream = False
        self._user_unsubscribe_inflight = False
        self._ws_api_authenticated = False
        await self._control_tasks.close()

        self._sync_data_stream_registry_refs()
        await self._data_stream_registry.close_all(code)

        if self._user_stream:
            await self._user_stream.close(code)
            self._user_stream = None
            if self._user_connection_lease is not None:
                self._rate_limiter.unregister_connection(
                    self._user_connection_lease
                )
                self._user_connection_lease = None

        self._handler_ctx = None

    async def _receive(self, msg, *, origin: Optional[Stream] = None) -> None:
        """Dispatch ``msg`` delivered by ``origin`` (the originating stream).

        Args:
            msg: The raw message payload from the server.
            origin: The :class:`Stream` instance that delivered the message.
                Must be passed by the per-stream ``on_message`` wrapper so
                ``serverShutdown`` (per 2026-05-06 Spot changelog: now sent on
                BOTH WS-API and market-data WS streams) recycles the
                connection that delivered it rather than always recycling
                the market-data stream. ``None`` (legacy) falls back to
                recycling every known data stream.
        """
        if not self._receiving:
            return

        event_type = _extract_event_type(msg)

        if event_type == EVENT_SERVER_SHUTDOWN:
            self._logger.warning(
                'serverShutdown received; recycling originating stream')
            if origin is not None:
                self._control_tasks.run_once(
                    ('serverShutdown', id(origin)),
                    origin.recycle
                )
            else:
                # Fallback for callers that have not yet been migrated to the
                # bound-origin callback (only legacy tests / direct calls).
                for stream in list(self._data_streams.values()):
                    self._control_tasks.run_once(
                        ('serverShutdown', id(stream)),
                        stream.recycle
                    )
            await asyncio.sleep(0)
            return

        if event_type == EVENT_STREAM_TERMINATED:
            self._control_tasks.run_once(
                'eventStreamTerminated:user-recovery',
                self._recover_user_stream_if_needed
            )
            await asyncio.sleep(0)

        # Invariant: a dispatchable message only arrives after a subscription
        # opened a stream, and subscribing always creates the handler context
        # first (serverShutdown returns above; it never reaches here). Assert
        # the invariant for the type-checker rather than silently dropping.
        assert self._handler_ctx is not None
        await self._handler_ctx.receive(msg)

    def _get_handler_ctx(self) -> HandlerContext:
        if not self._handler_ctx:
            self._handler_ctx = HandlerContext(self)

        return self._handler_ctx

    def _get_subscription_lock(self) -> asyncio.Lock:
        lock = getattr(self, '_subscription_lock', None)
        if lock is None:
            lock = asyncio.Lock()
            self._subscription_lock = lock
        return lock

    @property
    def _data_streams(self) -> Dict[str, Stream]:
        return self._data_stream_registry.streams

    @_data_streams.setter
    def _data_streams(self, streams: Dict[str, Stream]) -> None:
        self._data_stream_registry.streams = streams

    @property
    def _data_connection_leases(self) -> Dict[str, ConnectionLease]:
        return self._data_stream_registry.leases

    @_data_connection_leases.setter
    def _data_connection_leases(
        self,
        leases: Dict[str, ConnectionLease],
    ) -> None:
        self._data_stream_registry.leases = leases

    def _sync_data_stream_registry_refs(self) -> None:
        self._data_stream_registry.set_stream_factory(Stream)

    def _get_data_connection_lease(self, path: str) -> ConnectionLease:
        self._sync_data_stream_registry_refs()
        return self._data_stream_registry.get_lease(path)

    def _drop_unopened_data_connection_lease(self, path: str) -> None:
        self._sync_data_stream_registry_refs()
        self._data_stream_registry.drop_unopened_lease(path)

    def _get_user_connection_lease(self) -> ConnectionLease:
        lease = self._user_connection_lease
        if lease is None:
            lease = self._rate_limiter.open_connection(
                kind='ws_api',
                route=self._ws_api_host,
                label='user',
            )
            self._user_connection_lease = lease
        return lease

    def _schedule_recycle(self, key: tuple, stream: Stream) -> None:
        self._control_tasks.run_once(
            ('stream-error-recycle', *key, id(stream)),
            stream.recycle,
        )

    def _get_data_stream(self, path: Optional[str] = None) -> Stream:
        """Return the data stream for ``path``, opening it lazily.

        Args:
            path: A path key returned by the market's ``data_stream_router``
                (e.g. ``'/stream'``, ``'/public/stream'``, ``'/market/stream'``).
                ``None`` (default) falls back to the market's first / default
                data-stream path.

        Returns:
            The cached :class:`Stream` for ``path``.  Subsequent calls with
            the same path return the same instance.
        """
        if path is None:
            path = self._default_data_stream_path
        self._sync_data_stream_registry_refs()
        return self._data_stream_registry.get_stream(path)

    async def _receive_data_stream_message(self, stream: Stream, msg) -> None:
        await self._receive(msg, origin=stream)

    def _build_data_resubscribe(self, path: str) -> Callable[[], Awaitable[None]]:
        """Return an ``on_connected`` callback that resubscribes streams routed to ``path``.

        Each data stream gets its own callback so reconnects only replay the
        subscriptions that belong on that path -- not every subscription the
        client tracks.
        """
        async def _resubscribe_for_path() -> None:
            await self._resubscribe_path(path)

        return _resubscribe_for_path

    def _get_ws_api_stream(self) -> Stream:
        """Return the shared WS-API connection, opening it lazily.

        ONE connection to ``wss://ws-api...`` is shared by the user-data stream
        subscription and every :meth:`_ws_api_request` (former REST) call. On
        each (re)connect, ``on_connected`` runs the Ed25519 ``session.logon``
        (when applicable) and replays the user-stream subscription;
        ``on_response`` reconciles the authoritative ``rateLimits`` array of
        every response into the shared rate-limit core.
        """
        if self._user_stream is None:
            stream_holder: Dict[str, Stream] = {}
            connection_lease = self._get_user_connection_lease()

            async def on_message_bound(msg):
                await self._receive(msg, origin=stream_holder['stream'])

            async def on_response_bound(msg):
                self._reconcile_ws_api_rate_limits(msg)

            stream = Stream(
                self._ws_api_host,
                on_message=on_message_bound,
                on_connected=self._on_ws_api_connected,
                on_response=on_response_bound,
                retry_policy=self._stream_retry_policy,
                timeout=self._stream_timeout,
                logger=self._logger,
                rate_limiter=self._rate_limiter,
                connection_id='user',
                connection_lease=connection_lease,
            ).connect()
            stream_holder['stream'] = stream
            self._user_stream = stream

        return self._user_stream

    def _reconcile_ws_api_rate_limits(self, msg) -> None:
        """``on_response`` hook: reconcile a WS-API response's ``rateLimits``.

        Called by the shared WS-API :class:`Stream` with the full id-correlated
        response message. Two distinct arrays are folded into the shared
        rate-limit core:

        - the top-level authoritative ``rateLimits`` array (present on every
          WS-API response; carries ``count``) reconciles current pool *usage*,
          keeping the local weight/orders/raw pools exact; and
        - an ``exchangeInfo`` response's ``result.rateLimits`` array (carries
          ``limit``) reconfigures the pool *caps* to the account's real limits
          (mirrors the former REST ``_handle_response`` behaviour).
        """
        if not isinstance(msg, dict):
            return

        self._rate_limiter.sync_from_ws_rate_limits(
            msg.get(STREAM_KEY_RATE_LIMITS))

        # `exchangeInfo` carries the pool caps inside its `result`.
        result = msg.get(STREAM_KEY_RESULT)
        if isinstance(result, dict) and STREAM_KEY_RATE_LIMITS in result:
            self._rate_limiter.configure_from_exchange_info(
                result[STREAM_KEY_RATE_LIMITS])

    async def _on_ws_api_connected(self) -> None:
        """``on_connected`` hook for the shared WS-API connection.

        Runs on every (re)connect. The session.logon optimization is NOT
        persistent across reconnects, so the authenticated flag is reset first;
        an Ed25519 key then re-logs on. Finally the user-data stream
        subscription is replayed.

        Both the logon and resubscribe phases are guarded: a failure in either
        is ERROR-logged, delivered to registered ``StreamErrorHandlerBase``
        instances, and triggers a ``recycle()`` so aioretry starts a fresh
        reconnect cycle.
        """
        self._ws_api_authenticated = False
        try:
            await self._ws_api_session_logon_if_needed()
        except Exception as e:
            self._logger.error(format_msg(
                'WS-API session.logon failed after reconnect: %s',
                repr_exception(e)))
            error = StreamError(
                stream=StreamName.USER,
                phase=StreamErrorPhase.LOGON,
                exception=e,
                recovering=True
            )
            if self._user_stream is not None:
                self._schedule_recycle(
                    (StreamName.USER, StreamErrorPhase.LOGON),
                    self._user_stream,
                )
            if self._handler_ctx is not None:
                await self._handler_ctx.dispatch_stream_error(error)
            return
        await self._resubscribe_user()

    async def _ws_api_session_logon_if_needed(self) -> None:
        """Authenticate the WS-API session via ``session.logon`` (Ed25519 only).

        When the client holds an Ed25519 private key, sends a signed
        ``session.logon`` so subsequent SIGNED requests on this connection may
        omit ``apiKey``+``signature`` (still sending ``timestamp``). HMAC/RSA/
        no-key clients sign every request per-request and skip logon. A failed
        logon (e.g. ``-2015`` revoked key) leaves the session unauthenticated
        and is surfaced to the caller.
        """
        if not self._credentials.is_ed25519():
            return

        params = self._ws_api_signature_params()
        # session.logon costs 2 request weight, but we deliberately do NOT
        # proactively acquire it here: this runs inside the on_connected
        # callback, where a RAISE/SLEEP from the limiter could stall or break
        # reconnection. The logon RESPONSE carries an authoritative `rateLimits`
        # array that the on_response hook (_reconcile_ws_api_rate_limits) folds
        # into the shared core, so the +2 is reconciled immediately after logon
        # (once per connection).
        assert self._user_stream is not None  # only called from on_connected which fires after stream is open
        await self._user_stream.send({
            'method': WS_API_METHOD_SESSION_LOGON,
            'params': params
        })
        self._ws_api_authenticated = True
        self._logger.info(
            format_msg('WS-API session authenticated via session.logon'))

    def _ws_api_auth_params(
        self,
        method: str,
        params: dict,
        security: SecurityType
    ) -> dict:
        """Attach the auth fields a WS-API ``security`` level requires.

        - ``NONE``: returns ``params`` unchanged (public endpoint).
        - ``USER_STREAM`` / ``MARKET_DATA`` (``need_api_key=True, need_signed=False``):
          adds only ``apiKey`` — no timestamp, no signature.
        - ``TRADE``/``USER_DATA`` (SIGNED): when the connection already holds an
          authenticated session (Ed25519 ``session.logon``), only a
          ``timestamp`` (+offset) is added -- ``apiKey``/``signature`` are
          omitted; otherwise the full raw-value signed payload
          (``apiKey``+``timestamp``+``signature``) is built.

        Credential presence is validated by the caller
        (:meth:`_ws_api_request`) before any network round-trip, so this only
        assembles the auth fields.
        """
        need_api_key = security.requires_api_key
        need_signed = security.requires_signature

        if not need_api_key:
            # SecurityType.NONE -> public, no credentials.
            return params

        if not need_signed:
            # USER_STREAM / MARKET_DATA: apiKey only, no timestamp or signature.
            return {**params, 'apiKey': self._credentials.api_key}

        if self._ws_api_authenticated:
            # SIGNED + session logged on: omit apiKey + signature, keep timestamp.
            return {
                **params,
                'timestamp': int(time.time() * 1000) + self._time_offset
            }

        # SIGNED, per-request: apiKey + timestamp + signature.
        return self._ws_api_signature_params(**params)

    async def _ws_api_request(
        self,
        method: str,
        params: Optional[dict] = None,
        *,
        security: SecurityType,
        weight: int,
        is_order: bool = False
    ) -> Any:
        """Issue a request over the shared WS-API connection and return its result.

        The single entry point for every former-REST operation now served by
        the WebSocket API. It drops ``None`` params, attaches the auth fields
        the ``security`` level requires (per-request signing, or none when an
        Ed25519 session is logged on -- see :meth:`_ws_api_auth_params`),
        proactively accounts the request weight against the shared rate-limit
        core, lazily opens the WS-API connection, sends the
        ``{method, params}`` frame, and returns the ``result``. The response's
        authoritative ``rateLimits`` array is reconciled by the connection's
        ``on_response`` hook (:meth:`_reconcile_ws_api_rate_limits`).

        Args:
            method: WS-API method name (e.g. ``'depth'``, ``'order.place'``).
            params: Request params; ``None`` values are dropped.
            security: The endpoint's :class:`SecurityType` (keyword-only).
            weight: The endpoint's request weight (keyword-only).
            is_order: ``True`` for order-placing endpoints (keyword-only).

        Returns:
            The ``result`` field of the WS-API response.

        Raises:
            APIKeyNotDefinedException / APISecretNotDefinedException: missing
                credentials for the security level.
            StreamSubscribeException / StreamRateLimitException: server error.
        """
        request_params = {
            key: value
            for key, value in (params or {}).items()
            if value is not None
        }

        # F-07: reject float values before any network round-trip.
        _reject_float_params(request_params)

        # F-48: inject the client-level recv_window when the caller did not
        # supply one explicitly and the endpoint is signed.
        need_api_key = security.requires_api_key
        need_signed = security.requires_signature
        if need_signed and self._recv_window is not None:
            if 'recvWindow' not in request_params:
                request_params['recvWindow'] = min(
                    int(self._recv_window), 60000
                )

        # Validate credentials BEFORE any network round-trip (mirrors the REST
        # `_request` ordering), so a signed request lacking credentials raises
        # immediately rather than first issuing the lazy `time` sync.
        if need_api_key and self._credentials.api_key is None:
            raise APIKeyNotDefinedException(method)
        if need_signed and not self._credentials.has_signing():
            raise APISecretNotDefinedException(method)

        # Lazily sync the server-time offset before the FIRST signed request
        # (mirrors the old REST `_request`). `_sync_time()` itself issues the
        # unsigned (NONE) WS-API `time` request, so `need_signed` is False there
        # and this never recurses.
        if need_signed and not self._time_synced:
            await self._sync_time()

        request_params = self._ws_api_auth_params(method, request_params, security)

        await self._rate_limiter.acquire_request(weight=weight, is_order=is_order)

        stream = self._get_ws_api_stream()

        request: dict = {'method': method}
        if request_params:
            request['params'] = request_params

        try:
            return await stream.send(request)
        except StreamSubscribeException as e:
            if e.code == ERROR_CODE_UNAUTHORIZED:
                # The session was revoked/expired server-side (-2015). Drop the
                # authenticated flag so the next request re-signs per-request
                # and the next (re)connect re-runs session.logon.
                self._ws_api_authenticated = False
            elif e.code == ERROR_CODE_INVALID_TIMESTAMP:
                # -1021: our timestamp fell outside the recvWindow (clock
                # drift). Re-arm the lazy time-sync so the next signed request
                # re-fetches the server-time offset (mirrors the REST path).
                self._time_synced = False
            raise

    def _split_subscriptions(
        self,
        subscriptions: Iterable[tuple]
    ) -> Tuple[List[tuple], List[tuple]]:
        market_subscriptions = []
        user_subscriptions = []

        for subscription in subscriptions:
            if len(subscription) > 0 and subscription[0] == SubType.USER:
                user_subscriptions.append(subscription)
            else:
                market_subscriptions.append(subscription)

        return market_subscriptions, user_subscriptions

    async def _subscribe_only(
        self,
        subscribe: bool,
        subscriptions: Iterable[tuple]
    ) -> None:
        raw_params = await self._get_handler_ctx().subscribe_params(
            subscribe,
            subscriptions
        )
        # Market subscriptions always produce string stream names; cast is safe here
        params: Tuple[str, ...] = cast(Tuple[str, ...], raw_params)

        # Partition the requested stream names by the path they belong on,
        # then issue ONE SUBSCRIBE / UNSUBSCRIBE per path on the corresponding
        # connection.  Each path's connection holds its own ws-streams pool.
        per_path: Dict[str, List[str]] = {}
        for name in params:
            per_path.setdefault(self._data_stream_router(name), []).append(name)

        reserved_paths: List[str] = []
        sent_ok: List[Tuple[str, List[str]]] = []
        try:
            # Reserve stream slots per-path against each path's own
            # ws-streams pool. Compute the projected per-path total from the
            # currently committed names so reserve_streams is idempotent across
            # resubscribes.
            if subscribe:
                for path, names in per_path.items():
                    current = {
                        n for n in self._stream_names
                        if self._data_stream_router(n) == path
                    }
                    projected = current | set(names)
                    self._rate_limiter.reserve_streams(
                        self._get_data_connection_lease(path), len(projected)
                    )
                    reserved_paths.append(path)

            for path, names in per_path.items():
                stream = self._get_data_stream(path)
                await stream.send({
                    'method': 'SUBSCRIBE' if subscribe else 'UNSUBSCRIBE',
                    'params': tuple(names)
                })
                sent_ok.append((path, names))
        except Exception:
            if subscribe:
                # Roll back the optimistic stream reservation on EVERY path
                # whose reserve was raised by this call; ``len(committed)`` is
                # always <= cap so this never raises.
                for path in reserved_paths:
                    committed = sum(
                        1 for n in self._stream_names
                        if self._data_stream_router(n) == path
                    )
                    lease = self._data_connection_leases.get(path)
                    if lease is not None:
                        self._rate_limiter.reserve_streams(lease, committed)
                for path in per_path:
                    self._drop_unopened_data_connection_lease(path)
            raise

        if subscribe:
            for _path, names in sent_ok:
                self._stream_names.update(names)
        else:
            for path, names in sent_ok:
                removed = self._stream_names & set(names)
                self._stream_names.difference_update(names)
                self._rate_limiter.release_streams(
                    self._get_data_connection_lease(path), len(removed)
                )

    async def _subscribe_user_only(
        self,
        subscribe: bool,
        subscriptions: Iterable[tuple]
    ) -> None:
        params = await self._get_handler_ctx().subscribe_params(
            subscribe,
            subscriptions
        )

        stream = self._get_ws_api_stream()

        for param in params:
            method = (
                'userDataStream.subscribe.signature'
                if subscribe
                else 'userDataStream.unsubscribe'
            )

            req: Dict[str, Any] = {'method': method}

            if param:
                req['params'] = param

            # Both userDataStream.subscribe.signature and
            # userDataStream.unsubscribe carry **weight 2** per docs:
            # https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/user-data-stream-requests
            # ``stream.send`` only enforces the per-connection message rate;
            # the shared IP REQUEST_WEIGHT pool needs an explicit charge.
            await self._rate_limiter.acquire_request(
                weight=_USER_DATA_STREAM_SUB_WEIGHT,
                is_order=False,
            )

            result = await stream.send(req)

            # The 2025-08-12 Spot CHANGELOG added a ``subscriptionId`` to the
            # subscribe **response** (nested in ``result``).  Capture it so
            # callers can introspect which subscription this connection
            # opened; clear it on unsubscribe.
            # Docs: https://developers.binance.com/docs/binance-spot-api-docs/CHANGELOG
            if subscribe:
                if isinstance(result, dict) and 'subscriptionId' in result:
                    self._user_subscription_id = result['subscriptionId']
            else:
                self._user_subscription_id = None

    # subscribe to the stream for symbols
    async def _subscribe(
        self,
        subscribe: bool,
        args: Tuple
    ):
        async with self._get_subscription_lock():
            subscriptions = self._get_handler_ctx().overload_subscriptions(*args)
            market_subscriptions, user_subscriptions = self._split_subscriptions(
                subscriptions
            )
            if subscribe and user_subscriptions:
                user_subscriptions = [
                    subscription
                    for subscription in user_subscriptions
                    if subscription not in self._subscribed
                ]

            if len(market_subscriptions) > 0:
                await self._subscribe_only(subscribe, market_subscriptions)

            if len(user_subscriptions) > 0:
                prev_want_user_stream = self._want_user_stream

                if subscribe:
                    self._want_user_stream = True
                else:
                    self._want_user_stream = False
                    self._user_unsubscribe_inflight = True

                try:
                    await self._subscribe_user_only(subscribe, user_subscriptions)
                except Exception:
                    self._want_user_stream = prev_want_user_stream
                    raise
                finally:
                    if not subscribe:
                        self._user_unsubscribe_inflight = False

            for param in subscriptions:
                if subscribe:
                    self._subscribed.add(param)
                else:
                    self._subscribed.discard(param)

    async def _resubscribe(self) -> None:
        """Replay every market subscription on the right data stream(s).

        Used by callers that need to resubscribe the whole client
        (e.g. tests).  Per-stream reconnect uses the
        :meth:`_resubscribe_path` callback wired by :meth:`_get_data_stream`,
        so production resubscribes only replay the subscriptions whose
        router result matches the reconnecting stream's path.
        """
        async with self._get_subscription_lock():
            market_subscriptions, _ = self._split_subscriptions(self._subscribed)
            if len(market_subscriptions) == 0:
                return
            try:
                await self._subscribe_only(True, market_subscriptions)
            except Exception as e:
                self._logger.error(format_msg(
                    'data stream resubscribe failed after reconnect: %s',
                    repr_exception(e)))
                error = StreamError(
                    stream=StreamName.DATA,
                    phase=StreamErrorPhase.RESUBSCRIBE,
                    exception=e,
                    recovering=True
                )
                for stream in list(self._data_streams.values()):
                    self._schedule_recycle(
                        (StreamName.DATA, StreamErrorPhase.RESUBSCRIBE),
                        stream,
                    )
                if self._handler_ctx is not None:
                    await self._handler_ctx.dispatch_stream_error(error)

    async def _resubscribe_path(self, path: str) -> None:
        """Resubscribe only the streams whose router result equals ``path``.

        Wired as the ``on_connected`` callback for each per-path data stream
        so reconnecting one path does not re-send subscriptions belonging to
        the other path.
        """
        async with self._get_subscription_lock():
            market_subscriptions, _ = self._split_subscriptions(self._subscribed)
            if len(market_subscriptions) == 0:
                return

            # Filter the recorded subscriptions to those whose generated stream
            # name would route to ``path``.  Recompute the names here (rather than
            # caching) so the routing remains a pure function of the live router.
            raw_params = await self._get_handler_ctx().subscribe_params(
                True, market_subscriptions
            )
            names: Tuple[str, ...] = cast(Tuple[str, ...], raw_params)
            path_names = [n for n in names if self._data_stream_router(n) == path]
            if not path_names:
                return

            try:
                stream = self._get_data_stream(path)
                await stream.send({
                    'method': 'SUBSCRIBE',
                    'params': tuple(path_names),
                })
            except Exception as e:
                self._logger.error(format_msg(
                    'data stream resubscribe failed after reconnect: %s',
                    repr_exception(e)))
                error = StreamError(
                    stream=StreamName.DATA,
                    phase=StreamErrorPhase.RESUBSCRIBE,
                    exception=e,
                    recovering=True
                )
                stream_to_recycle = self._data_streams.get(path)
                if stream_to_recycle is not None:
                    self._schedule_recycle(
                        (StreamName.DATA, StreamErrorPhase.RESUBSCRIBE, path),
                        stream_to_recycle,
                    )
                if self._handler_ctx is not None:
                    await self._handler_ctx.dispatch_stream_error(error)

    async def _resubscribe_user(self) -> None:
        async with self._get_subscription_lock():
            _, user_subscriptions = self._split_subscriptions(self._subscribed)
            if len(user_subscriptions) == 0:
                return
            try:
                await self._subscribe_user_only(True, user_subscriptions)
            except Exception as e:
                self._logger.error(format_msg(
                    'user stream resubscribe failed after reconnect: %s',
                    repr_exception(e)))
                error = StreamError(
                    stream=StreamName.USER,
                    phase=StreamErrorPhase.RESUBSCRIBE,
                    exception=e,
                    recovering=True
                )
                if self._user_stream is not None:
                    self._schedule_recycle(
                        (StreamName.USER, StreamErrorPhase.RESUBSCRIBE),
                        self._user_stream,
                    )
                if self._handler_ctx is not None:
                    await self._handler_ctx.dispatch_stream_error(error)

    async def _recover_user_stream_if_needed(self) -> bool:
        async with self._get_subscription_lock():
            if (
                not self._want_user_stream
                or self._user_unsubscribe_inflight
                or (SubType.USER,) not in self._subscribed
            ):
                return False

            await self._subscribe_user_only(True, ((SubType.USER,),))
            self._logger.warning(
                'Recovered user stream subscription after eventStreamTerminated.'
            )
            return True

    async def subscribe(self, *args):
        """Subscribe to one or more market or user-data streams.

        Supports several calling conventions (overloads):

        - ``subscribe(SubType.TRADE, 'BTCUSDT')`` — single subtype + symbol.
        - ``subscribe([SubType.TRADE, SubType.TICKER], ['BTCUSDT', 'BNBUSDT'])``
          — lists of subtypes and symbols; the Cartesian product is subscribed.
        - ``subscribe((SubType.KLINE, 'BTCUSDT', TimeFrame.D1), ...)``
          — tuple pairs/triples for subtypes that require extra parameters
          (klines need a ``TimeFrame`` interval; order-book streams accept an
          optional update-speed interval in ms).
        - ``subscribe(SubType.ALL_MARKET_MINI_TICKERS)`` — subtypes that
          require no symbol parameter.
        - ``subscribe(SubType.USER)`` — authenticate and subscribe to the
          user-data stream; sends a signed ``userDataStream.subscribe``
          request over the WebSocket API connection.

        The data stream (and user stream when needed) is created lazily on
        the first call. After a reconnect, all subscriptions are replayed
        automatically by the ``on_connected`` hook.

        Args:
            *args: Subtype(s) and parameter(s) per the overloads above.
                See the project README for the full calling convention.

        Returns:
            None
        """
        return await self._subscribe(True, args)

    async def unsubscribe(self, *args):
        """Unsubscribe from one or more market or user-data streams.

        Accepts the same calling conventions as ``subscribe`` (same overloads
        for subtype, symbol, and extra parameters).

        For the user-data stream (``SubType.USER``), sends an unsigned
        ``userDataStream.unsubscribe`` request over the WebSocket API and
        clears ``_want_user_stream`` so the stream is not re-established
        after reconnects.

        Args:
            *args: Subtype(s) and parameter(s) — same shape as ``subscribe``.

        Returns:
            None
        """
        return await self._subscribe(False, args)

    async def list_subscriptions(self) -> List[str]:
        """Query the live data stream for the names of currently active streams.

        Sends a ``LIST_SUBSCRIPTIONS`` request over the data WebSocket and
        returns the result as a list of stream-name strings (e.g.
        ``['btcusdt@aggTrade', 'btcusdt@depth']``).

        This queries the Binance server's view of the connection, which may
        differ from the local ``_subscribed`` set during reconnect races.

        Returns:
            List[str]: stream names currently active on the data WebSocket
            connection, as reported by the Binance server.
        """
        return await self._get_data_stream(self._default_data_stream_path).send({
            'method': 'LIST_SUBSCRIPTIONS'
        })

    def handler(self, *handlers):
        """Sets the callback processing object to be used to handle websocket messages.

        Args:
            *handlers (HandlerBase):

        Returns:
            self
        """

        ctx = self._get_handler_ctx()

        for handler in handlers:
            if not ctx.set_handler(handler):
                raise InvalidHandlerException(handler)

        return self
