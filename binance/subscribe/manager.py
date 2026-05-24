import time
from typing import (
    Any,
    List,
    Iterable,
    Set,
    Tuple,
    Optional
)
from logging import Logger

from aioretry import RetryPolicy

# Ed25519 is the only key type that supports WS-API `session.logon`.
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from binance.common.constants import (
    DEFAULT_STREAM_CLOSE_CODE,
    EVENT_SERVER_SHUTDOWN,
    SecurityType,
    SubType,
    STREAM_KEY_RATE_LIMITS,
    WS_API_METHOD_SESSION_LOGON,
    ERROR_CODE_UNAUTHORIZED
)
from binance.common.exceptions import (
    APIKeyNotDefinedException,
    APISecretNotDefinedException,
    InvalidHandlerException,
    StreamSubscribeException
)
from binance.common.types import Timeout
from binance.common.utils import (
    format_msg,
    repr_exception
)
from binance.rate_limit import RateLimiter

from .stream import Stream
from .handler_context import HandlerContext

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


class SubscriptionManager:
    """Internal mixin merged into ``Client`` that manages data and user WebSocket stream lifecycles."""

    _data_stream: Optional[Stream]
    # The shared WS-API request/response connection (wss://ws-api...). It
    # carries BOTH the user-data stream subscription and every `_ws_api_request`
    # (former REST) call -- one connection, lazily opened.
    _user_stream: Optional[Stream]
    _subscribed: Set[tuple]
    _stream_names: Set[str]
    _stream_host: str
    _ws_api_host: str
    _stream_retry_policy: RetryPolicy
    _stream_timeout: Timeout
    _rate_limiter: RateLimiter
    _logger: Logger
    _want_user_stream: bool
    _user_unsubscribe_inflight: bool
    _user_recovering: bool
    _ws_api_authenticated: bool
    # Credentials / signing live on ClientBase; declared here for the WS-API
    # request path that runs on the merged Client via these mixin attributes.
    _api_key: Optional[str]
    _api_secret: Optional[str]
    _private_key: Optional[object]
    _time_offset: int

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
        self._user_recovering = False
        self._ws_api_authenticated = False

        if self._data_stream:
            await self._data_stream.close(code)
            self._data_stream = None
            self._rate_limiter.unregister_connection('data')

        if self._user_stream:
            await self._user_stream.close(code)
            self._user_stream = None
            self._rate_limiter.unregister_connection('user')

        self._handler_ctx = None

    async def _receive(self, msg) -> None:
        if not self._receiving:
            return

        event_type = _extract_event_type(msg)

        if event_type == EVENT_SERVER_SHUTDOWN:
            self._logger.warning(
                'serverShutdown received; recycling data stream proactively')
            if self._data_stream is not None:
                await self._data_stream.recycle()
            return

        if event_type == 'eventStreamTerminated':
            try:
                await self._recover_user_stream_if_needed()
            except Exception as e:
                self._logger.error(format_msg(
                    'Failed to recover user stream after eventStreamTerminated: %s',
                    repr_exception(e)))

        await self._handler_ctx.receive(msg)

    def _get_handler_ctx(self) -> HandlerContext:
        if not self._handler_ctx:
            self._handler_ctx = HandlerContext(self)

        return self._handler_ctx

    def _get_data_stream(self) -> Stream:
        if self._data_stream is None:
            self._data_stream = Stream(
                self._stream_host + '/stream',
                on_message=self._receive,
                on_connected=self._resubscribe,
                retry_policy=self._stream_retry_policy,
                timeout=self._stream_timeout,
                logger=self._logger,
                rate_limiter=self._rate_limiter,
                connection_id='data'
            ).connect()

        return self._data_stream

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
            self._user_stream = Stream(
                self._ws_api_host,
                on_message=self._receive,
                on_connected=self._on_ws_api_connected,
                on_response=self._reconcile_ws_api_rate_limits,
                retry_policy=self._stream_retry_policy,
                timeout=self._stream_timeout,
                logger=self._logger,
                rate_limiter=self._rate_limiter,
                connection_id='user'
            ).connect()

        return self._user_stream

    def _reconcile_ws_api_rate_limits(self, msg) -> None:
        """``on_response`` hook: reconcile a WS-API response's ``rateLimits``.

        Called by the shared WS-API :class:`Stream` with the full id-correlated
        response message. The authoritative ``rateLimits`` array (present on
        every WS-API response) is folded into the shared rate-limit core,
        keeping the local weight/orders/raw pools exact.
        """
        if isinstance(msg, dict):
            self._rate_limiter.sync_from_ws_rate_limits(
                msg.get(STREAM_KEY_RATE_LIMITS))

    async def _on_ws_api_connected(self) -> None:
        """``on_connected`` hook for the shared WS-API connection.

        Runs on every (re)connect. The session.logon optimization is NOT
        persistent across reconnects, so the authenticated flag is reset first;
        an Ed25519 key then re-logs on. Finally the user-data stream
        subscription is replayed.
        """
        self._ws_api_authenticated = False
        await self._ws_api_session_logon_if_needed()
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
        if not isinstance(self._private_key, Ed25519PrivateKey):
            return

        params = self._ws_api_signature_params()
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
        - ``TRADE``/``USER_DATA`` (SIGNED): when the connection already holds an
          authenticated session (Ed25519 ``session.logon``), only a
          ``timestamp`` (+offset) is added -- ``apiKey``/``signature`` are
          omitted; otherwise the full raw-value signed payload
          (``apiKey``+``timestamp``+``signature``) is built.
        - ``USER_STREAM``: always the full signed payload.

        Raises the same credential guards as the REST path before any send.
        """
        need_api_key, need_signed = security.value

        if not need_api_key:
            # SecurityType.NONE -> public, no credentials.
            return params

        if self._api_key is None:
            raise APIKeyNotDefinedException(method)

        if need_signed:
            if self._api_secret is None and self._private_key is None:
                raise APISecretNotDefinedException(method)

            if self._ws_api_authenticated:
                # Session is logged on: omit apiKey + signature, keep timestamp.
                return {
                    **params,
                    'timestamp': int(time.time() * 1000) + self._time_offset
                }

            return self._ws_api_signature_params(**params)

        # USER_STREAM: api key + timestamp + signature (no session shortcut).
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
        params = await self._get_handler_ctx().subscribe_params(
            subscribe,
            subscriptions
        )

        if subscribe:
            projected = self._stream_names | set(params)
            self._rate_limiter.reserve_streams('data', len(projected))

        stream = self._get_data_stream()

        try:
            await stream.send({
                'method': 'SUBSCRIBE' if subscribe else 'UNSUBSCRIBE',
                'params': params
            })
        except Exception:
            if subscribe:
                # roll back the optimistic stream reservation; len(self._stream_names)
                # is the committed count and is always <= cap, so this never raises
                self._rate_limiter.reserve_streams('data', len(self._stream_names))
            raise

        if subscribe:
            self._stream_names.update(params)
        else:
            removed = self._stream_names & set(params)
            self._stream_names.difference_update(params)
            self._rate_limiter.release_streams('data', len(removed))

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

            req = {'method': method}

            if param:
                req['params'] = param

            await stream.send(req)

    # subscribe to the stream for symbols
    async def _subscribe(
        self,
        subscribe: bool,
        args: Tuple
    ):
        subscriptions = self._get_handler_ctx().overload_subscriptions(*args)
        market_subscriptions, user_subscriptions = self._split_subscriptions(
            subscriptions
        )

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
        market_subscriptions, _ = self._split_subscriptions(self._subscribed)
        if len(market_subscriptions) > 0:
            await self._subscribe_only(True, market_subscriptions)

    async def _resubscribe_user(self) -> None:
        _, user_subscriptions = self._split_subscriptions(self._subscribed)
        if len(user_subscriptions) > 0:
            await self._subscribe_user_only(True, user_subscriptions)

    async def _recover_user_stream_if_needed(self) -> bool:
        if (
            not self._want_user_stream
            or self._user_unsubscribe_inflight
            or self._user_recovering
            or (SubType.USER,) not in self._subscribed
        ):
            return False

        self._user_recovering = True

        try:
            await self._subscribe_user_only(True, ((SubType.USER,),))
            self._logger.warning(
                'Recovered user stream subscription after eventStreamTerminated.'
            )
            return True
        finally:
            self._user_recovering = False

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
        return await self._get_data_stream().send({
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
