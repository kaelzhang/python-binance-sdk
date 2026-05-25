"""Futures user-data stream lifecycle mixin.

``FuturesUserStreamMixin`` overrides the Spot (``SubscriptionManager``) user-stream
lifecycle methods to implement the USDⓈ-M and COIN-M Futures **listenKey flow**,
which is fundamentally different from the Spot ``userDataStream.subscribe`` flow.

Verified mechanism (USDⓈ-M Futures WS-API docs, 2026-05-25):
  - ``userDataStream.start``  (security: USER_STREAM = apiKey only, weight 1)
    → returns ``{"listenKey": "..."}`` over the ws-fapi connection.
  - User-data events arrive on a SEPARATE dedicated stream connection:
      ``MARKET.stream_host + '/ws/' + listenKey``
      (e.g. ``wss://fstream.binance.com/ws/<listenKey>``),
    NOT on the ws-fapi connection.
  - ``userDataStream.ping``   (security: USER_STREAM, weight 1) — keep the
    listen key alive; send every ~50 minutes (key expires after 60 minutes).
  - ``userDataStream.stop``   (security: USER_STREAM, weight 1) — invalidate
    the listen key on close.

On ``listenKeyExpired`` (dispatched by ``FuturesUserProcessor`` to
``FuturesListenKeyExpiredHandlerBase``), the mixin recreates the listen key and
reconnects the dedicated user stream.

The mixin is intentionally market-agnostic: it reads ``self._stream_host`` (and
all other BaseClient attributes) at runtime, so both UM and CM clients get the
correct ``stream_host`` via the MRO without any hard-coded host.

Ref:
https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams
"""

import asyncio
from logging import Logger
from typing import Any, Awaitable, Callable, Iterable, Optional

from aioretry import RetryPolicy

from binance.core.common.constants import (
    DEFAULT_STREAM_CLOSE_CODE,
    SecurityType,
)
from binance.core.common.utils import format_msg, repr_exception
from binance.core.common.types import StreamError, StreamName, StreamErrorPhase, Timeout
from binance.core.handlers.context import HandlerContext
from binance.core.rate_limit import RateLimiter
from binance.core.transport.stream import Stream

# listenKey keepalive interval (seconds) — ping every 50 min; key expires after 60 min.
def _extract_event_type(msg):
    """Return the Binance event type ('e') from a message in any documented shape."""
    if not isinstance(msg, dict):
        return None
    for container_key in ('data', 'event'):
        container = msg.get(container_key)
        if isinstance(container, dict) and 'e' in container:
            return container['e']
    return msg.get('e')


_KEEPALIVE_INTERVAL = 50 * 60

# WS-API method names (USDⓈ-M / COIN-M Futures, confirmed from docs 2026-05-25).
# Security: USER_STREAM (apiKey only, no signature), weight 1.
_METHOD_START = 'userDataStream.start'
_METHOD_PING = 'userDataStream.ping'
_METHOD_STOP = 'userDataStream.stop'
_USER_STREAM_SECURITY = SecurityType.USER_STREAM
_USER_STREAM_WEIGHT = 1

# Connection ID used by the dedicated futures user-data fstream connection.
# Kept distinct from 'user' (the ws-api/SubscriptionManager connection) so
# rate-limit registration and unregistration do not collide.
_FUTURES_USER_CONN_ID = 'futures_user'

# The 'e' field value for the listenKeyExpired event
_EVENT_LISTEN_KEY_EXPIRED = 'listenKeyExpired'


class FuturesUserStreamMixin:
    """Mixin that replaces the Spot user-stream subscription lifecycle for Futures clients.

    Mix this in BEFORE ``BaseClient`` in the MRO (i.e. listed first) so its
    overrides take priority over ``SubscriptionManager``'s methods.

    The futures user-data stream uses the **listenKey flow** instead of Spot's
    ``userDataStream.subscribe.signature``.  A dedicated :class:`Stream`
    connection (``_futures_user_stream``) carries the user-data events.  The
    ws-fapi connection (``_user_stream`` in ``SubscriptionManager``) is still
    used for ``userDataStream.start/ping/stop`` WS-API calls.
    """

    # Kept as a separate attribute so it does not collide with the
    # SubscriptionManager's ``_user_stream`` (the shared ws-api connection).
    _futures_user_stream: Optional[Stream]
    _futures_keepalive_task: Optional[asyncio.Task]
    _futures_listen_key: Optional[str]

    # Cross-mixin attributes declared on SubscriptionManager / BaseClient;
    # listed here so type checkers can verify access within the mixin.
    _want_user_stream: bool
    _stream_host: str
    _stream_retry_policy: RetryPolicy
    _stream_timeout: Timeout
    _rate_limiter: RateLimiter
    _handler_ctx: Optional[HandlerContext]
    _logger: Logger
    _get_handler_ctx: Callable[[], HandlerContext]
    _ws_api_request: Callable[..., Awaitable[Any]]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._futures_user_stream = None
        self._futures_keepalive_task = None
        self._futures_listen_key = None

    # ------------------------------------------------------------------
    # Override: subscribe path
    # ------------------------------------------------------------------

    async def _subscribe_user_only(
        self,
        subscribe: bool,
        subscriptions: Iterable[tuple]
    ) -> None:
        """Override Spot's subscribe path with the futures listenKey flow.

        On subscribe: call ``userDataStream.start`` to obtain a listenKey, open
        a dedicated fstream connection, start the keepalive task, and track the
        subscribed state in the processor.

        On unsubscribe: cancel the keepalive, call ``userDataStream.stop``, close
        the dedicated stream, and clear processor state.
        """
        # Resolve the subscription state via the processor (tracks _subscribed).
        await self._get_handler_ctx().subscribe_params(subscribe, subscriptions)

        if subscribe:
            await self._futures_user_stream_start()
        else:
            await self._futures_user_stream_stop()

    # ------------------------------------------------------------------
    # Override: intercept listenKeyExpired in _receive
    # ------------------------------------------------------------------

    async def _receive(self, msg) -> None:
        """Override: intercept ``listenKeyExpired`` for SDK-side recovery, then dispatch normally."""
        # Extract the event type from the message (same logic as subscription.py
        # _extract_event_type, duplicated here to avoid importing a private symbol).
        event_type = _extract_event_type(msg)
        if event_type == _EVENT_LISTEN_KEY_EXPIRED and self._want_user_stream:
            asyncio.get_running_loop().create_task(
                self._on_futures_listen_key_expired()
            )
        await super()._receive(msg)  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Override: on_connected — do NOT replay subscribe on ws-fapi reconnect
    # ------------------------------------------------------------------

    async def _resubscribe_user(self) -> None:
        """Override: futures user-data events are on the dedicated fstream.

        The dedicated fstream ``Stream`` reconnects itself via aioretry; there is
        nothing to replay over the ws-fapi connection on ws-fapi reconnect.
        """
        # No-op: the dedicated fstream self-reconnects; reconnecting the ws-fapi
        # connection does not require replaying the user subscription.
        return

    async def _recover_user_stream_if_needed(self) -> bool:
        """Override: no recovery needed; the dedicated fstream handles reconnection itself."""
        # The dedicated futures user-data Stream uses aioretry to reconnect.
        # EVENT_STREAM_TERMINATED on the ws-fapi connection does not affect it.
        return False

    # ------------------------------------------------------------------
    # Override: close
    # ------------------------------------------------------------------

    async def close(self, code: int = DEFAULT_STREAM_CLOSE_CODE) -> None:
        """Close the futures user stream (keepalive + dedicated fstream), then call super().close()."""
        await self._futures_cleanup(code)
        await super().close(code)  # type: ignore[misc]

    # ------------------------------------------------------------------
    # listenKeyExpired — called by the FuturesUserProcessor dispatch path
    # ------------------------------------------------------------------

    async def _on_futures_listen_key_expired(self) -> None:
        """Handle ``listenKeyExpired``: re-obtain a listenKey and reconnect the dedicated stream.

        Called from ``_receive`` when a ``listenKeyExpired`` event arrives (the
        ``FuturesListenKeyExpiredHandlerBase`` handlers are still called normally
        by the dispatcher; this is a parallel SDK-side recovery action).
        """
        if not self._want_user_stream:
            return

        self._logger.warning(format_msg('futures listenKey expired; restarting user stream'))

        # Cancel old keepalive — a new one will be started after re-obtaining the key.
        self._cancel_futures_keepalive()

        # Close the old dedicated stream (if any).
        old_stream = self._futures_user_stream
        self._futures_user_stream = None
        self._futures_listen_key = None
        if old_stream is not None:
            try:
                await old_stream.close(code=DEFAULT_STREAM_CLOSE_CODE)
            except Exception as e:
                self._logger.error(format_msg(
                    'futures user stream close on listenKeyExpired failed: %s',
                    repr_exception(e)))

        # Re-start.
        try:
            await self._futures_user_stream_start()
        except Exception as e:
            self._logger.error(format_msg(
                'futures user stream restart after listenKeyExpired failed: %s',
                repr_exception(e)))
            error = StreamError(
                stream=StreamName.USER,
                phase=StreamErrorPhase.RESUBSCRIBE,
                exception=e,
                recovering=True
            )
            if self._handler_ctx is not None:
                await self._handler_ctx.dispatch_stream_error(error)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _futures_user_stream_start(self) -> None:
        """Obtain a listenKey, open the dedicated fstream, start the keepalive."""
        result = await self._ws_api_request(
            _METHOD_START,
            params=None,
            security=_USER_STREAM_SECURITY,
            weight=_USER_STREAM_WEIGHT,
        )
        listen_key: str = result['listenKey']
        self._futures_listen_key = listen_key

        uri = self._stream_host + '/ws/' + listen_key

        self._futures_user_stream = Stream(
            uri,
            on_message=self._receive,
            retry_policy=self._stream_retry_policy,
            timeout=self._stream_timeout,
            logger=self._logger,
            rate_limiter=self._rate_limiter,
            connection_id=_FUTURES_USER_CONN_ID,
        ).connect()

        self._futures_keepalive_task = asyncio.get_running_loop().create_task(
            self._futures_keepalive_loop()
        )

    async def _futures_user_stream_stop(self) -> None:
        """Cancel keepalive, call ``userDataStream.stop``, close dedicated stream."""
        await self._futures_cleanup()

    async def _futures_cleanup(self, code: int = DEFAULT_STREAM_CLOSE_CODE) -> None:
        """Cancel keepalive, call ``userDataStream.stop``, close the dedicated stream."""
        self._cancel_futures_keepalive()

        # Call userDataStream.stop (best-effort: the key may already be expired)
        if self._futures_listen_key is not None:
            listen_key = self._futures_listen_key
            self._futures_listen_key = None
            try:
                await self._ws_api_request(
                    _METHOD_STOP,
                    params={'listenKey': listen_key},
                    security=_USER_STREAM_SECURITY,
                    weight=_USER_STREAM_WEIGHT,
                )
            except Exception as e:
                self._logger.error(format_msg(
                    'userDataStream.stop failed: %s', repr_exception(e)))

        old_stream = self._futures_user_stream
        self._futures_user_stream = None
        if old_stream is not None:
            try:
                await old_stream.close(code)
            except Exception as e:
                self._logger.error(format_msg(
                    'futures user stream close failed: %s', repr_exception(e)))
            self._rate_limiter.unregister_connection(_FUTURES_USER_CONN_ID)

    def _cancel_futures_keepalive(self) -> None:
        task = self._futures_keepalive_task
        self._futures_keepalive_task = None
        if task is not None and not task.done():
            task.cancel()

    async def _futures_keepalive_loop(self) -> None:
        """Periodically call ``userDataStream.ping`` to keep the listenKey alive."""
        try:
            while True:
                await asyncio.sleep(_KEEPALIVE_INTERVAL)
                listen_key = self._futures_listen_key
                if listen_key is None:
                    return
                try:
                    await self._ws_api_request(
                        _METHOD_PING,
                        params={'listenKey': listen_key},
                        security=_USER_STREAM_SECURITY,
                        weight=_USER_STREAM_WEIGHT,
                    )
                    self._logger.debug(format_msg('futures listenKey keepalive sent'))
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._logger.error(format_msg(
                        'futures listenKey keepalive failed: %s', repr_exception(e)))
        except asyncio.CancelledError:
            pass
