from typing import (
    List,
    Iterable,
    Set,
    Tuple,
    Optional
)
from logging import Logger

from aioretry import RetryPolicy

from binance.common.constants import (
    DEFAULT_STREAM_CLOSE_CODE,
    EVENT_SERVER_SHUTDOWN,
    SubType
)
from binance.common.exceptions import InvalidHandlerException
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
    """Mixin mixed into ``Client`` that owns the data and user WebSocket streams.

    ``SubscriptionManager`` is responsible for the full subscription lifecycle:
    creating and lazily starting the data stream (``/stream``) and the
    user-data WebSocket API stream, routing incoming messages to the active
    ``HandlerContext``, and resubscribing all previously registered streams
    after a reconnect.

    It exposes the public user-facing API: ``subscribe``, ``unsubscribe``,
    ``list_subscriptions``, ``handler``, ``start``, ``stop``, and ``close``.
    The underlying ``Stream`` objects and ``HandlerContext`` are private
    implementation details.

    State is partitioned into two WebSocket connections:
    - *data stream* (``_data_stream``): carries all market-data streams
      (klines, trades, tickers, etc.) via the combined ``/stream`` endpoint.
    - *user stream* (``_user_stream``): carries authenticated user-data events
      via the WebSocket API (``wss://ws-api.binance.com``).
    """

    _data_stream: Optional[Stream]
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

    def _get_user_stream(self) -> Stream:
        if self._user_stream is None:
            self._user_stream = Stream(
                self._ws_api_host,
                on_message=self._receive,
                on_connected=self._resubscribe_user,
                retry_policy=self._stream_retry_policy,
                timeout=self._stream_timeout,
                logger=self._logger,
                rate_limiter=self._rate_limiter,
                connection_id='user'
            ).connect()

        return self._user_stream

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

        stream = self._get_user_stream()

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
