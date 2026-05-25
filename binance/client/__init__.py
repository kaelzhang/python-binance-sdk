from binance.apis import WsApiGetters
from binance.core.auth import Credentials
from binance.core.client_base import BaseClient
from binance.core.transport.ws_api import _apply_time_unit
from binance.processors import (
    PROCESSORS,
    ExceptionProcessor,
    StreamErrorProcessor,
)

__all__ = ['Client', '_apply_time_unit']


class Client(BaseClient, WsApiGetters):  # type: ignore[misc]  # diamond mixin: _ws_api_request is a Callable hint in WsApiGetters and an actual method in SubscriptionManager; compatible at runtime
    """Async Binance Spot REST + WebSocket client — the primary public entry point.

    Combines the shared :class:`~binance.core.client_base.BaseClient` (REST +
    WS-API transports + stream subscription lifecycle) with ``WsApiGetters``,
    the generated async methods for every Spot WebSocket-API endpoint --
    general (``get_server_time``, ``get_exchange_info``), market-data
    (``get_orderbook``, ``get_klines``, ``get_ticker``, ...), account
    (``get_account``, ``get_commission``, ...) and trading (``create_order``,
    ``cancel_order``, ``create_oco``, ...).

    Typical usage::

        from binance import Credentials

        client = Client(Credentials(api_key='KEY', api_secret='SECRET'))

        # WebSocket-API call — awaitable coroutine
        info = await client.get_exchange_info()

        # Subscribe to a trade stream and attach a handler
        client.handler(on_trade)
        await client.subscribe('btcusdt@trade')
    """

    # The Spot market's processor set, injected into the HandlerContext.
    PROCESSORS = PROCESSORS
    EXCEPTION_PROCESSOR = ExceptionProcessor
    STREAM_ERROR_PROCESSOR = StreamErrorProcessor

    def __init__(
        self,
        api_key=None,
        api_secret=None,
        private_key=None,
        private_key_pass=None,
        **kwargs
    ):
        """Binance API Client constructor.

        Accepts the legacy credential keyword arguments (``api_key`` /
        ``api_secret`` / ``private_key`` / ``private_key_pass``) and wraps them
        in a :class:`Credentials` instance forwarded to
        :class:`~binance.core.client_base.BaseClient`. All other keyword
        arguments (hosts, ``rate_limiter``, ``request_timeout``, ``time_unit``,
        ``recv_window`` …) are forwarded unchanged.
        """
        credentials = Credentials(
            api_key=api_key,
            api_secret=api_secret,
            private_key=private_key,
            private_key_pass=private_key_pass,
        )

        super().__init__(credentials, **kwargs)
