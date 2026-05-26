"""General WS-API endpoint stubs.

Connectivity (``ping``/``time``/``exchangeInfo``) and shared connection session
management (``session.status``/``session.subscriptions``/``session.logout``).
The first three are pre-declared stubs whose bodies are replaced by
``define_ws_getter``; ``session_logout`` is implemented inline because it also
mutates client-local session-auth state.
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.core.common.constants import SecurityType


class WsApiGeneralGetters:
    """Connectivity + session-management mixin for ``WsApiGetters``."""

    _ws_api_request: Callable[..., Awaitable]
    _ws_api_authenticated: bool

    # ----- general ---------------------------------------------------------

    def ping(self) -> Awaitable:
        """Tests connectivity to the WebSocket API.

        Returns:
            dict: An empty dict `{}`
        """
        ...  # pragma: no cover

    def get_server_time(self) -> Awaitable:
        """Tests connectivity to the WebSocket API and gets the current server time.

        Returns:
            dict: A dict contains only one key `serverTime`. For example::

                {"serverTime": 1499827319559}
        """
        ...  # pragma: no cover

    def get_exchange_info(self) -> Awaitable:
        """Gets Current exchange trading rules and symbol information.

        Returns:
            dict: A dict of the exchange info. For example::

                {
                    'timezone': 'UTC',
                    'serverTime': 1565246363776,
                    'rateLimits': [
                        {
                            # These are defined in the `ENUM definitions` section under `Rate Limiters (rateLimitType)`.
                            # All limits are optional
                        }
                    ],
                    'exchangeFilters': [
                        # These are the defined filters in the `Filters` section.
                        # All filters are optional.
                    ],
                    'symbols': [
                        {
                            'symbol': 'ETHBTC',
                            'status': 'TRADING',
                            'baseAsset': 'ETH',
                            'baseAssetPrecision': 8,
                            'quoteAsset': 'BTC',
                            'quotePrecision': 8,
                            'baseCommissionPrecision': 8,
                            'quoteCommissionPrecision': 8,
                            'orderTypes': [
                                'LIMIT',
                                'LIMIT_MAKER',
                                'MARKET',
                                'STOP_LOSS',
                                'STOP_LOSS_LIMIT',
                                'TAKE_PROFIT',
                                'TAKE_PROFIT_LIMIT'
                            ],
                            'icebergAllowed': True,
                            'ocoAllowed': True,
                            'quoteOrderQtyMarketAllowed': True,
                            'isSpotTradingAllowed': True,
                            'isMarginTradingAllowed': True,
                            'filters': [
                                # These are defined in the Filters section.
                                # All filters are optional
                            ]
                        }
                    ]
                }
        """
        ...  # pragma: no cover

    # ----- session management ----------------------------------------------

    def get_session_status(self) -> Awaitable:
        """Reports which API key is currently authorizing the shared WS-API connection.

        Weight: 2

        Returns:
            dict: Session status, including the authorizing API key (if any).
        """
        ...  # pragma: no cover

    def get_session_subscriptions(self) -> Awaitable:
        """Lists active user-data subscriptions on the shared WS-API connection.

        Weight: 2

        Returns:
            list: Active user-data subscription descriptors.
        """
        ...  # pragma: no cover

    async def session_logout(self):
        """Forgets the API key authenticated on the shared WS-API connection.

        Sends ``session.logout`` and clears the local session-auth flag so
        subsequent signed requests fall back to per-request signing (matching
        the now-unauthenticated connection). The connection stays open; a later
        reconnect re-runs ``session.logon`` automatically (Ed25519 only).

        Weight: 2

        Returns:
            dict: an empty dict ``{}``.
        """
        result = await self._ws_api_request(
            'session.logout', security=SecurityType.NONE, weight=2
        )
        self._ws_api_authenticated = False
        return result
