"""USDⓈ-M Futures general WS-API endpoint stubs.

Shared-connection session management (``session.status`` / ``session.logout``)
on the WS-API per the futures general-info docs. ``get_session_status`` is a
pre-declared stub patched by ``define_getter`` at import time;
``session_logout`` is implemented inline because it also mutates client-local
session-auth state (mirrors Spot's ``WsApiGeneralGetters`` pattern).

UM has no public ``ping`` / ``serverTime`` / ``exchangeInfo`` WS-API methods
(those live on REST), so the only general WS-API surface on futures is
session management.

Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info
"""

from typing import (
    Awaitable,
    Callable,
)

from binance.core.common.constants import SecurityType


class UMGeneralGetters:
    """Session-management mixin for :class:`UMFuturesGetters`."""

    _ws_api_request: Callable[..., Awaitable]
    _ws_api_authenticated: bool

    # ----- session management ----------------------------------------------

    def get_session_status(self) -> Awaitable:
        """Reports which API key is currently authorizing the shared WS-API connection.

        Weight: 2.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info

        Returns:
            dict: Session status, including the authorizing API key (if any).
        """
        ...  # pragma: no cover

    async def session_logout(self):
        """Forgets the API key authenticated on the shared WS-API connection.

        Sends ``session.logout`` and clears the local session-auth flag so
        subsequent signed requests fall back to per-request signing (matching
        the now-unauthenticated connection). The connection stays open; a later
        reconnect re-runs ``session.logon`` automatically (Ed25519 only).

        Weight: 2.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info

        Returns:
            dict: an empty dict ``{}``.
        """
        result = await self._ws_api_request(
            'session.logout', security=SecurityType.NONE, weight=2
        )
        self._ws_api_authenticated = False
        return result
