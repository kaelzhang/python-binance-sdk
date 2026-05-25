"""WebSocket-API transport helpers: connection-URL time-unit option and the
raw-value signed-request payload builder.

The actual request/response-over-ws machinery (``_ws_api_request``) lives on
:class:`~binance.core.transport.subscription.SubscriptionManager`, which owns
the shared WS-API connection. This module holds the market-agnostic signing /
URL helpers mixed into :class:`~binance.core.client_base.BaseClient`.
"""

import time
from operator import itemgetter

from binance.core.auth import Credentials
from binance.core.common.exceptions import (
    APIKeyNotDefinedException,
    APISecretNotDefinedException,
)
from binance.core.common.constants import (
    WS_API_TIME_UNIT_QUERY,
    WS_API_TIME_UNIT_MICROSECOND,
    WS_API_TIME_UNIT_MILLISECOND,
)
from binance.core.transport.rest import _param_str


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


class WsApiTransport:
    """Market-agnostic WebSocket-API signing helpers.

    Mixed into :class:`~binance.core.client_base.BaseClient`. Builds the raw
    (non-percent-encoded) signature payload the WS-API spec requires.
    """

    _credentials: Credentials
    _time_offset: int

    def _ws_api_query(self, params: dict) -> str:
        """Build the WS-API signature payload: sorted RAW ``key=value&...``.

        Per the Binance WebSocket-API spec ("no percent encoding here!"), the
        signature is computed over the params (excluding ``signature``) sorted
        alphabetically by key and joined as ``key=value&key=value`` using the
        **raw** UTF-8 string value of each param -- NO URL/percent-encoding,
        unlike the REST path (:func:`~binance.core.transport.rest.encode_params`).
        The JSON ``params`` sent on the wire carry the same raw values, so what
        Binance reconstructs matches what was signed.
        """
        return '&'.join(
            f'{key}={value}'
            for key, value in sorted(
                (
                    (k, _param_str(v))
                    for k, v in params.items()
                    if k != 'signature'
                ),
                key=itemgetter(0)
            )
        )

    def _ws_api_signature_params(
        self,
        **params
    ) -> dict:
        """Build signed params for a WebSocket-API request (raw-value payload).

        Assembles the caller's ``params`` with ``apiKey`` and a
        ``timestamp`` (local clock + ``_time_offset``), signs the
        :meth:`_ws_api_query` raw sorted ``key=value&...`` payload via the bound
        :class:`Credentials`, and attaches the resulting ``signature``. The
        raw-value payload (NOT percent-encoded) is what the WS-API spec requires
        and what is sent on the wire, so the signature always reconciles.
        """
        if self._credentials.api_key is None:
            raise APIKeyNotDefinedException('userDataStream.subscribe.signature')

        if not self._credentials.has_signing():
            raise APISecretNotDefinedException('userDataStream.subscribe.signature')

        signed = {
            **params,
            'apiKey': self._credentials.api_key,
            'timestamp': int(time.time() * 1000) + self._time_offset
        }
        signed['signature'] = self._credentials.sign(self._ws_api_query(signed))

        return signed
