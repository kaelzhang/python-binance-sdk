"""REST transport: session management, request building (sign == wire), and
response / rate-limit-header handling.

This is the market-agnostic REST machinery. It is mixed into
:class:`~binance.core.client_base.BaseClient`; market-specific endpoint methods
are installed separately. The generic ``get``/``post``/``put``/``delete``
escape hatch issues signed, rate-limit-accounted requests to any absolute URL.
"""

import time
from operator import itemgetter
from urllib.parse import quote

import yarl

from typing import (
    Awaitable,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from aiohttp import (
    ClientSession,
    ClientResponse,
    ClientTimeout
)

from binance.core.auth import Credentials
from binance.core.common.exceptions import (
    APIKeyNotDefinedException,
    APISecretNotDefinedException,
    StatusException,
    InvalidResponseException,
    RateLimitException,
    IPBannedException
)

from binance.core.common.constants import (
    HEADER_API_KEY,
    SecurityType,
    RequestMethod,
    HEADER_USED_WEIGHT_PREFIX,
    HEADER_ORDER_COUNT_PREFIX,
    HTTP_TOO_MANY_REQUESTS,
    HTTP_IP_BANNED,
    ERROR_CODE_INVALID_TIMESTAMP
)

from binance.core.rate_limit import RateLimiter, parse_retry_after

from binance.core.common.types import APIResponse

# pylint: disable=no-member

_CONTENT_TYPE_FORM = 'application/x-www-form-urlencoded'


def sort_params(data: dict) -> List[Tuple[str, str]]:
    """
    Convert params to list with signature as last element
    """
    has_signature = False
    params = []

    for key, value in data.items():
        if key == 'signature':
            has_signature = True
        else:
            params.append((key, _param_str(value)))

    # sort parameters by key
    params.sort(key=itemgetter(0))

    if has_signature:
        params.append(('signature', data['signature']))

    return params


def encode_params(data: dict) -> str:
    """Build an URL-encoded query string sorted by parameter name.

    Uses ``quote(safe='')`` so every special character (``/``, ``:``, space,
    ``+``, non-ASCII …) is percent-encoded.  This is the canonical wire
    encoding for Binance REST requests and the exact string that
    :meth:`RestTransport._generate_signature` signs.  The same pre-built string
    is sent verbatim -- aiohttp is never allowed to re-encode it -- so the
    signed bytes are always identical to the bytes on the wire (F-35).
    """
    return '&'.join(
        f'{quote(key, safe="")}={quote(str(value), safe="")}'
        for key, value in sort_params(data)
    )


def _reject_float_params(data: Union[dict, list, tuple]) -> None:
    """Raise ``ValueError`` if any value (recursively) in *data* is a ``float`` (F-07).

    Floats must be rejected at the API boundary because Python's ``str(float)``
    can produce scientific notation (e.g. ``'1e-08'``) or imprecise decimal
    representations that silently corrupt price/quantity fields.  Pass a string
    (e.g. ``price='0.00000001'``) or an int instead.  Nested ``dict``/``list``/
    ``tuple`` values are checked recursively so a float buried inside a nested
    param cannot slip through.
    """
    items = data.items() if isinstance(data, dict) else enumerate(data)
    for key, value in items:
        if isinstance(value, float):
            raise ValueError(
                f"param {key!r} is a float ({value!r}); "
                "pass a string for prices/quantities to avoid precision loss "
                "or scientific notation"
            )
        if isinstance(value, (dict, list, tuple)):
            _reject_float_params(value)


def _param_str(value) -> str:
    """Serialize a param value for the wire/signature.

    Booleans must become JSON-style lowercase ``true``/``false`` (Binance does
    not accept Python's ``True``/``False``); everything else uses ``str``.
    ``bool`` is checked before anything else because it is an ``int`` subclass.
    """
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)


KEY_REQUEST_PARAMS = 'request_params'
KEY_FORCE_PARAMS = 'force_params'

_BASE_HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'binance-sdk',
}


class RestTransport:
    """Market-agnostic REST machinery: auth, request building, and rate-limit accounting.

    Mixed into :class:`~binance.core.client_base.BaseClient`. Holds the shared
    :class:`~aiohttp.ClientSession`, builds requests so the signed bytes equal
    the wire bytes (F-35), and reconciles rate-limit response headers.
    """

    _credentials: Credentials
    _request_params: Optional[dict]
    _used_weight: Dict[str, int]
    _order_count: Dict[str, int]
    _rate_limiter: RateLimiter
    _rest_session: Optional[ClientSession]
    _request_timeout: float
    _time_offset: int
    _time_synced: bool
    # Provided by BaseClient; re-armed by the signed-request flow below.
    sync_time: Callable[[], Awaitable]

    def _get_session(self) -> ClientSession:
        """Return the shared REST :class:`~aiohttp.ClientSession`, creating it lazily.

        A single session is reused across all REST calls (F-12 / F-42).  The
        API-key header is set **per request** (not per session) via the
        ``headers`` kwarg in :meth:`_request`, so the session itself carries no
        credentials and can safely be shared regardless of security type.
        """
        if self._rest_session is None or self._rest_session.closed:
            self._rest_session = ClientSession(
                headers=_BASE_HEADERS,
                timeout=ClientTimeout(total=self._request_timeout),
            )
        return self._rest_session

    async def _close_rest_session(self) -> None:
        """Close the shared REST session if it was ever opened."""
        if self._rest_session is not None and not self._rest_session.closed:
            await self._rest_session.close()
        self._rest_session = None

    def _build_rest_request(
        self,
        method: RequestMethod,
        uri: str,
        need_signed: bool,
        api_key: Optional[str],
        **data
    ) -> Tuple[str, Dict[str, Any]]:
        """Build the final (url, kwargs) pair for an aiohttp request (F-35).

        ONE canonical percent-encoded string is built for the entire param set
        (including ``timestamp`` and ``signature`` when ``need_signed`` is
        True).  That same string is BOTH what is signed and what goes on the
        wire -- aiohttp never re-encodes it:

        - GET / ``force_params``: the encoded string is appended to ``uri``
          (``?<encoded>`` or ``&<encoded>``) and the URL is handed to aiohttp
          as a pre-encoded :class:`~yarl.URL` (``encoded=True``) so yarl does
          not touch it.
        - POST/PUT/DELETE body: the encoded string is passed as ``data=<str>``
          with ``Content-Type: application/x-www-form-urlencoded``; aiohttp
          forwards it verbatim.

        The API-key header is attached per-request (not per-session) so one
        shared :class:`~aiohttp.ClientSession` can serve any security type.
        """
        # Strip internal meta-keys before building the param string.
        extra_kwargs: Dict[str, Any] = {}
        if self._request_params is not None:
            extra_kwargs.update(self._request_params)
        if KEY_REQUEST_PARAMS in data:
            extra_kwargs.update(data.pop(KEY_REQUEST_PARAMS))

        force_params = bool(data.pop(KEY_FORCE_PARAMS, False))

        # F-07: reject float values
        _reject_float_params(data)

        # Add timestamp + signature AFTER float-rejection so the internally
        # added int timestamp does not trigger the guard.
        if need_signed:
            data['timestamp'] = int(time.time() * 1000) + self._time_offset
            data['signature'] = self._generate_signature(data)

        # Build ONE canonical encoded string used for BOTH signing and sending.
        use_query = force_params or method == RequestMethod.GET

        req_kwargs: Dict[str, Any] = {**extra_kwargs}

        # Attach the API-key as a per-request header (not a session header).
        if api_key is not None:
            req_kwargs['headers'] = {HEADER_API_KEY: api_key}

        if data:
            encoded = encode_params(data)
            if use_query:
                separator = '&' if '?' in uri else '?'
                url = yarl.URL(f'{uri}{separator}{encoded}', encoded=True)
                req_kwargs['url'] = url
            else:
                req_kwargs['data'] = encoded
                hdr = req_kwargs.setdefault('headers', {})
                hdr['Content-Type'] = _CONTENT_TYPE_FORM

        return uri, req_kwargs

    def _generate_signature(
        self,
        data: dict
    ) -> str:
        """Sign REST params: percent-encoded sorted ``key=value&...`` payload."""
        return self._credentials.sign(encode_params(data))

    def _capture_rate_limit_headers(self, response) -> None:
        for key, value in response.headers.items():
            lower = key.lower()
            if lower.startswith(HEADER_USED_WEIGHT_PREFIX):
                interval = lower[len(HEADER_USED_WEIGHT_PREFIX):]
                try:
                    self._used_weight[interval] = int(value)
                except (TypeError, ValueError):
                    pass
            elif lower.startswith(HEADER_ORDER_COUNT_PREFIX):
                interval = lower[len(HEADER_ORDER_COUNT_PREFIX):]
                try:
                    self._order_count[interval] = int(value)
                except (TypeError, ValueError):
                    pass

    @property
    def used_weight(self) -> Dict[str, int]:
        """Latest X-MBX-USED-WEIGHT-* values keyed by interval, e.g. {'1m': 12}."""
        return dict(self._used_weight)

    @property
    def order_count(self) -> Dict[str, int]:
        """Latest X-MBX-ORDER-COUNT-* values keyed by interval, e.g. {'10s': 3}."""
        return dict(self._order_count)

    async def _handle_response(
        self,
        response: ClientResponse
    ) -> APIResponse:
        self._capture_rate_limit_headers(response)
        self._rate_limiter.sync_from_headers(self._used_weight, self._order_count)

        status = response.status
        if status == HTTP_TOO_MANY_REQUESTS:
            self._rate_limiter.note_retry_after(
                parse_retry_after(response), response.status)
            raise RateLimitException(
                response, await response.text(), parse_retry_after(response))
        if status == HTTP_IP_BANNED:
            self._rate_limiter.note_retry_after(
                parse_retry_after(response), response.status)
            raise IPBannedException(
                response, await response.text(), parse_retry_after(response))
        if not str(status).startswith('2'):
            exc = StatusException(response, await response.text())
            if exc.code == ERROR_CODE_INVALID_TIMESTAMP:
                self._time_synced = False
            raise exc
        try:
            data = await response.json()
        except ValueError:
            raise InvalidResponseException(response, await response.text())

        if isinstance(data, dict) and 'rateLimits' in data:
            self._rate_limiter.configure_from_exchange_info(data['rateLimits'])

        return data

    async def _request(
        self,
        method: RequestMethod,
        uri: str,
        security_type: SecurityType = SecurityType.NONE,
        weight: int = 1,
        is_order: bool = False,
        **kwargs
    ) -> APIResponse:
        """Issue a generic REST request (the escape hatch for SAPI / unwrapped endpoints).

        Credentials, signing, rate-limit accounting, and error handling all
        apply as for any named endpoint.  Uses the shared
        :class:`~aiohttp.ClientSession` (lazily created; closed by
        :meth:`close`).  The API-key header is set per-request so the session
        is credential-neutral.
        """
        need_api_key, need_signed = security_type.value

        if need_api_key:
            if self._credentials.api_key is None:
                raise APIKeyNotDefinedException(uri)
            api_key = self._credentials.api_key
        else:
            api_key = None

        if need_signed and not self._credentials.has_signing():
            raise APISecretNotDefinedException(uri)

        if need_signed and not self._time_synced:
            await self.sync_time()

        await self._rate_limiter.acquire_rest(weight=weight, is_order=is_order)

        # Build the final URL + aiohttp kwargs (F-35: sign == wire).
        final_uri, req_kwargs = self._build_rest_request(
            method, uri, need_signed, api_key, **kwargs
        )

        # Use 'url' override when _build_rest_request encoded the query into the
        # URL object, otherwise use the raw string uri.
        request_url = req_kwargs.pop('url', final_uri)

        session = self._get_session()
        async with getattr(session, method.value)(
            request_url, **req_kwargs
        ) as response:
            return await self._handle_response(response)

    def get(self, uri, **kwargs) -> Awaitable[APIResponse]:
        """Sends a GET request.

        For details, see `client.post(uri, **kwargs)`
        """
        return self._request(RequestMethod.GET, uri, **kwargs)

    def post(self, uri, **kwargs) -> Awaitable[APIResponse]:
        """Sends a POST request.

        Args:
            uri (str): The absolute url to be requested.
            security_type (:obj:`SecurityType`, optional): The security type of the API of uri. Defaults to `SecurityType.NONE` which means the endpoint can be accessed freely.
            requests_params (:obj:`dict`, optional): Other params passed into `aiohttp::ClientSession::post()`.
            force_params (:obj:`bool`, optional): `True` to make ``**kwargs`` as querystring after the ``uri``. Defaults to `False`
            **kwargs: Arbitrary keyword arguments. For POST/PUT/DELETE requests, `kwargs` will be the request body if ``force_params`` is not `True` otherwise the querystring of the request url. For GET requests, `kwargs` will always be converted to querystring of the url.

        Returns:
            object: The server response JSON.

        Raises:
            StatusException: If the response status is not `2xx`.
            InvalidResponseException: If the response is not a valid JSON.
            APIKeyNotDefinedException: If the API endpoint requires a valid api key, but the api key is not defined for the client.
            APISecretNotDefinedException: If the API endpoint requires a valid signature, but the api secret is not defined for the client.
        """
        return self._request(RequestMethod.POST, uri, **kwargs)

    def put(self, uri, **kwargs) -> Awaitable[APIResponse]:
        """Sends a PUT request.

        For details, see `client.post(uri, **kwargs)`
        """
        return self._request(RequestMethod.PUT, uri, **kwargs)

    def delete(self, uri, **kwargs) -> Awaitable[APIResponse]:
        """Sends a DELETE request.

        For details, see `client.post(uri, **kwargs)`
        """
        return self._request(RequestMethod.DELETE, uri, **kwargs)
