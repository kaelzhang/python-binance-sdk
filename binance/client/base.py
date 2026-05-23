import asyncio
import hashlib
import hmac
import time
from operator import itemgetter
from urllib.parse import quote

from typing import (
    List,
    Tuple,
    Dict,
    Awaitable,
    Optional,
    Any
)

from aiohttp import (
    ClientSession,
    ClientResponse
)

from binance.common.exceptions import (
    APIKeyNotDefinedException,
    APISecretNotDefinedException,
    StatusException,
    InvalidResponseException,
    RateLimitException,
    IPBannedException
)

from binance.common.constants import (
    HEADER_API_KEY,
    SecurityType,
    RequestMethod,
    HEADER_USED_WEIGHT_PREFIX,
    HEADER_ORDER_COUNT_PREFIX,
    HTTP_TOO_MANY_REQUESTS,
    HTTP_IP_BANNED
)

from binance.common.rate_limit import (
    parse_retry_after
)

from binance.rate_limit import RateLimiter

from binance.common.types import APIResponse

# pylint: disable=no-member


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
            params.append((key, str(value)))

    # sort parameters by key
    params.sort(key=itemgetter(0))

    if has_signature:
        params.append(('signature', data['signature']))

    return params


def encode_params(data: dict) -> str:
    """Build an URL-encoded query string sorted by parameter name."""
    return '&'.join(
        f'{quote(key, safe="")}={quote(value, safe="")}'
        for key, value in sort_params(data)
    )


KEY_REQUEST_PARAMS = 'request_params'
KEY_FORCE_PARAMS = 'force_params'


def get_headers(
    api_key: Optional[str]
) -> Dict[str, str]:
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'binance-sdk'
    }

    if api_key is not None:
        headers[HEADER_API_KEY] = api_key

    return headers


class ClientBase:
    _api_key: Optional[str]
    _api_secret: Optional[str]
    _request_params: Optional[dict]
    _used_weight: Dict[str, int]
    _order_count: Dict[str, int]
    _rate_limiter: RateLimiter

    def _init_api_session(
        self,
        api_key: Optional[str]
    ) -> ClientSession:
        session = ClientSession(
            loop=asyncio.get_running_loop(),
            headers=get_headers(api_key)
        )
        return session

    def _get_request_kwargs(
        self,
        method: RequestMethod,
        need_signed: bool,
        **data
    ) -> Dict[str, Any]:
        # Usually, `data` is the data param for aiohttp

        kwargs: Dict[str, Any] = dict(
            # set default requests timeout
            # TODO: no hard coding
            timeout=10
        )

        # add global requests params for aiohttp
        if self._request_params is not None:
            kwargs.update(self._request_params)

        # find any requests params passed and apply them
        if KEY_REQUEST_PARAMS in data:
            # merge requests params into kwargs
            kwargs.update(data[KEY_REQUEST_PARAMS])
            del data[KEY_REQUEST_PARAMS]

        force_params = False
        if KEY_FORCE_PARAMS in data:
            force_params = True
            del data[KEY_FORCE_PARAMS]

        if need_signed:
            # generate signature
            data['timestamp'] = int(time.time() * 1000)
            data['signature'] = self._generate_signature(data)

        sorted_data = sort_params(data)

        param_key = (
            'params'
            if force_params or method == RequestMethod.GET
            else 'data'
        )

        kwargs[param_key] = sorted_data

        return kwargs

    def _generate_signature(
        self,
        data: dict
    ) -> str:
        query_string = encode_params(data)

        m = hmac.new(
            self._api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256)

        return m.hexdigest()

    def _ws_api_signature_params(
        self,
        **params
    ) -> dict:
        """Build signed params for WebSocket API requests."""
        if self._api_key is None:
            raise APIKeyNotDefinedException('userDataStream.subscribe.signature')

        if self._api_secret is None:
            raise APISecretNotDefinedException('userDataStream.subscribe.signature')

        signed = {
            **params,
            'apiKey': self._api_key,
            'timestamp': int(time.time() * 1000)
        }
        signed['signature'] = self._generate_signature(signed)

        return signed

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
            raise StatusException(response, await response.text())
        try:
            return await response.json()
        except ValueError:
            raise InvalidResponseException(response, await response.text())

    # self._request('get', uri, symbol='BTCUSDT')
    async def _request(
        self,
        method: RequestMethod,
        uri: str,
        security_type: SecurityType = SecurityType.NONE,
        weight: int = 1,
        is_order: bool = False,
        **kwargs
    ) -> APIResponse:
        need_api_key, need_signed = security_type.value

        if need_api_key:
            if self._api_key is None:
                raise APIKeyNotDefinedException(uri)

            api_key = self._api_key
        else:
            api_key = None

        if need_signed and self._api_secret is None:
            raise APISecretNotDefinedException(uri)

        await self._rate_limiter.acquire_rest(weight=weight, is_order=is_order)

        req_kwargs = self._get_request_kwargs(
            method, need_signed, **kwargs)

        async with self._init_api_session(api_key) as session:
            async with getattr(
                session, method.value
            )(uri, **req_kwargs) as response:
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
