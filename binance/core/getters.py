"""Endpoint getter factory.

A market module describes each endpoint as a small spec and uses
:func:`define_getter` to install a concrete async method on its client class.
Two transports are supported:

- ``transport='ws_api'`` (default): request/response over the shared WS-API
  connection via :meth:`_ws_api_request`.
- ``transport='rest'``: HTTP request via :meth:`get` (RestTransport), using
  the ``rest_url`` (absolute URL) and ``method`` (GET by default) from the
  endpoint spec.
"""

from typing import Callable, Union

from binance.core.common.constants import RequestMethod


def define_getter(
    Target,
    name,
    *,
    security_type,
    weight: Union[int, Callable[[dict], int]],
    # WS-API transport
    ws_method: str = '',
    params: bool = True,
    is_order: bool = False,
    # REST transport
    transport: str = 'ws_api',
    rest_url: str = '',
    method: RequestMethod = RequestMethod.GET,
):
    """Build a concrete async closure and install it on ``Target``.

    For ``transport='ws_api'`` (default): the generated coroutine forwards its
    keyword arguments straight to :meth:`_ws_api_request` with the registered
    ``security``/``weight``/``is_order``.

    For ``transport='rest'``: the generated coroutine issues an HTTP request
    via :meth:`get` (or :meth:`post`/... for other methods) on the
    ``rest_url``. All keyword arguments become query/body parameters. When the
    spec sets ``is_order=True``, the account ORDERS pool is consumed alongside
    the IP REQUEST_WEIGHT / RAW_REQUESTS pools — the same semantics that apply
    to WS-API order-placing endpoints.

    ``weight`` may be a static ``int`` or a callable ``(kwargs) -> int``
    resolved per call for params-dependent endpoints.

    The docstring of the pre-declared stub method of the same ``name`` on
    ``Target`` is migrated onto the generated getter so the public API keeps its
    documentation.
    """
    weight_is_dynamic = callable(weight)

    if transport == 'rest':
        # REST getter: call self.get()/post()/... via RestTransport
        _method = method  # capture
        _is_order = is_order

        def getter(self, **kwargs):
            resolved_weight = weight(kwargs) if weight_is_dynamic else weight

            return self._request(
                _method,
                rest_url,
                security_type=security_type,
                weight=resolved_weight,
                is_order=_is_order,
                **kwargs,
            )
    else:
        # Default: WS-API getter
        _ws_method = ws_method
        _params = params

        def getter(self, **kwargs):
            resolved_weight = weight(kwargs) if weight_is_dynamic else weight

            return self._ws_api_request(
                _ws_method,
                kwargs if _params else None,
                security=security_type,
                weight=resolved_weight,
                is_order=is_order
            )

    origin = getattr(Target, name)

    # Migrate the docstring to the new getter.
    getter.__doc__ = origin.__doc__

    setattr(Target, name, getter)
