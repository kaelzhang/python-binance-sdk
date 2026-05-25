"""Endpoint getter factory.

A market module describes each endpoint as a small spec and uses
:func:`define_getter` to install a concrete async method on its client class.
This currently supports the WebSocket-API transport (request/response over the
shared WS-API connection); REST transport support is added in a later task.
"""

from typing import Callable, Union


def define_getter(
    Target,
    name,
    ws_method,
    params=True,
    *,
    security_type,
    weight: Union[int, Callable[[dict], int]],
    is_order=False,
):
    """Build a concrete async WS-API closure and install it on ``Target``.

    The generated coroutine forwards its keyword arguments straight to
    :meth:`_ws_api_request` with the registered ``security``/``weight``/
    ``is_order``. ``weight`` may be a static ``int`` or a callable
    ``(kwargs) -> int`` resolved per call for params-dependent endpoints
    (e.g. ``order.test``, ``openOrders.status``).

    The docstring of the pre-declared stub method of the same ``name`` on
    ``Target`` is migrated onto the generated getter so the public API keeps its
    documentation.
    """
    weight_is_dynamic = callable(weight)

    def getter(self, **kwargs):
        resolved_weight = weight(kwargs) if weight_is_dynamic else weight

        return self._ws_api_request(
            ws_method,
            kwargs if params else None,
            security=security_type,
            weight=resolved_weight,
            is_order=is_order
        )

    origin = getattr(Target, name)

    # Migrate the docstring to the new getter.
    getter.__doc__ = origin.__doc__

    setattr(Target, name, getter)
