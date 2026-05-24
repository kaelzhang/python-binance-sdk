from binance.common.constants import (
    REST_API_VERSION,
    SecurityType,
    RequestMethod
)
from binance.rate_limit import REST_ENDPOINT_WEIGHTS, depth_weight

# Rest APIs ref:
# https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md
#
# NOTE: every former REST endpoint has been migrated to the Binance WebSocket
# API and now lives in `binance.apis.ws_api.WsApiGetters`. This list is
# intentionally empty -- the REST request plumbing (`define_getter` /
# `RestAPIGetters._rest_uri`) below is now dead and kept only as a shell to be
# removed in the next task (G-06), together with the generic `ClientBase._request`
# escape hatch. It is therefore excluded from coverage (`# pragma: no cover`).
APIS = []


def define_getter(  # pragma: no cover - dead REST shell, removed in G-06
    Target,
    name,
    path,
    params=True,
    version=REST_API_VERSION,
    method=RequestMethod.GET,
    security_type=SecurityType.NONE,
    is_order=False
):
    """Internal factory: build a concrete async REST closure and install it on ``Target``."""
    base_weight = REST_ENDPOINT_WEIGHTS.get(path, 1)

    def getter(self, **kwargs):
        uri = self._rest_uri(path, version)
        ka = kwargs if params else {}
        if path == 'depth':
            weight = depth_weight(int(kwargs.get('limit', 100)))
        else:
            weight = base_weight

        return self._request(
            method,
            uri,
            security_type,
            weight=weight,
            is_order=is_order,
            **ka
        )

    origin = getattr(Target, name)

    # Migrate the docstring to the new getter
    getter.__doc__ = origin.__doc__

    setattr(Target, name, getter)

# Google Style guide:
# https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
# Sphinx extension supports a mixed style of
#   google and reStructuredText formatting

# Neither VSCode Python language server nor Jedi server could handle
#   class methods which are dynamically added by `setattr`, see:
# https://jedi.readthedocs.io/en/latest/docs/features.html#not-supported
#
# So, however, we need to just create those methods and docstrings first,
#   then override them.

# pylint: disable=no-member


class RestAPIGetters:
    """Internal mixin providing dynamically-generated async methods for Binance ``/api/`` endpoints."""

    _api_host: str

    def _rest_uri(  # pragma: no cover - dead REST shell, removed in G-06
        self, path, version=REST_API_VERSION
    ) -> str:
        return self._api_host + '/api/' + version + '/' + path


for getter_setting in APIS:
    define_getter(RestAPIGetters, **getter_setting)  # pragma: no cover
