"""Backward-compatible re-exports of REST helpers relocated to
:mod:`binance.core.transport.rest`.

Historically these lived here on ``ClientBase``; the machinery now lives in the
market-agnostic ``core`` transport. This shim keeps the old import paths working
for the test suite during the restructure.
"""

from binance.core.transport.rest import (  # noqa: F401
    sort_params,
    encode_params,
    _reject_float_params,
    _param_str,
)
