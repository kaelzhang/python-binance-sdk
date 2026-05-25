"""USDⓈ-M Futures framework processors.

The exception and stream-error processors for USDⓈ-M Futures reuse the shared
market-agnostic framework processors defined in :mod:`binance.core.processors.framework`.
"""

from binance.core.processors.framework import (  # noqa: F401
    ExceptionProcessor,
    StreamErrorProcessor,
)
