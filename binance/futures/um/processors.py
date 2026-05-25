"""USDⓈ-M Futures framework processors.

The exception and stream-error processors for USDⓈ-M Futures reuse the same
handler base classes as Spot -- they are market-agnostic framework handlers
defined in :mod:`binance.spot.handlers`.
"""

from binance.core.processors.base import Processor
from binance.spot.handlers import (
    HandlerExceptionHandlerBase,
    StreamErrorHandlerBase,
)


class ExceptionProcessor(Processor):
    """Processor that routes handler exceptions to registered exception handlers."""
    HANDLER = HandlerExceptionHandlerBase


class StreamErrorProcessor(Processor):
    """Processor that routes stream-control errors to registered StreamErrorHandlerBase handlers."""
    HANDLER = StreamErrorHandlerBase
