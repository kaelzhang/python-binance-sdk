"""Framework-level processors shared across all markets.

These processors are market-agnostic and are used by both Spot and
USDⓈ-M Futures (and any future market) to route stream-control errors
and handler exceptions to their respective handler bases.
"""

from binance.core.handlers.framework import (
    HandlerExceptionHandlerBase,
    StreamErrorHandlerBase,
)
from binance.core.processors.base import Processor


class StreamErrorProcessor(Processor):
    """Processor that routes stream-control errors to registered StreamErrorHandlerBase handlers."""
    HANDLER = StreamErrorHandlerBase


class ExceptionProcessor(Processor):
    """Processor that routes dispatch exceptions to registered exception handlers."""
    HANDLER = HandlerExceptionHandlerBase
