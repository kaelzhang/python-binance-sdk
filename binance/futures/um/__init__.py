"""USDⓈ-M Perpetual Futures market module.

Exports :class:`~binance.futures.um.client.UMFuturesClient`, the async client
for Binance USDⓈ-M Futures market data (funding / open-interest / liquidation).
"""

from binance.futures.um.client import UMFuturesClient

__all__ = ['UMFuturesClient']
