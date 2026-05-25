"""COIN-M Perpetual Futures market module.

Exports :class:`~binance.futures.cm.client.CMFuturesClient`, the async client
for Binance COIN-M (coin-margined) Futures market data (funding / open-interest /
liquidation).
"""

from binance.futures.cm.client import CMFuturesClient

__all__ = ['CMFuturesClient']
