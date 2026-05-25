"""USDⓈ-M Futures stream wiring: handlers, processors, and the PROCESSORS list.

USDⓈ-M reuses the shared futures handler bases and processors from
:mod:`binance.futures.streams`.  This module adds the USDⓈ-M-specific
``ap`` (mark price moving average) field to the mark-price column map, which is
present in USDⓈ-M but absent from COIN-M.

Confirmed payload field mappings (2026-05-25) against official Binance docs:
- Mark Price stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
- Liquidation Order stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams

UM-specific vs CM:
- Mark Price: UM has ``ap`` (mark price moving average); CM does NOT.
- Force Order: UM does NOT have ``ps`` (pair) in nested ``o``; CM DOES.
"""

from binance.futures.streams import (  # noqa: F401  (re-exported for public API)
    MarkPriceHandlerBase as _MarkPriceHandlerBase,
    ForceOrderHandlerBase,
    FORCE_ORDER_COLUMNS_MAP_BASE,
    MARK_PRICE_COLUMNS_MAP_BASE,
    MarkPriceProcessor as _MarkPriceProcessor,
    ForceOrderProcessor,
)


# ---------------------------------------------------------------------------
# UM-specific mark price: add the 'ap' (mark price moving average) field.
# Confirmed present in USDⓈ-M (2026-05-25); absent from COIN-M.
# Fields confirmed from docs (2026-05-25):
#   e  event type
#   E  event time
#   s  symbol
#   p  mark price
#   ap mark price moving average  <-- UM-only
#   i  index price
#   P  estimated settle price
#   r  funding rate
#   T  next funding time
# ---------------------------------------------------------------------------

MARK_PRICE_COLUMNS_MAP = {
    **MARK_PRICE_COLUMNS_MAP_BASE,
    'ap': 'mark_price_avg',  # UM-only field
}

MARK_PRICE_COLUMNS = MARK_PRICE_COLUMNS_MAP.keys()

FORCE_ORDER_COLUMNS_MAP = FORCE_ORDER_COLUMNS_MAP_BASE
FORCE_ORDER_COLUMNS = FORCE_ORDER_COLUMNS_MAP.keys()


class MarkPriceHandlerBase(_MarkPriceHandlerBase):
    """Base handler for the USDⓈ-M ``SubType.MARK_PRICE`` stream.

    Extends the shared :class:`~binance.futures.streams.MarkPriceHandlerBase` with
    the USDⓈ-M-specific ``ap`` (mark price moving average) column, which is present
    in USDⓈ-M payloads but absent from COIN-M.

    Subclass this and override ``receive(payload)`` to handle the event.
    The base ``receive`` converts the raw dict into a ``StockDataFrame`` with
    human-readable column names (e.g. ``mark_price``, ``mark_price_avg``,
    ``funding_rate``, ``next_funding_time``).

    Example::

        from binance import UMFuturesClient, MarkPriceHandlerBase

        class MyHandler(MarkPriceHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['mark_price'])

        client = UMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.MARK_PRICE, 'btcusdt')
    """

    COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP
    COLUMNS = MARK_PRICE_COLUMNS


class MarkPriceProcessor(_MarkPriceProcessor):
    """Processor for the USDⓈ-M mark-price stream (``<symbol>@markPrice``).

    Uses the USDⓈ-M :class:`MarkPriceHandlerBase` (which includes ``ap``).
    """

    HANDLER = MarkPriceHandlerBase


PROCESSORS = [
    MarkPriceProcessor,
    ForceOrderProcessor,
]
