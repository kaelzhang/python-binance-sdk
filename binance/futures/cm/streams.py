"""COIN-M Futures stream wiring: handlers, processors, and the PROCESSORS list.

COIN-M reuses the shared futures handler bases from :mod:`binance.futures.streams`.
This module:
1. Adds the COIN-M-specific ``ps`` (pair) field to the force-order column map.
2. Overrides ``subscribe_param`` on both processors to preserve underscores in
   COIN-M symbol names (e.g. ``BTCUSD_PERP`` -> ``btcusd_perp``, not ``btcusdperp``).

Confirmed payload field mappings (2026-05-25) against official Binance COIN-M docs:
- Mark Price stream: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Stream
- Liquidation Order stream: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Liquidation-Order-Streams

CM-specific vs UM:
- Mark Price: CM does NOT have ``ap`` (mark price moving average); UM DOES.
  The shared :class:`~binance.futures.streams.MarkPriceHandlerBase` base (without
  ``ap``) is used directly for COIN-M.
- Force Order: CM has ``ps`` (pair) in the nested ``o`` object; UM does NOT.
  :class:`ForceOrderHandlerBase` extends the shared base with ``ps``.
- Symbol normalization: COIN-M symbols contain underscores (e.g. ``BTCUSD_PERP``).
  The shared ``normalize_symbol`` helper strips underscores (designed for Spot/UM
  symbols like ``BTCUSDT``), which is incorrect for COIN-M stream subscriptions.
  Both CM processors override ``subscribe_param`` to use ``symbol.lower()`` instead,
  preserving the underscore (``btcusd_perp@markPrice``).
"""

from binance.core.common.constants import SubType
from binance.futures.streams import (  # noqa: F401  (re-exported for convenience)
    MarkPriceHandlerBase,
    ForceOrderHandlerBase as _ForceOrderHandlerBase,
    FORCE_ORDER_COLUMNS_MAP_BASE,
    MarkPriceProcessor as _MarkPriceProcessor,
    ForceOrderProcessor as _ForceOrderProcessor,
)


# ---------------------------------------------------------------------------
# CM-specific force order: add the 'ps' (pair) field.
# Confirmed present in COIN-M (2026-05-25); absent from USDⓈ-M.
# Fields confirmed from COIN-M docs (2026-05-25):
#   (all common fields from FORCE_ORDER_COLUMNS_MAP_BASE)
#   ps  pair  <-- CM-only field in nested 'o'
# ---------------------------------------------------------------------------

FORCE_ORDER_COLUMNS_MAP = {
    **FORCE_ORDER_COLUMNS_MAP_BASE,
    'ps': 'pair',  # CM-only: pair designation in the nested order object
}

FORCE_ORDER_COLUMNS = FORCE_ORDER_COLUMNS_MAP.keys()


class ForceOrderHandlerBase(_ForceOrderHandlerBase):
    """Base handler for the COIN-M ``SubType.FORCE_ORDER`` (liquidation order) stream.

    Extends the shared :class:`~binance.futures.streams.ForceOrderHandlerBase` with
    the COIN-M-specific ``ps`` (pair) column, which is present in COIN-M nested
    order objects but absent from USDⓈ-M.

    The raw payload nests order details under an ``'o'`` key (inherited flattening
    from the shared base applies; ``ps`` is one of the nested fields).

    Subclass and override ``receive(payload)`` to handle events.  The base ``receive``
    converts the raw dict into a ``StockDataFrame`` with human-readable column names
    (e.g. ``symbol``, ``pair``, ``side``, ``price``, ``avg_price``, ``order_status``).

    Example::

        from binance import CMFuturesClient, SubType
        from binance.futures.cm.streams import ForceOrderHandlerBase

        class MyHandler(ForceOrderHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['symbol'], df['pair'], df['price'])

        client = CMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.FORCE_ORDER, 'btcusd_perp')
    """

    COLUMNS_MAP = FORCE_ORDER_COLUMNS_MAP
    COLUMNS = FORCE_ORDER_COLUMNS


# ---------------------------------------------------------------------------
# CM processors
# Both override subscribe_param to preserve underscores in COIN-M symbols.
# COIN-M stream names: btcusd_perp@markPrice, btcusd_perp@forceOrder
# The shared normalize_symbol() strips underscores (designed for Spot/UM); wrong here.
# ---------------------------------------------------------------------------

class MarkPriceProcessor(_MarkPriceProcessor):
    """Processor for the COIN-M mark-price stream (``<symbol>@markPrice``).

    Overrides ``subscribe_param`` to use ``symbol.lower()`` (preserving underscores)
    instead of ``normalize_symbol`` (which would strip the underscore in
    ``BTCUSD_PERP`` -> ``btcusdperp``, incorrectly).
    """

    HANDLER = MarkPriceHandlerBase

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@markPrice``, preserving underscores in COIN-M symbols.

        Accepts an optional second positional argument ``update_speed``:
        pass ``'1s'`` to get the 1-second stream (``<symbol>@markPrice@1s``).
        """
        symbol = self._get_param_symbol(t, args)
        base = f'{symbol.lower()}@{SubType.MARK_PRICE}'
        if len(args) >= 2 and args[1] == '1s':
            return f'{base}@1s'
        return base


class ForceOrderProcessor(_ForceOrderProcessor):
    """Processor for the COIN-M liquidation order stream (``<symbol>@forceOrder``).

    Uses the COIN-M :class:`ForceOrderHandlerBase` (which includes ``pair``).
    Overrides ``subscribe_param`` to use ``symbol.lower()`` (preserving underscores).
    """

    HANDLER = ForceOrderHandlerBase

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@forceOrder``, preserving underscores in COIN-M symbols."""
        symbol = self._get_param_symbol(t, args)
        return f'{symbol.lower()}@{SubType.FORCE_ORDER}'


PROCESSORS = [
    MarkPriceProcessor,
    ForceOrderProcessor,
]
