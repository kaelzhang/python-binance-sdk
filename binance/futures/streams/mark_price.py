"""Mark price stream handlers and processors (per-symbol and all-market).

Hosts the shared ``MarkPriceHandlerBase`` (and column map) plus the all-market
``AllMarketMarkPriceHandlerBase`` and their processors.  See
:mod:`binance.futures.streams._common` for the per-stream verified findings.
"""

from typing import ClassVar

from binance.core.common.constants import (
    SubType,
    STREAM_TYPE_MAP,
    KEY_STREAM_TYPE,
    KEY_PAYLOAD,
)
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.common.utils import normalize_symbol
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor


# ---------------------------------------------------------------------------
# Shared Mark Price base
# Common fields confirmed present in BOTH UM and CM (2026-05-25):
#   e  event type ('markPriceUpdate')
#   E  event time
#   s  symbol
#   p  mark price
#   i  index price
#   P  estimated settle price
#   r  funding rate
#   T  next funding time
#
# UM-only field: ap (mark price moving average)  — added by UMMarkPriceHandlerBase
# CM lacks 'ap' entirely.
# ---------------------------------------------------------------------------

# Common mark price fields shared by both markets.
MARK_PRICE_COLUMNS_MAP_BASE = {
    **STREAM_TYPE_MAP,        # 'e' -> 'type'
    'E': 'event_time',
    's': 'symbol',
    'p': 'mark_price',
    'i': 'index_price',
    'P': 'est_settle_price',
    'r': 'funding_rate',
    'T': 'next_funding_time',
}


class MarkPriceHandlerBase(Handler):
    """Base handler for the futures ``SubType.MARK_PRICE`` stream.

    Shared across USDⓈ-M (``UMFuturesClient``) and COIN-M (``CMFuturesClient``) markets.
    Covers the common payload fields: mark price, index price, estimated settlement price,
    funding rate, and next funding time.

    USDⓈ-M extends this with ``ap`` (mark price moving average) via the market-specific
    ``COLUMNS_MAP`` override in ``binance.futures.um.streams``.  COIN-M uses this base
    directly (COIN-M does not publish ``ap``).

    Subclass and override ``receive(payload)`` to handle events.  The base ``receive``
    converts the raw dict into a ``StockDataFrame`` with human-readable column names.

    Example (COIN-M)::

        from binance import CMFuturesClient, MarkPriceHandlerBase

        class MyHandler(MarkPriceHandlerBase):
            def receive(self, payload):
                df = super().receive(payload)
                print(df['mark_price'])

        client = CMFuturesClient()
        client.handler(MyHandler())
        await client.subscribe(SubType.MARK_PRICE, 'btcusd_perp')
    """

    COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP_BASE
    COLUMNS = MARK_PRICE_COLUMNS_MAP_BASE.keys()


# ---------------------------------------------------------------------------
# All-market arrays: AllMarketMarkPrice
# Wire stream: !markPrice@arr[@1s]
# Each element is a markPriceUpdate dict (identical to per-symbol markPrice).
# UM elements include 'ap'; CM elements do not.
# The shared base uses MARK_PRICE_COLUMNS_MAP_BASE (no 'ap').
# UM extends via AllMarketMarkPriceHandlerBase in um/streams.py.
# ---------------------------------------------------------------------------

class AllMarketMarkPriceHandlerBase(Handler):
    """Base handler for the ``SubType.ALL_MARKET_MARK_PRICE`` stream (``!markPrice@arr[@1s]``).

    Shared by USDⓈ-M and COIN-M markets (base uses common fields only).
    Receives an array of ``markPriceUpdate`` events for every futures symbol.
    USDⓈ-M extends this base to include the ``ap`` (mark price moving average) field.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Stream
    """

    COLUMNS_MAP = MARK_PRICE_COLUMNS_MAP_BASE
    COLUMNS = MARK_PRICE_COLUMNS_MAP_BASE.keys()


class MarkPriceProcessor(Processor):
    """Processor for the futures mark-price stream (``<symbol>@markPrice``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = MarkPriceHandlerBase
    SUB_TYPE = SubType.MARK_PRICE
    PAYLOAD_TYPE = 'markPriceUpdate'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@markPrice``.

        Accepts an optional second positional argument ``update_speed``:
        pass ``'1s'`` to get the 1-second stream (``<symbol>@markPrice@1s``).
        """
        symbol = self._get_param_symbol(t, args)
        base = f'{normalize_symbol(symbol)}@{SubType.MARK_PRICE}'
        if len(args) >= 2 and args[1] == '1s':
            return f'{base}@1s'
        return base


class AllMarketMarkPriceProcessor(Processor):
    """Processor for the futures all-market mark price stream (``!markPrice@arr[@1s]``).

    Routing is by stream name prefix.  Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = AllMarketMarkPriceHandlerBase
    SUB_TYPE = SubType.ALL_MARKET_MARK_PRICE
    STREAM_TYPE_PREFIX: ClassVar[str] = '!markPrice@arr'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if (
            stream_type is not None
            and stream_type.startswith(self.STREAM_TYPE_PREFIX)
        ):
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``!markPrice@arr`` or ``!markPrice@arr@1s``."""
        if len(args) > 1:
            raise InvalidSubTypeParamException(
                t, 'update_speed',
                '`SubType.ALL_MARKET_MARK_PRICE` accepts at most one optional '
                'parameter: update_speed (pass ``\'1s\'`` for 1-second updates)'
            )
        base = self.STREAM_TYPE_PREFIX
        if len(args) == 1 and args[0] == '1s':
            return f'{base}@1s'
        return base
