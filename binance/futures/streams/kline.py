"""Kline and continuous-kline stream handlers and processors.

Hosts ``KlineHandlerBase``/``KlineProcessor`` (per-symbol kline) and the
``ContinuousKlineHandlerBase``/``ContinuousKlineProcessor`` plus their valid
interval / contract-type constants.  See
:mod:`binance.futures.streams._common` for the per-stream verified findings.
"""

from typing import List, Optional

from binance.core.common.constants import (
    SubType,
    STREAM_TYPE_MAP,
    STREAM_OHLC_MAP,
    KLINE_TYPE_PREFIX,
)
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.common.types import DictPayload
from binance.core.common.utils import normalize_symbol
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor


# ---------------------------------------------------------------------------
# Futures Kline
# Confirmed fields (UM + CM identical, 2026-05-26):
# Same nested 'k' structure as Spot kline.  The shared Spot column map applies.
# Fields in nested 'k':
#   t  open time
#   T  close time
#   s  symbol
#   i  interval
#   f  first trade id
#   L  last trade id
#   o, h, l, c  OHLC
#   x  is closed
#   v  volume (base asset)
#   q  quote volume
#   V  taker volume
#   Q  taker quote volume
#   n  total trades
# Outer: E event time (lifted into k['E'] by _receive)
# ---------------------------------------------------------------------------

FUTURES_KLINE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    't': 'open_time',
    'T': 'close_time',
    's': 'symbol',
    'i': 'interval',
    'f': 'first_trade_id',
    'L': 'last_trade_id',
    **STREAM_OHLC_MAP,
    'x': 'is_closed',
    'v': 'volume',
    'q': 'quote_volume',
    'V': 'taker_volume',
    'Q': 'taker_quote_volume',
    'n': 'total_trades',
}

FUTURES_KLINE_COLUMNS = FUTURES_KLINE_COLUMNS_MAP.keys()

# Per developers.binance.com (UM + CM kline / continuousKline /
# indexPriceKline / markPriceKline streams), the smallest supported kline
# interval on futures is ``1m``.  ``1s`` is Spot-only and is NOT accepted on
# any futures kline stream.
VALID_FUTURES_KLINE_INTERVALS = frozenset((
    '1m', '3m', '5m', '15m', '30m',
    '1h', '2h', '4h', '6h', '8h', '12h',
    '1d', '3d', '1w', '1M'
))


class KlineHandlerBase(Handler):
    """Base handler for the futures ``SubType.KLINE`` stream.

    Shared across USDⓈ-M and COIN-M markets.  The nested ``k`` payload structure
    is identical to the Spot kline; the internal ``_receive`` lifts ``E`` (event
    time) from the outer envelope into the flattened ``k`` dict before converting.

    Subclass this and override ``receive(payload)`` to handle events.  The base
    ``receive`` returns a ``StockDataFrame`` with human-readable column names
    (e.g. ``open``, ``close``, ``volume``, ``is_closed``).

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
    """

    COLUMNS_MAP = FUTURES_KLINE_COLUMNS_MAP
    COLUMNS = FUTURES_KLINE_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        k = payload['k']
        k['E'] = payload['E']
        return super()._receive(k, index)


# ---------------------------------------------------------------------------
# Futures ContinuousKline
# Wire stream: <pair>_<contractType>@continuousKline_<interval>
# Confirmed fields (UM + CM identical, 2026-05-26):
# Outer:
#   e  'continuous_kline'
#   E  event time
#   ps pair  (e.g. 'BTCUSDT' for UM, 'BTCUSD' for CM)
#   ct contract type (e.g. 'PERPETUAL', 'CURRENT_QUARTER')
# Nested 'k':
#   same kline fields as regular kline (t,T,i,f,L,o,h,l,c,v,n,q,V,Q,x)
#   but 's' is set to '' (empty string) in continuous kline
# ---------------------------------------------------------------------------

FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    'ps': 'pair',
    'ct': 'contract_type',
    't': 'open_time',
    'T': 'close_time',
    'i': 'interval',
    'f': 'first_trade_id',
    'L': 'last_trade_id',
    **STREAM_OHLC_MAP,
    'x': 'is_closed',
    'v': 'volume',
    'q': 'quote_volume',
    'V': 'taker_volume',
    'Q': 'taker_quote_volume',
    'n': 'total_trades',
}

FUTURES_CONTINUOUS_KLINE_COLUMNS = FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP.keys()

VALID_CONTRACT_TYPES = frozenset((
    'PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER',
    'CURRENT_QUARTER_DELIVERING', 'NEXT_QUARTER_DELIVERING',
))


class ContinuousKlineHandlerBase(Handler):
    """Base handler for the futures ``SubType.CONTINUOUS_KLINE`` stream.

    Shared across USDⓈ-M and COIN-M markets.  The stream name has the form
    ``<pair>_<contractType>@continuousKline_<interval>`` (e.g.
    ``btcusdt_perpetual@continuousKline_1m``).  The nested ``k`` dict is
    flattened; outer ``ps`` (pair) and ``ct`` (contract type) are merged in.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
    """

    COLUMNS_MAP = FUTURES_CONTINUOUS_KLINE_COLUMNS_MAP
    COLUMNS = FUTURES_CONTINUOUS_KLINE_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        k = payload['k']
        flat = {
            'e': payload['e'],
            'E': payload['E'],
            'ps': payload['ps'],
            'ct': payload['ct'],
            **k,
        }
        return super()._receive(flat, index)


class KlineProcessor(Processor):
    """Processor for the futures kline stream (``<symbol>@kline_<interval>``).

    Shared by both USDⓈ-M and COIN-M markets.
    """

    HANDLER = KlineHandlerBase
    SUB_TYPE = SubType.KLINE

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<symbol>@kline_<interval>``."""
        symbol = self._get_param_symbol(t, args)

        if len(args) < 2:
            raise InvalidSubTypeParamException(
                t, 'interval', '`TimeFrame` expected but not specified')

        interval = args[1]
        interval_str = str(interval)

        if interval_str not in VALID_FUTURES_KLINE_INTERVALS:
            raise InvalidSubTypeParamException(
                t,
                'interval',
                'invalid kline interval `%s`; must be one of %s'
                % (interval_str, sorted(VALID_FUTURES_KLINE_INTERVALS))
            )

        return f'{normalize_symbol(symbol)}@{KLINE_TYPE_PREFIX}{interval}'


class ContinuousKlineProcessor(Processor):
    """Processor for the futures continuous-contract kline stream.

    Wire name: ``<pair>_<contractType>@continuousKline_<interval>``
    (e.g. ``btcusdt_perpetual@continuousKline_1m`` for UM).

    Shared by both USDⓈ-M and COIN-M markets.
    Subscription requires three positional parameters after the SubType:
    ``pair``, ``contract_type``, and ``interval``.
    """

    HANDLER = ContinuousKlineHandlerBase
    SUB_TYPE = SubType.CONTINUOUS_KLINE
    PAYLOAD_TYPE = 'continuous_kline'

    def subscribe_param(self, _, t, *args) -> str:
        """Return ``<pair>_<contractType>@continuousKline_<interval>``.

        Args:
            args[0]: pair string (e.g. ``'BTCUSDT'`` for UM, ``'BTCUSD'`` for CM).
            args[1]: contract type string (e.g. ``'PERPETUAL'``, ``'CURRENT_QUARTER'``).
            args[2]: interval (``TimeFrame`` or str, e.g. ``TimeFrame.M1``).
        """
        if len(args) < 3:
            raise InvalidSubTypeParamException(
                t, 'pair/contract_type/interval',
                'CONTINUOUS_KLINE requires pair, contract_type, and interval parameters'
            )

        pair = args[0]
        contract_type = args[1]
        interval = args[2]

        if type(pair) is not str:
            raise InvalidSubTypeParamException(
                t, 'pair', 'string expected but got `%s`' % pair)

        if type(contract_type) is not str:
            raise InvalidSubTypeParamException(
                t, 'contract_type', 'string expected but got `%s`' % contract_type)

        ct_upper = contract_type.upper()
        if ct_upper not in VALID_CONTRACT_TYPES:
            raise InvalidSubTypeParamException(
                t, 'contract_type',
                'invalid contract type `%s`; must be one of %s'
                % (contract_type, sorted(VALID_CONTRACT_TYPES))
            )

        interval_str = str(interval)
        if interval_str not in VALID_FUTURES_KLINE_INTERVALS:
            raise InvalidSubTypeParamException(
                t, 'interval',
                'invalid kline interval `%s`; must be one of %s'
                % (interval_str, sorted(VALID_FUTURES_KLINE_INTERVALS))
            )

        return f'{normalize_symbol(pair)}_{ct_upper.lower()}@continuousKline_{interval}'
