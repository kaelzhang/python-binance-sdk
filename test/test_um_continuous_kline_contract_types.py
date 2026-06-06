"""UM Continuous-Kline contract-type validation aligns with developers.binance.com.

Per the UM Continuous Contract Kline/Candlestick Streams docs the accepted
``contractType`` values are: ``perpetual``, ``current_quarter``,
``next_quarter`` and ``tradifi_perpetual`` (the TradFi-Perps product added
2025-12-11). The previously-listed ``CURRENT_QUARTER_DELIVERING`` and
``NEXT_QUARTER_DELIVERING`` are NOT documented for this stream.

COIN-M's docs accept only ``perpetual``, ``current_quarter`` and
``next_quarter`` -- ``tradifi_perpetual`` is UM-only.

Refs:
- UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
- CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
"""

import pytest
from volas import TimeFrame

from binance import SubType
from binance.core.common.exceptions import InvalidSubTypeParamException


# ---------------------------------------------------------------------------
# UM: tradifi_perpetual is accepted; *_DELIVERING are NOT accepted.
# ---------------------------------------------------------------------------


def test_um_continuous_kline_accepts_tradifi_perpetual():
    """UM accepts ``TRADIFI_PERPETUAL`` (case-insensitive; emitted lowercase)."""
    from binance.futures.um.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    result = proc.subscribe_param(
        True, SubType.CONTINUOUS_KLINE, 'BTCUSDT',
        'TRADIFI_PERPETUAL', TimeFrame.m1,
    )
    assert result == 'btcusdt_tradifi_perpetual@continuousKline_1m'


def test_um_continuous_kline_accepts_tradifi_perpetual_lowercase():
    """UM accepts the literal lowercase ``tradifi_perpetual`` per docs."""
    from binance.futures.um.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    result = proc.subscribe_param(
        True, SubType.CONTINUOUS_KLINE, 'ETHUSDT',
        'tradifi_perpetual', TimeFrame.H1,
    )
    assert result == 'ethusdt_tradifi_perpetual@continuousKline_1h'


def test_um_continuous_kline_rejects_current_quarter_delivering():
    """``CURRENT_QUARTER_DELIVERING`` was removed; UM rejects it."""
    from binance.futures.um.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(
            True, SubType.CONTINUOUS_KLINE, 'BTCUSDT',
            'CURRENT_QUARTER_DELIVERING', TimeFrame.m1,
        )


def test_um_continuous_kline_rejects_next_quarter_delivering():
    """``NEXT_QUARTER_DELIVERING`` was removed; UM rejects it."""
    from binance.futures.um.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(
            True, SubType.CONTINUOUS_KLINE, 'BTCUSDT',
            'NEXT_QUARTER_DELIVERING', TimeFrame.m1,
        )


@pytest.mark.parametrize('ct', ['PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER'])
def test_um_continuous_kline_still_accepts_documented_types(ct):
    """UM still accepts the three baseline contract types."""
    from binance.futures.um.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    result = proc.subscribe_param(
        True, SubType.CONTINUOUS_KLINE, 'BTCUSDT', ct, TimeFrame.m1,
    )
    assert result == f'btcusdt_{ct.lower()}@continuousKline_1m'


# ---------------------------------------------------------------------------
# CM: ``tradifi_perpetual`` NOT documented -> rejected. *_DELIVERING also gone.
# ---------------------------------------------------------------------------


def test_cm_continuous_kline_rejects_tradifi_perpetual():
    """``tradifi_perpetual`` is UM-only; CM rejects it."""
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(
            True, SubType.CONTINUOUS_KLINE, 'BTCUSD',
            'TRADIFI_PERPETUAL', TimeFrame.m1,
        )


def test_cm_continuous_kline_rejects_current_quarter_delivering():
    """CM also no longer documents ``CURRENT_QUARTER_DELIVERING``."""
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(
            True, SubType.CONTINUOUS_KLINE, 'BTCUSD',
            'CURRENT_QUARTER_DELIVERING', TimeFrame.m1,
        )


def test_cm_continuous_kline_rejects_next_quarter_delivering():
    """CM also no longer documents ``NEXT_QUARTER_DELIVERING``."""
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='contract_type'):
        proc.subscribe_param(
            True, SubType.CONTINUOUS_KLINE, 'BTCUSD',
            'NEXT_QUARTER_DELIVERING', TimeFrame.m1,
        )


@pytest.mark.parametrize('ct', ['PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER'])
def test_cm_continuous_kline_still_accepts_documented_types(ct):
    """CM still accepts the three baseline contract types."""
    from binance.futures.cm.streams import ContinuousKlineProcessor
    proc = ContinuousKlineProcessor(None)
    result = proc.subscribe_param(
        True, SubType.CONTINUOUS_KLINE, 'BTCUSD', ct, TimeFrame.m1,
    )
    assert result == f'btcusd_{ct.lower()}@continuousKline_1m'


# ---------------------------------------------------------------------------
# Module-level constants for downstream consumers.
# ---------------------------------------------------------------------------


def test_base_valid_contract_types_no_longer_carries_delivering_or_tradifi():
    """The shared base set is the CM-aligned three-value set."""
    from binance.futures.streams import VALID_CONTRACT_TYPES
    assert VALID_CONTRACT_TYPES == frozenset(
        {'PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER'}
    )


def test_um_valid_contract_types_adds_tradifi_perpetual():
    """UM-specific constant adds ``TRADIFI_PERPETUAL`` on top of the base."""
    from binance.futures.um.streams import UM_VALID_CONTRACT_TYPES
    assert UM_VALID_CONTRACT_TYPES == frozenset(
        {'PERPETUAL', 'CURRENT_QUARTER', 'NEXT_QUARTER', 'TRADIFI_PERPETUAL'}
    )
