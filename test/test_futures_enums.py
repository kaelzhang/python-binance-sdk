"""Tests for USDⓈ-M / Futures enums (binance/futures/enums.py).

Confirms:
- all five enum classes are importable from ``binance`` (top-level export);
- wire values match the Binance API spec;
- ``str(member)`` returns the raw wire string (StringEnum contract).
"""

from binance import (
    PositionSide,
    FuturesOrderType,
    WorkingType,
    MarginType,
    FuturesTimeInForce,
)


# ---------------------------------------------------------------------------
# PositionSide
# ---------------------------------------------------------------------------

def test_position_side_values():
    assert str(PositionSide.BOTH) == 'BOTH'
    assert str(PositionSide.LONG) == 'LONG'
    assert str(PositionSide.SHORT) == 'SHORT'


def test_position_side_members():
    members = {m.value for m in PositionSide}
    assert members == {'BOTH', 'LONG', 'SHORT'}


# ---------------------------------------------------------------------------
# FuturesOrderType
# ---------------------------------------------------------------------------

def test_futures_order_type_values():
    assert str(FuturesOrderType.LIMIT) == 'LIMIT'
    assert str(FuturesOrderType.MARKET) == 'MARKET'
    assert str(FuturesOrderType.STOP) == 'STOP'
    assert str(FuturesOrderType.STOP_MARKET) == 'STOP_MARKET'
    assert str(FuturesOrderType.TAKE_PROFIT) == 'TAKE_PROFIT'
    assert str(FuturesOrderType.TAKE_PROFIT_MARKET) == 'TAKE_PROFIT_MARKET'
    assert str(FuturesOrderType.TRAILING_STOP_MARKET) == 'TRAILING_STOP_MARKET'


def test_futures_order_type_members():
    members = {m.value for m in FuturesOrderType}
    assert members == {
        'LIMIT', 'MARKET', 'STOP', 'STOP_MARKET',
        'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET',
    }


# ---------------------------------------------------------------------------
# WorkingType
# ---------------------------------------------------------------------------

def test_working_type_values():
    assert str(WorkingType.MARK_PRICE) == 'MARK_PRICE'
    assert str(WorkingType.CONTRACT_PRICE) == 'CONTRACT_PRICE'


def test_working_type_members():
    members = {m.value for m in WorkingType}
    assert members == {'MARK_PRICE', 'CONTRACT_PRICE'}


# ---------------------------------------------------------------------------
# MarginType
# ---------------------------------------------------------------------------

def test_margin_type_values():
    assert str(MarginType.ISOLATED) == 'ISOLATED'
    assert str(MarginType.CROSSED) == 'CROSSED'


def test_margin_type_members():
    members = {m.value for m in MarginType}
    assert members == {'ISOLATED', 'CROSSED'}


# ---------------------------------------------------------------------------
# FuturesTimeInForce
# ---------------------------------------------------------------------------

def test_futures_time_in_force_values():
    assert str(FuturesTimeInForce.GTC) == 'GTC'
    assert str(FuturesTimeInForce.IOC) == 'IOC'
    assert str(FuturesTimeInForce.FOK) == 'FOK'
    assert str(FuturesTimeInForce.GTX) == 'GTX'
    assert str(FuturesTimeInForce.GTD) == 'GTD'
    assert str(FuturesTimeInForce.RPI) == 'RPI'


def test_futures_time_in_force_members():
    members = {m.value for m in FuturesTimeInForce}
    assert members == {'GTC', 'IOC', 'FOK', 'GTX', 'GTD', 'RPI'}


# ---------------------------------------------------------------------------
# StringEnum contract: str(member) == member.value
# ---------------------------------------------------------------------------

def test_string_enum_str_returns_wire_value():
    for enum_cls in (PositionSide, FuturesOrderType, WorkingType,
                     MarginType, FuturesTimeInForce):
        for member in enum_cls:
            assert str(member) == member.value, (
                f'{enum_cls.__name__}.{member.name}: '
                f'str() {str(member)!r} != value {member.value!r}'
            )


# ---------------------------------------------------------------------------
# Top-level import works (confirm __init__.py exports)
# ---------------------------------------------------------------------------

def test_top_level_imports_all_five_enums():
    # If these imports succeed (already tested by the module-level imports
    # above), the names are in the binance package namespace.
    assert PositionSide is not None
    assert FuturesOrderType is not None
    assert WorkingType is not None
    assert MarginType is not None
    assert FuturesTimeInForce is not None
