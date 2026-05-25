"""Shared enums for Binance Futures markets (USDⓈ-M and COIN-M).

These supplement the shared core enums (``OrderSide``, ``SecurityType``, …)
with futures-specific concepts such as position side, futures order types,
working price type, margin mode, and the extended futures time-in-force values.
"""

from binance.core.common.constants import StringEnum


class PositionSide(StringEnum):
    """Specifies the position direction for a futures order or position.

    Each member's wire value is returned by ``str(member)``, e.g.
    ``str(PositionSide.BOTH) == 'BOTH'``.  Compare members to members
    (``pos_side == PositionSide.LONG``).

    Members:
        BOTH: One-way (non-hedge) mode — the single net position.
        LONG: Hedge mode long position.
        SHORT: Hedge mode short position.
    """

    BOTH = 'BOTH'
    LONG = 'LONG'
    SHORT = 'SHORT'


class FuturesOrderType(StringEnum):
    """Futures order execution type.

    Supersedes the Spot ``OrderType``; futures markets support an extended
    set that includes stop-market, take-profit-market, and trailing-stop
    variants.

    Members:
        LIMIT: Limit order.
        MARKET: Market order.
        STOP: Stop-limit order (triggered by ``stopPrice``; requires ``price``).
        STOP_MARKET: Stop-market order (triggered by ``stopPrice``).
        TAKE_PROFIT: Take-profit limit order (triggered by ``stopPrice``; requires ``price``).
        TAKE_PROFIT_MARKET: Take-profit market order (triggered by ``stopPrice``).
        TRAILING_STOP_MARKET: Trailing-stop market order (activated by ``callbackRate``).
    """

    LIMIT = 'LIMIT'
    MARKET = 'MARKET'
    STOP = 'STOP'
    STOP_MARKET = 'STOP_MARKET'
    TAKE_PROFIT = 'TAKE_PROFIT'
    TAKE_PROFIT_MARKET = 'TAKE_PROFIT_MARKET'
    TRAILING_STOP_MARKET = 'TRAILING_STOP_MARKET'


class WorkingType(StringEnum):
    """The price type used to trigger a conditional (stop/take-profit) order.

    Members:
        MARK_PRICE: Use the mark price (fair value; default for most stop orders).
        CONTRACT_PRICE: Use the last traded price.
    """

    MARK_PRICE = 'MARK_PRICE'
    CONTRACT_PRICE = 'CONTRACT_PRICE'


class MarginType(StringEnum):
    """Margin mode for a futures position.

    Members:
        ISOLATED: Each position has its own isolated margin; losses are capped
            at the margin allocated to that position.
        CROSSED: All available balance in the account is used as margin; risk
            is shared across positions.
    """

    ISOLATED = 'ISOLATED'
    CROSSED = 'CROSSED'


class FuturesTimeInForce(StringEnum):
    """Time-in-force values for futures orders.

    Extends the Spot ``TimeInForce`` (GTC/IOC/FOK) with futures-only values.

    Members:
        GTC: Good Till Cancelled.
        IOC: Immediate Or Cancel.
        FOK: Fill Or Kill.
        GTX: Good Till Crossing (post-only; rejected if it would immediately match).
        GTD: Good Till Date (expires at ``goodTillDate``).
        RPI: Retail Price Improvement (for RPI orders).
    """

    GTC = 'GTC'
    IOC = 'IOC'
    FOK = 'FOK'
    GTX = 'GTX'
    GTD = 'GTD'
    RPI = 'RPI'
