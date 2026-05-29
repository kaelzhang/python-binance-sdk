"""Verified findings and shared depth helpers for futures market streams.

This module is the single canonical place to look up the per-stream payload
findings that drove the shape of the shared bases.  It also hosts the tiny
depth-parameter validation helpers reused by partial-depth and diff-depth
processors across both USDⓈ-M and COIN-M markets.

Key verified findings (2026-05-25):
- Both markets share the *same* ``markPriceUpdate`` event type.
- Both markets share the *same* ``forceOrder`` event type and nested ``o`` structure.
- USDⓈ-M markPrice payload includes ``ap`` (mark price moving average); COIN-M does NOT.
- COIN-M forceOrder nested ``o`` includes ``ps`` (pair); USDⓈ-M does NOT.

Key verified findings on shared stream schemas (2026-05-26):
- aggTrade: Futures adds field ``X`` (ignored/placeholder); does NOT have Spot-specific
  ``b``/``a`` (buyer/seller order id).  UM and CM share identical futures aggTrade schema.
- kline: Futures kline nested ``k`` object is identical in structure to Spot kline.
  However, futures klines include an extra ``ps`` (pair/symbol) field in UM;
  the shared base uses only the Spot-common fields (same as Spot ``KlineHandlerBase``).
- miniTicker / ticker: Futures 24hrMiniTicker and 24hrTicker payloads are identical
  in field structure to Spot equivalents.  Shared bases reuse Spot column maps.
- bookTicker: Futures bookTicker payloads have NO ``e`` event field (stream-name routing
  required).  Identical schema for UM and CM.
- depth (partial + diff): Futures depth stream payloads are structurally identical to
  Spot depth streams.  Update speed options differ (UM/CM: 100ms/500ms vs Spot 100ms/1000ms).
- continuousKline: Futures-specific stream; nested ``k`` object is identical to kline ``k``
  but the outer event has ``ps`` (pair) and ``ct`` (contract type) instead of ``s`` (symbol).
  Shared between UM and CM.
- contractInfo: Outer payload; same across UM and CM (contract spec change events).
- forceOrder all-market: ``!forceOrder@arr`` array; each element has the same nested ``o``
  structure as per-symbol forceOrder.  Shared between UM and CM.
- markPrice all-market: ``!markPrice@arr[@1s]`` array; each element is a markPriceUpdate dict.
  UM elements include ``ap``; CM elements do not -- handled via market-specific subclasses.

Therefore the shared base:
- ``MarkPriceHandlerBase``: exposes only the common fields; UM adds ``ap``, CM adds nothing extra.
- ``ForceOrderHandlerBase``: exposes only the common nested ``o`` fields; CM adds ``ps``.

The UM and CM modules provide *market-specific* column maps (via subclasses/overrides) while
inheriting the common ``_receive`` flattening logic from ``ForceOrderHandlerBase``.

Stream docs:
- UM Mark Price: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
- CM Mark Price: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Stream
- UM Force Order: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
- CM Force Order: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Liquidation-Order-Streams
- UM Agg Trade: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
- CM Agg Trade: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
- UM Kline: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
- CM Kline: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
- UM Continuous Kline: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
- CM Continuous Kline: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
- UM Mini Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Mini-Ticker-Stream
- UM Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
- UM Book Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
- UM Partial Depth: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
- UM Diff Depth: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- UM All Market Tickers: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
- UM All Book Tickers: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
- UM All Force Orders: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
- UM Contract Info: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream
"""

from binance.core.common.exceptions import InvalidSubTypeParamException


# ---------------------------------------------------------------------------
# Futures depth parameter constants and helpers
# ---------------------------------------------------------------------------

FUTURES_DEPTH_LEVELS = (5, 10, 20)
# Per developers.binance.com (UM + CM partial-book and diff-book depth
# streams), the allowed update speeds are 100ms, 250ms (default) and 500ms.
FUTURES_DEPTH_SPEEDS = (100, 250, 500)


def _get_futures_depth_level(t, args, default=20):
    if len(args) == 0:
        return default

    level = args[0]

    if type(level) is not int:
        raise InvalidSubTypeParamException(
            t, 'level', '`int` expected but got `%s`' % level)

    if level not in FUTURES_DEPTH_LEVELS:
        raise InvalidSubTypeParamException(
            t, 'level',
            '`level` should be one of %s but got `%s`'
            % (FUTURES_DEPTH_LEVELS, level)
        )

    return level


def _get_futures_depth_speed(t, args):
    """Return the speed int (one of 100 / 250 / 500) or None if not provided."""
    if len(args) == 0:
        return None

    speed = args[0]

    if type(speed) is not int:
        raise InvalidSubTypeParamException(
            t, 'speed', '`int` expected but got `%s`' % speed)

    if speed not in FUTURES_DEPTH_SPEEDS:
        raise InvalidSubTypeParamException(
            t, 'speed',
            '`speed` should be one of %s but got `%s`'
            % (FUTURES_DEPTH_SPEEDS, speed)
        )

    return speed
