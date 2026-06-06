"""Per-stream payload findings and shared depth helpers for futures streams.

This module is the single canonical place to look up the per-stream payload
findings that drove the shape of the shared bases.  It also hosts the tiny
depth-parameter validation helpers reused by partial-depth and diff-depth
processors across both USDⓈ-M and COIN-M markets.

Per developers.binance.com (2026-05) -- payload deltas between UM and CM:

- markPriceUpdate: USDⓈ-M includes ``ap`` (mark-price moving average); COIN-M
  does NOT.  UM extends the shared ``MarkPriceHandlerBase`` accordingly.
- forceOrder: COIN-M nested ``o`` includes ``ps`` (pair); USDⓈ-M does NOT.
  CM extends the shared ``ForceOrderHandlerBase`` accordingly.
- aggTrade: USDⓈ-M payload includes ``nq`` (normal quantity excluding RPI
  trades); COIN-M does NOT.  UM extends the shared ``AggTradeHandlerBase``.
- 24hrMiniTicker / 24hrTicker / bookTicker: COIN-M payloads include ``ps``
  (pair) alongside the instrument symbol ``s``; USDⓈ-M does NOT.  CM
  provides extended handler subclasses for these streams.
- 24hrTicker: per docs the futures payload does NOT include the Spot-only
  fields ``x`` (first trade price), ``b``/``B`` (best bid), or ``a``/``A``
  (best ask).  Best bid/ask are delivered via the separate book-ticker
  stream on futures.
- bookTicker: payload includes ``e='bookTicker'`` on both UM and CM (since
  late 2022); processor dispatch routes by stream-name suffix to
  disambiguate the per-symbol ``@bookTicker`` and all-market ``!bookTicker``
  streams that share the same event type.
- compositeIndex: UM-only.  Payload top-level ``C`` is the composition
  method label; ``c`` is the composition list.  The list is exposed as compact
  JSON in the ``composition`` string column.
- tradingSession: UM-only.  ``S`` = session type, ``t`` = session start time
  (ms), ``T`` = session end time (ms) -- the earlier SDK comment treating
  ``T`` as an open/close string was wrong per docs.
- contractInfo: UM + CM.  Includes ``bks`` (brackets) -- now exposed as compact
  JSON in the ``brackets`` string column.
- partial depth (``@depth<N>[@speed]``): payload includes ``lastUpdateId``;
  the handler now returns ``(last_update_id, bids_df, asks_df)``.
- kline / continuousKline / indexPriceKline / markPriceKline: futures klines
  start at the ``1m`` interval; ``1s`` is Spot-only.
- depth update speeds: futures partial/diff depth supports 100ms, 250ms
  (default) and 500ms; Spot uses 100ms/1000ms instead.
- forceOrder all-market: ``!forceOrder@arr`` -- each element matches the
  per-symbol ``forceOrder`` nested ``o`` shape.  CM elements include ``ps``.
- markPrice all-market: ``!markPrice@arr[@1s]`` -- each element matches the
  per-symbol ``markPriceUpdate`` shape.  UM elements include ``ap``.

The UM and CM modules provide market-specific column maps via subclasses
where the docs diverge, and reuse the shared ``_receive`` flattening logic
where they agree.

Stream docs:
- UM Mark Price: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream
- CM Mark Price: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Mark-Price-Stream
- UM Force Order: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
- CM Force Order: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Liquidation-Order-Streams
- UM Agg Trade: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
- CM Agg Trade: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
- UM Kline: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
- CM Kline: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
- UM Continuous Kline: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
- CM Continuous Kline: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Continuous-Contract-Kline-Candlestick-Streams
- UM Mini Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Mini-Ticker-Stream
- CM Mini Ticker: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Mini-Ticker-Stream
- UM Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
- CM Ticker: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
- UM Book Ticker: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
- CM Book Ticker: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
- UM Partial Depth: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
- CM Partial Depth: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
- UM Diff Depth: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- CM Diff Depth: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- UM All Market Tickers: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
- CM All Market Tickers: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
- UM All Book Tickers: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
- CM All Book Tickers: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Book-Tickers-Stream
- UM All Force Orders: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
- CM All Force Orders: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
- UM Contract Info: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream
- CM Contract Info: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Contract-Info-Stream
- UM Composite Index: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Composite-Index-Symbol-Information-Streams
- UM Trading Session: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trading-Session-Stream
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
