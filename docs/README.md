# binance-sdk

`binance-sdk` is an unofficial async Binance SDK for Python 3.9+, now multi-market (Spot + USDⓈ-M Futures). It:

- Routes every Spot request/response call over the **Binance WebSocket API** (`wss://ws-api.binance.com`) for low latency, while keeping a generic `get`/`post`/`put`/`delete` REST escape hatch for arbitrary endpoints.
- Provides a separate **USDⓈ-M Futures** client (`UMFuturesClient`) for REST market-data (open interest, funding rates, mark price) and WebSocket streams (mark price, liquidations).
- Returns `StockDataFrame` (from `stock-pandas`) for stream payloads with renamed columns.
- Uses a first-class `Credentials` object that can be shared across market clients.
- Based on Python `async`/`await`.
- Manages the order book for you (via `OrderBookHandlerBase`), handling WebSocket reconnection and message losses automatically.
- Supports changing API endpoints (e.g. for faster regional hosts).

> **Prices and quantities must be strings.** The SDK rejects `float` params at the API boundary because Python's `str(float)` can produce scientific notation (`1e-08`) or imprecise decimal representations that silently corrupt price/quantity fields. Pass strings (e.g. `price='0.00000001'`) or format with `Decimal`.

## Install

```sh
pip install binance-sdk
```

## Credentials

`Credentials` is a standalone object that holds an API key plus optional signing material. A single instance may be shared across multiple market clients:

```python
from binance import Credentials, SpotClient, UMFuturesClient

# HMAC (deprecated by Binance; prefer asymmetric keys):
creds = Credentials(api_key='KEY', api_secret='SECRET')

# Asymmetric key (recommended — Ed25519 enables session.logon for zero
# per-request signing overhead):
creds = Credentials(api_key='KEY', private_key='/path/to/ed25519.pem')
# or: private_key=<PEM string or bytes>
# optional: private_key_pass=<bytes> for encrypted PEM

# Share the same creds across markets, or pass different creds per market,
# or pass no creds for public-data-only access:
spot = SpotClient(creds)
um   = UMFuturesClient(creds)   # same key pair; credentials are optional here
um_public = UMFuturesClient()   # no creds — public market data only
```

## SpotClient

`SpotClient` provides the full Spot trading surface over the Binance WebSocket API.

### Basic usage

```python
import asyncio
from binance import Credentials, SpotClient

creds = Credentials(api_key='KEY', api_secret='SECRET')
spot = SpotClient(creds)

async def main():
    print(await spot.get_exchange_info())

asyncio.run(main())
```

### SpotClient(**kwargs)

All arguments are keyword arguments; the first positional argument is an optional `Credentials`:

- **credentials?** `Credentials=None` — API credentials. `None` creates an unauthenticated client (public endpoints only).
- **request_params?** `dict=None` — global request params for aiohttp (REST escape hatch).
- **rest_host?** `str` — REST host for the generic escape hatch. Defaults to `'https://api.binance.com'`.
- **stream_host?** `str` — host for the market-data stream connection. Defaults to `'wss://stream.binance.com'`.
- **ws_api_host?** `str` — host for the shared WS-API connection. Defaults to `'wss://ws-api.binance.com/ws-api/v3'`.
- **stream_retry_policy?** `RetryPolicy` — retry policy for WebSocket streams. See [RetryPolicy](#retrypolicy).
- **stream_timeout?** `int=30` — seconds of stream silence before the SDK pings to probe the connection.
- **rate_limit_guard?** `bool=True` — when `True`, proactively throttle requests. When `False`, usage is still tracked but requests are never delayed.
- **rate_limiter?** `RateLimiter=None` — inject a shared `RateLimiter` so multiple clients on the same IP share one IP-level pool. Never share a limiter across markets.
- **request_timeout?** `float=10` — total seconds before an aiohttp REST request is abandoned.
- **recv_window?** `int=None` — default `recvWindow` (ms) for signed WS-API requests. Clamped to 60000. `None` uses Binance's server-side default (5000 ms).
- **time_unit?** `str=None` — WS-API timestamp unit. `None` or `'millisecond'` keeps ms default; `'microsecond'` opts into microsecond precision.

### WS-API request methods

Every method is an `await`-able coroutine. All take keyword arguments matching the [Binance WebSocket API](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api) parameters and return the parsed `result`.

General / market-data (public, no credentials):

- `ping()` / `get_server_time()` / `get_exchange_info()`
- `get_orderbook(**kwargs)` / `get_klines(**kwargs)` / `get_ui_klines(**kwargs)` / `get_average_price(**kwargs)`
- `get_recent_trades(**kwargs)` / `get_historical_trades(**kwargs)` / `get_aggregate_trades(**kwargs)`
- `get_historical_block_trades(**kwargs)` — historical block trades for a symbol
- `get_ticker(**kwargs)` / `get_ticker_price(**kwargs)` / `get_orderbook_ticker(**kwargs)`
- `get_rolling_window_ticker(**kwargs)` — rolling-window price statistics (1m–7d window)
- `get_trading_day_ticker(**kwargs)` — trading-day price statistics for a symbol or list
- `get_execution_rules(**kwargs)` — per-symbol execution rules (price bands, order limits)
- `get_reference_price(**kwargs)` — current reference price for a symbol
- `get_reference_price_calculation(**kwargs)` — methodology used to compute the reference price

Account (signed):

- `get_account(**kwargs)` / `get_trades(**kwargs)`
- `get_commission(**kwargs)` — current account commission rates
- `get_order_rate_limit(**kwargs)` — current unfilled order count per order rate limit
- `get_prevented_matches(**kwargs)` — orders expired by self-trade prevention
- `get_allocations(**kwargs)` — allocations resulting from SOR order placement
- `get_order_amendments(**kwargs)` — amendment history for a single order
- `get_my_filters(**kwargs)` — account-relevant filters including MAX_ASSET limits

Trading (signed):

- `create_order(**kwargs)` / `create_test_order(**kwargs)`
- `get_order(**kwargs)` / `get_open_orders(**kwargs)` / `get_all_orders(**kwargs)`
- `cancel_order(**kwargs)` / `cancel_all_orders(**kwargs)`
- `cancel_replace_order(**kwargs)` — cancel an order and place a new one atomically
- `amend_order(**kwargs)` — reduce an open order's quantity, keeping its priority
- `create_sor_order(**kwargs)` / `create_test_sor_order(**kwargs)` — Smart Order Routing (live and test)
- `create_oco(**kwargs)` / `create_oto(**kwargs)` / `create_otoco(**kwargs)`
- `create_opo(**kwargs)` — One-Pending-the-Other order list
- `create_opoco(**kwargs)` — One-Pending-One-Cancels-the-Other order list
- `cancel_oco(**kwargs)` / `get_oco(**kwargs)` / `get_all_oco(**kwargs)` / `get_open_oco(**kwargs)`

Session management (the SDK normally manages the session automatically):

- `get_session_status()` — reports which API key is authorizing the current connection
- `get_session_subscriptions()` — lists active user-data subscriptions on the connection
- `session_logout()` — sends `session.logout` and clears the local auth flag

```python
from binance import (
    Credentials, SpotClient,
    OrderSide, OrderType, TimeInForce
)

creds = Credentials(api_key='KEY', private_key='/path/to/ed25519.pem')
spot = SpotClient(creds)

# All arguments are keyword arguments.
await spot.create_order(
    symbol='BTCUSDT',
    side=OrderSide.BUY,
    type=OrderType.LIMIT,
    timeInForce=TimeInForce.GTC,
    # Pass prices and quantities as strings, not Python floats.
    quantity='10',
    price='7000.1'
)
```

### await spot.sync_time() -> int

Syncs the local clock offset against Binance server time by issuing `get_server_time()` and storing `server_time - local_time` (ms). This offset is added to the `timestamp` of every signed request, preventing `-1021` rejections from clock drift. Called automatically before the first signed request and on each `-1021` response. Returns the new offset in milliseconds.

### spot.rate_limit_snapshot() -> RateLimitSnapshot

Returns a read-only, local (no network) `RateLimitSnapshot`. See [Rate Limits](#rate-limits).

### await spot.get(uri, **kwargs) / post / put / delete

Generic REST escape hatch for arbitrary endpoints not yet covered by the named WS-API methods (e.g. `/sapi/` paths). Raises `RateLimitException` (HTTP 429), `IPBannedException` (HTTP 418), or `StatusException` (other non-2xx).

### await spot.subscribe(subtype, *subtype_params) / subscribe(*subscriptions)

Subscribe to market-data or user-data WebSocket streams. See [SubType (Spot)](#subtype-spot).

### spot.handler(*handlers) -> self

Register message handlers for streams. See [Handling messages](#handling-messages).

### spot.start() / spot.stop()

Start or stop receiving stream messages.

### await spot.close(code=4999)

Close all stream connections and the shared REST session.

## UMFuturesClient (USDⓈ-M Futures)

`UMFuturesClient` provides read-only market-data access for USDⓈ-M Perpetual Futures. This release covers market data only — no order placement or account endpoints. Credentials are optional (all endpoints are public).

```python
from binance import UMFuturesClient

um = UMFuturesClient()   # no creds needed for public data

oi  = await um.get_open_interest(symbol='BTCUSDT')
fr  = await um.get_funding_rate(symbol='BTCUSDT', limit=10)
mp  = await um.get_premium_index(symbol='BTCUSDT')
```

### REST market-data methods

All are public (`SecurityType.NONE`), no credentials required:

- `get_open_interest(**kwargs)` — present open interest for a symbol (weight 1). Required: `symbol`.
- `get_open_interest_hist(**kwargs)` — historical open interest stats (weight 1). Required: `symbol`, `period` (one of `'5m'`/`'15m'`/`'30m'`/`'1h'`/`'2h'`/`'4h'`/`'6h'`/`'12h'`/`'1d'`). Optional: `limit` (default 30, max 500), `startTime`, `endTime`.
- `get_funding_rate(**kwargs)` — historical funding rate data (weight 1). Optional: `symbol`, `startTime`, `endTime`, `limit` (default 100, max 1000).
- `get_funding_info(**kwargs)` — funding rate cap/floor and interval for all symbols (weight 1).
- `get_premium_index(**kwargs)` — mark price, index price, and funding rate (weight 1 with `symbol`; 10 for all symbols). Optional: `symbol`.

### Futures streams: SubType.MARK_PRICE and SubType.FORCE_ORDER

```python
from binance import (
    UMFuturesClient, SubType,
    MarkPriceHandlerBase, ForceOrderHandlerBase
)

um = UMFuturesClient()

# Mark price stream — <symbol>@markPrice (every 3 s by default)
# Pass '1s' as second param for the 1-second variant.
class MyMarkPrice(MarkPriceHandlerBase):
    def receive(self, payload):
        df = super().receive(payload)
        # df columns: type, event_time, symbol, mark_price, mark_price_avg,
        #             index_price, est_settle_price, funding_rate, next_funding_time
        print(df['mark_price'], df['funding_rate'])

um.handler(MyMarkPrice())
await um.subscribe(SubType.MARK_PRICE, 'btcusdt')
# For 1-second updates:
await um.subscribe(SubType.MARK_PRICE, 'btcusdt', '1s')

# Force order (liquidation) stream — <symbol>@forceOrder
class MyForceOrder(ForceOrderHandlerBase):
    def receive(self, payload):
        df = super().receive(payload)
        # df columns: type, event_time, symbol, side, order_type, time_in_force,
        #             orig_quantity, price, avg_price, order_status,
        #             last_filled_qty, acc_filled_qty, trade_time
        print(df['symbol'], df['price'])

um.handler(MyForceOrder())
await um.subscribe(SubType.FORCE_ORDER, 'btcusdt')
```

`UMFuturesClient` accepts the same constructor kwargs as `SpotClient` (see above), plus an optional `Credentials` first argument.

## Handling messages

`binance-sdk` provides handler-based APIs for all WebSocket messages.

```python
from binance import SpotClient, Credentials, TickerHandlerBase, SubType

creds = Credentials(api_key='KEY')
spot = SpotClient(creds)

async def main():
    class TickerPrinter(TickerHandlerBase):
        async def receive(self, payload):
            # `ticker_df` is a StockDataFrame with columns renamed
            ticker_df = super().receive(payload)
            print(ticker_df)

    spot.handler(TickerPrinter())
    await spot.subscribe(SubType.TICKER, 'BTCUSDT')

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.run_forever()
```

### Subscribe to more symbol pairs and types

```python
# Subscribe to bnbusdt@aggTrade, bnbusdt@depth, bnbbtc@aggTrade, bnbbtc@depth
await spot.subscribe(
    [SubType.AGG_TRADE, SubType.ORDER_BOOK],
    ['BNB_USDT', 'BNBBTC']
)
```

### Subscribe to user streams

```python
# Credentials must include api_key; asymmetric key enables session.logon.
await spot.subscribe(SubType.USER)
```

### Subscribe to handler exceptions

```python
from binance import HandlerExceptionHandlerBase, KlineHandlerBase

class KlineHandler(KlineHandlerBase):
    def receive(self, payload):
        raise RuntimeError('this will ruin my day')

class HandlerExceptionHandler(HandlerExceptionHandlerBase):
    async def receive(self, exception):
        super().receive(exception)   # prints the stack
        await send_to_monitor(exception)

spot.handler(KlineHandler())
spot.handler(HandlerExceptionHandler())

# Or just print error stacks with the default:
spot.handler(HandlerExceptionHandlerBase())
```

### Handle stream-control errors (resubscribe / logon failures)

After a WebSocket reconnect the SDK automatically replays all subscriptions. If that replay (or `session.logon`) fails, the SDK logs it, calls `receive` on every registered `StreamErrorHandlerBase`, and schedules a `recycle()` on the affected stream.

```python
from binance import StreamErrorHandlerBase

class MyStreamErrors(StreamErrorHandlerBase):
    async def receive(self, error):
        # error.stream      -> 'data' | 'user'
        # error.phase       -> 'resubscribe' | 'logon'
        # error.exception   -> the underlying exception
        # error.recovering  -> True (SDK is recycling the stream)
        await alert_ops_team(
            f"Stream {error.stream!r} {error.phase} failed: {error.exception}"
        )

spot.handler(MyStreamErrors())
```

# APIs

## SubType (Spot)

Spot `SpotClient` streams. Pass as the first argument to `client.subscribe(subtype, ...)`.

### `SubType` with parameters `symbol` and `interval`

- `SubType.KLINE`
- `SubType.KLINE_UTC8`

And `interval` should be one of the `TimeFrame` enumerables.

### `SubType`s with a param `symbol`

- `SubType.TRADE`
- `SubType.AGG_TRADE`
- `SubType.BLOCK_TRADE`
- `SubType.REFERENCE_PRICE`
- `SubType.BOOK_TICKER`
- `SubType.AVG_PRICE`
- `SubType.MINI_TICKER`
- `SubType.TICKER`
- `SubType.ORDER_BOOK`

### `SubType`s with params `symbol` and `level`

- `SubType.PARTIAL_ORDER_BOOK` (`level` should be one of `5`, `10`, `20`)

### `SubType`s with an optional param `updateInterval=1000` (ms)

- `SubType.ORDER_BOOK` (`1000` or `100`)
- `SubType.PARTIAL_ORDER_BOOK` (`symbol`, `level`, optional `interval`: `1000` or `100`)

### `SubType`s with an optional param `window=TimeFrame.H1`

- `SubType.WINDOW_TICKER` (with `symbol`; one of `TimeFrame.H1/H4/D1`)
- `SubType.ALL_MARKET_WINDOW_TICKERS` (one of `TimeFrame.H1/H4/D1`)

### `Subtype` with no param

- `SubType.ALL_MARKET_MINI_TICKERS`
- `SubType.USER`

### Full multi-subscribe example

```python
from binance import SubType, TimeFrame

await spot.subscribe(SubType.TICKER, 'BNBUSDT')
await spot.subscribe(SubType.BOOK_TICKER, 'BNBUSDT')
await spot.subscribe(SubType.AVG_PRICE, 'BNBUSDT')
await spot.subscribe(SubType.WINDOW_TICKER, 'BNBUSDT', TimeFrame.H1)
await spot.subscribe(SubType.PARTIAL_ORDER_BOOK, 'BNBUSDT', 20)
await spot.subscribe(SubType.ALL_MARKET_MINI_TICKERS)
await spot.subscribe(SubType.ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4)

# Multiple at once
await spot.subscribe(
    (SubType.KLINE, 'BTC_USDT', TimeFrame.D1),
    (SubType.KLINE_UTC8, 'BTC_USDT', TimeFrame.D1),
    (SubType.TICKER, 'BNBUSDT'),
    (
        [SubType.ORDER_BOOK, SubType.TRADE],
        ['BNBUSDT', 'BTCUSDT']
    ),
    (SubType.ALL_MARKET_MINI_TICKERS,)  # <-- note the trailing comma
)
```

Spot stream handler bases:

- `TradeHandlerBase`
- `AggTradeHandlerBase`
- `BlockTradeHandlerBase`
- `ReferencePriceHandlerBase`
- `BookTickerHandlerBase`
- `PartialOrderBookHandlerBase`
- `AvgPriceHandlerBase`
- `WindowTickerHandlerBase`
- `OrderBookHandlerBase`
- `KlineHandlerBase`
- `MiniTickerHandlerBase`
- `TickerHandlerBase`
- `AllMarketMiniTickersHandlerBase`
- `AllMarketWindowTickersHandlerBase`
- `AccountPositionHandlerBase`
- `BalanceUpdateHandlerBase`
- `OrderUpdateHandlerBase`
- `OrderListStatusHandlerBase`
- `ExternalLockUpdateHandlerBase`
- `EventStreamTerminatedHandlerBase`
- `HandlerExceptionHandlerBase` — handles exceptions raised inside other handlers
- `StreamErrorHandlerBase` — handles resubscribe/logon failures after reconnect

## SubType (USDⓈ-M Futures)

Futures streams are only available on `UMFuturesClient`.

### `SubType.MARK_PRICE` — mark-price and funding-rate updates

- Required param: `symbol` (lowercase, e.g. `'btcusdt'`)
- Optional second param: `'1s'` for the 1-second update variant (default is 3 s)
- Wire stream: `<symbol>@markPrice` / `<symbol>@markPrice@1s`
- Handler base: `MarkPriceHandlerBase`
- Payload columns: `type`, `event_time`, `symbol`, `mark_price`, `mark_price_avg`, `index_price`, `est_settle_price`, `funding_rate`, `next_funding_time`

### `SubType.FORCE_ORDER` — liquidation order events

- Required param: `symbol` (lowercase, e.g. `'btcusdt'`)
- Wire stream: `<symbol>@forceOrder`
- Handler base: `ForceOrderHandlerBase`
- Payload columns: `type`, `event_time`, `symbol`, `side`, `order_type`, `time_in_force`, `orig_quantity`, `price`, `avg_price`, `order_status`, `last_filled_qty`, `acc_filled_qty`, `trade_time`

Possible subscribe exceptions (both markets):

- `InvalidSubParamsException`
- `UnsupportedSubTypeException`
- `InvalidSubTypeParamException`
- `StreamAbandonedException`

## RetryPolicy

Retry policy determines what to do after a stream connection failure.

```python
abandon, delay = stream_retry_policy(info)

# `info.fails` — how many times the stream has failed (1 on first failure)
# `info.exception` — the exception that caused the failure
# If abandon is True, the client gives up reconnecting.
# Otherwise, the client waits `delay` seconds before reconnecting.
```

Since `3.2.0` the default policy is a bounded, jittered exponential backoff (≈0.5 s → 30 s, never abandoning).

## Rate Limits

`binance-sdk` tracks and respects [Binance's documented rate limits](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/limits) to avoid the `429` → `418` IP-ban escalation.

**Each client has its own per-market rate limiter.** Spot and Futures rate limits are independent server-side. A shared `RateLimiter` may be injected via the `rate_limiter=` constructor argument only within the same market (never across Spot and Futures).

### The pools

| Pool | Scope | Default budget | On exceed (guard on) |
| --- | --- | --- | --- |
| Request weight | IP | 6000 / 1m (used at 90% → 5400) | sleep until headroom |
| Raw requests | IP | 300000 / 5m | sleep until headroom |
| Orders | account | 100 / 10s **and** 200000 / 1d | **raise** `RateLimitReachedException` |
| WS connections | IP | 290 / 5m | sleep until headroom |
| WS messages | per connection | 5 / 1s | sleep until headroom |
| WS streams | per connection | 1024 (cap) | **raise** `TooManyStreamsException` |

Orders never sleep — delaying an order can be worse than not sending it, so an over-budget order fails fast with `RateLimitReachedException` (carrying `retry_after`) and lets your strategy decide. Usage is **always** accounted (even with the guard off), so monitoring stays accurate.

### WS-API: typed errors from trading/account/market-data calls

- `StreamSubscribeException` — any server-side error (`code`/`msg` fields match the Binance WS-API error object).
- `StreamRateLimitException` (subclass of `StreamSubscribeException`) — rate-limit rejection (code `-1003`, status `429`/`418`); carries `retry_after` in milliseconds.

```python
from binance import StreamSubscribeException, StreamRateLimitException

try:
    await spot.create_order(symbol='BTCUSDT', side='BUY', ...)
except StreamRateLimitException as e:
    await asyncio.sleep(e.retry_after / 1000)
except StreamSubscribeException as e:
    print(f'WS-API error {e.code}: {e.msg}')
```

### REST escape hatch: typed errors

```python
from binance import RateLimitException, IPBannedException, StatusException

try:
    await spot.get('https://api.binance.com/sapi/v1/...')
except IPBannedException as e:
    await asyncio.sleep(e.retry_after)
except RateLimitException as e:
    await asyncio.sleep(e.retry_after)
except StatusException as e:
    print(f'HTTP {e.status_code}: {e.response}')
```

Both `RateLimitException` and `IPBannedException` subclass `StatusException`. The client **never auto-retries** — it surfaces `retry_after` and lets your strategy decide.

### REST: used-weight visibility

```python
await spot.get('https://api.binance.com/api/v3/exchangeInfo')

spot.used_weight   # e.g. {'1m': 20}   (from X-MBX-USED-WEIGHT-*)
spot.order_count   # e.g. {'10s': 3}   (from X-MBX-ORDER-COUNT-*)
```

### Monitoring: `client.rate_limit_snapshot()`

`rate_limit_snapshot()` returns a read-only, local (no network) `RateLimitSnapshot`:

```python
snap = spot.rate_limit_snapshot()

snap.max_utilization   # 0.0–1.0+, the busiest pool right now
snap.throttled         # True if anything is queued/sleeping or a retry-after is active
snap.retry_after       # seconds remaining on a 429/418 ban, or None
snap.pending           # total calls currently waiting on a pool

for w in snap.windows:
    print(w.scope, w.type, w.interval, f'{w.used}/{w.limit}', w.utilization, w.source)
    # e.g. ip request_weight 1m 5400/5400 1.0 header
```

A `RateLimitWindow` describes one pool: `scope` (`ip`/`account`/`connection`), `type` (`request_weight`/`raw_requests`/`orders`/`ws_connections`/`ws_messages`/`ws_streams`), `interval` (`1m`, `10s`, …), `used`, `limit` (the effective, safety-adjusted cap), `remaining`, `utilization` (`used/limit`), `pending`, and `source` — `header` when reconciled from an authoritative Binance header, otherwise `client`. `RateLimitSnapshot` exposes `windows`, `pending`, `retry_after`, `throttled`, `at` (epoch seconds), and `max_utilization`. Both types are importable from `binance`.

### WebSocket: connection, message, and stream limits

- **Connections** are gated to stay under Binance's 300 attempts / 5 min / IP limit (default cap 290/5 min).
- **Outgoing messages** are limited to 5/second (including subscribe/unsubscribe and ping/pong).
- **Streams per connection** are capped at 1024; exceeding it raises `TooManyStreamsException` (carrying `requested`/`limit`).
- **`serverShutdown`** events trigger a proactive reconnect ~10 min before Binance's 24 h forced disconnect.

## OrderBookHandlerBase(**kwargs)

- **kwargs**
  - **limit?** `int=1000` — snapshot depth (default 1000, max 5000)
  - **retry_policy?** `RetryPolicy`

`OrderBookHandlerBase` maintains the order book according to [the official documentation](https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly), handling reconnection and message-loss automatically.

```python
async def main():
    spot = SpotClient(Credentials(api_key='KEY'))
    handler = OrderBookHandlerBase()

    spot.handler(handler)
    await spot.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')

    # Get the reference of the managed OrderBook for 'BTCUSDT'
    orderbook = handler.orderbook('BTCUSDT')

    while True:
        try:
            await orderbook.updated()
        except Exception as e:
            print('exception occurred')
        else:
            await doSomethingWith(orderbook.asks, orderbook.bids)
```

`handler.orderbook(symbol, limit=...)` returns (and optionally configures the snapshot depth for) the `OrderBook` object for a symbol. Per-symbol override defaults to the handler-level `limit`.

## OrderBook(symbol, **kwargs)

- **symbol** `str`
- **kwargs**
  - **limit?** `int=1000`
  - **client** `SpotClient=None`
  - **retry_policy?** `RetryPolicy`

```python
async def main():
    orderbook = OrderBook('BTCUSDT', client=spot)
    await orderbook.updated()
    print(orderbook.asks)
```

### orderbook.set_client(client) / set_limit(limit) / set_retry_policy(retry_policy)

Configure the orderbook after construction.

### property `orderbook.ready` -> bool

`False` while a new snapshot is being fetched after a detected gap.

### property `orderbook.asks` / `orderbook.bids` -> list

Ask and bid levels in ascending order.

### await orderbook.updated()

Wait for the next update. Raises an `aiohttp` exception if the snapshot fetch is abandoned by the `retry_policy`.

## Upgrading from v3.x

**Breaking change:** `from binance import Client` has been removed. Replace it with `SpotClient` + `Credentials`:

```python
# Before (v3.x):
from binance import Client
client = Client(api_key='KEY', api_secret='SECRET')

# After (v4.0):
from binance import Credentials, SpotClient
creds = Credentials(api_key='KEY', api_secret='SECRET')
client = SpotClient(creds)
```

Constructor kwargs `api_key`, `api_secret`, `private_key`, and `private_key_pass` have moved into `Credentials`. All other constructor kwargs (`stream_retry_policy`, `rate_limit_guard`, `rate_limiter`, etc.) remain on the client constructor, unchanged.

## License

[MIT](../LICENSE)
