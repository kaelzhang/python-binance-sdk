# binance-sdk

`binance-sdk` is an unofficial Binance SDK for Python 3.9+, which:

- Based on [Binance Official API Docs v3](https://github.com/binance/binance-official-api-docs).
- Routes every request/response call (general, market-data, account and trading) over the **Binance WebSocket API** (`wss://ws-api.binance.com`) for low latency, while keeping a generic `client.get/post/put/delete` REST escape hatch for arbitrary endpoints.
- Uses **TWO** WebSocket connections: a market-data **stream** connection (`subscribe(...)`) and a shared **WS-API** connection (request/response calls + user-data-stream subscription).
- Returns `StockDataFrame` (from `stock-pandas`) for stream payloads with renamed columns.
- Based on Python `async`/`await`
- Manages the order book for you (handled by `OrderBookHandlerBase`), so that you need not to worry about websocket reconnection and message losses. For details, see the section [`OrderBookHandlerBase`](#orderbookhandlerbasekwargs)
- Supports changing API endpoints, so that you can use faster API hosts.

> **Prices and quantities must be strings.** The SDK rejects `float` params at the API boundary because Python's `str(float)` can produce scientific notation (`1e-08`) or imprecise decimal representations that silently corrupt price/quantity fields. Pass strings (e.g. `price='0.00000001'`) or format with `Decimal`.

## Install

```sh
pip install binance-sdk
```

## Basic Usage

```py
#!/usr/bin/env python

import asyncio
from binance import Client

client = Client()

async def main():
    print(await client.get_exchange_info())

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

## Handling messages

Binance-sdk provides handler-based APIs to handle all websocket messages, and you are able to not worry about websockets.

```py
#!/usr/bin/env python

from binance import Client, TickerHandlerBase, SubType

client = Client(api_key)

async def main():
    # Implement your own TickerHandler.
    class TickerPrinter(TickerHandlerBase):
        async def receive(self, payload):
            """The function to receive ticker streams.
            The function could either be sync or async

            Args:
                payload (dict): the raw stream payload which is
                message['data'] of the original stream message
            """

            # `ticker_df` is a StockDataFrame with columns renamed
            ticker_df = super().receive(payload)

            # Just print the ticker
            print(ticker_df)

    # Register the handler for `SubType.TICKER`
    client.handler(TickerPrinter())

    # Subscribe to ticker change for symbol BTCUSDT
    await client.subscribe(SubType.TICKER, 'BTCUSDT')

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

# Run the loop forever to keep receiving messages
loop.run_forever()

# It prints a StockDataFrame for each message

#    type        event_time     symbol   open            high            low            ...
# 0  24hrTicker  1581597461196  BTCUSDT  10328.26000000  10491.00000000  10080.00000000 ...

# ...(to be continued)
```

### Subscribe to more symbol pairs and types

```py
# This will subscribe to
# - bnbusdt@aggTrade
# - bnbusdt@depth
# - bnbbtc@aggTrade
# - bnbbtc@depth
await client.subscribe(
    # We could also subscribe multiple types
    #   for both `BNBUSDT` and 'BNBBTC'
    [
        SubType.AGG_TRADE,
        SubType.ORDER_BOOK
    ],
    # We could subscribe more than one symbol pairs at a time
    [
        # Which is equivalent to `BNBUSDT`
        'BNB_USDT',
        'BNBBTC'
    ]
)
```

And since we subscribe to **THREE** new types of messages, we need to set the handlers each of which should `isinstance()` of one of
- `TradeHandlerBase`
- `AggTradeHandlerBase`
- `BlockTradeHandlerBase`
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
- `HandlerExceptionHandlerBase` a special handler to handle stream exceptions
- `StreamErrorHandlerBase` a special handler for post-reconnect resubscribe and `session.logon` failures; receives a `StreamError` with `stream`/`phase`/`exception`/`recovering` fields

```py
client.handler(MyTradeHandler(), MyOrderBookHandler(), MyKlineHandler())
```

### Subscribe to user streams

```py
# Before subscribing to the user stream, provide credentials via the constructor.
# HMAC (deprecated by Binance):
client = Client(api_key=api_key, api_secret=api_secret)

# Asymmetric key (recommended — Ed25519 enables session.logon for zero
# per-request signing overhead):
client = Client(api_key=api_key, private_key='/path/to/ed25519.pem')

# binance-sdk handles user stream subscription internally via the
# WebSocket API `userDataStream.subscribe.signature` method.
# With an Ed25519 private_key the WS-API connection also issues
# session.logon automatically after (re)connect.
await client.subscribe(SubType.USER)
```

### Subscribe to handler exceptions

`Binance-sdk` receives stream messages in background tasks, so sometimes it is difficult to detect the exceptions raised in `receive` function of user handlers.

Fortunately, we could use `HandlerExceptionHandlerBase`

```py
from binance import (
    HandlerExceptionHandlerBase,
    KlineHandlerBase
)

class KlineHandler(KlineHandlerBase):
    def receive(self, payload):
        raise RuntimeError('this will ruin my day')

class HandlerExceptionHandler(HandlerExceptionHandlerBase):
    async def receive(self, exception):
        # By calling `super().receive(exception)`,
        # it will print the error stack.
        super().receive(exception)

        await send_to_monitor(exception)

client.handler(KlineHandler())
client.handler(HandlerExceptionHandler())
```

If you just want to print error stacks, we could:

```py
client.handler(HandlerExceptionHandlerBase())
```

### Handle stream-control errors (resubscribe / logon failures)

After a WebSocket reconnect, the SDK automatically replays all subscriptions. If
that replay (or the WS-API ``session.logon``) fails, the SDK:

1. logs the failure at `ERROR` level,
2. calls `receive` on every registered `StreamErrorHandlerBase`, and
3. schedules a `recycle()` on the affected stream so aioretry starts a fresh
   reconnect cycle.

```py
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

client.handler(MyStreamErrors())
```

# APIs

## Client(**kwargs)

All arguments of the constructor Client are keyword arguments and all optional.

- **api_key?** `str=None` Binance API key
- **api_secret?** `str=None` Binance API secret for HMAC-SHA256 signing (deprecated by Binance; prefer asymmetric keys via `private_key`)
- **private_key?** `str|bytes=None` Ed25519 or RSA PEM private key (PEM content or file path). When provided, used for request signing instead of `api_secret`. Binance recommends Ed25519 (fastest) or RSA over the deprecated HMAC keys. With an Ed25519 key the WS-API connection issues `session.logon` automatically after each (re)connect, allowing subsequent signed requests to omit `apiKey`/`signature` entirely.
- **private_key_pass?** `str|bytes=None` password to decrypt an encrypted PEM private key; `None` for unencrypted keys
- **request_params?** `dict=None` global request params for aiohttp (REST escape hatch)
- **stream_retry_policy?** `Callable[[int, Exception], Tuple[bool, int, bool]]` retry policy for websocket streams. For details, see [RetryPolicy](#retrypolicy)
- **stream_timeout?** `int=30` seconds of stream silence before the SDK pings to probe a possibly-dead connection
- **rate_limit_guard?** `bool=True` when `True`, the client proactively throttles requests with a client-side weight/raw/order budget to stay under Binance's per-IP and per-account caps. When `False`, usage is still tracked (so monitoring works) but requests are never delayed. See [Rate Limits](#rate-limits).
- **rate_limiter?** `RateLimiter=None` inject a shared `RateLimiter` instance so multiple `Client` objects on the same IP share one IP-level pool. When `None` (default) a private limiter is built from `rate_limit_guard`.
- **request_timeout?** `float=10` total seconds before an aiohttp REST request (via the generic `get`/`post`/... escape hatch) is abandoned.
- **recv_window?** `int=None` default `recvWindow` (ms) injected into every signed WS-API request that does not already supply one. Clamped to at most 60000 ms. `None` uses Binance's server-side default (5000 ms). Can always be overridden per-call by passing `recvWindow=<value>` to any signed method.
- **time_unit?** `str=None` WebSocket-API timestamp unit. `None` (default) or `'millisecond'` keeps Binance's millisecond default; `'microsecond'` (case-insensitive) opts the whole WS-API connection into microsecond-precision timestamps (appends `?timeUnit=MICROSECOND` to the connection URL).
- **api_host?** `str='https://api.binance.com'` REST host for the generic `get`/`post`/`put`/`delete` escape hatch.
- **stream_host?** `str='wss://stream.binance.com'` host for the market-data stream connection (`subscribe(...)`).
- **ws_api_host?** `str='wss://ws-api.binance.com/ws-api/v3'` host for the shared WS-API connection (request/response calls + user-data-stream subscription).

Create a binance client.

Each API method accepts only keyworded arguments (kwargs) and has verbosed Python doc strings (Google style) which you could check out when you are coding.

The following example shows how to create a new order.

```py
from binance import (
    OrderSide,
    OrderType,
    TimeInForce
)

# All arguments are keyword arguments.
await client.create_order(
    symbol='BTCUSDT',

    # Use the built-in enum types to avoid spelling mistakes.
    side=OrderSide.BUY,
    type=OrderType.LIMIT,
    timeInForce=TimeInForce.GTC,

    # IMPORTANT: pass prices and quantities as STRINGS, not Python floats.
    # The SDK rejects float params — str(float) can produce scientific
    # notation ('1e-08') or imprecise decimals that silently corrupt orders.
    quantity='10',
    price='7000.1'
)
```

Every request/response call is sent over Binance's **WebSocket API** (a single
shared connection, opened lazily) rather than REST, while keeping the same
method names and keyword arguments. The public market-data **streams**
(`subscribe(...)`) are a separate push mechanism and are unchanged.

General / market-data (public, no credentials):

- `ping()` / `get_server_time()` / `get_exchange_info()`
- `get_orderbook(**kwargs)` / `get_klines(**kwargs)` / `get_average_price(**kwargs)`
- `get_recent_trades(**kwargs)` / `get_historical_trades(**kwargs)` / `get_aggregate_trades(**kwargs)`
- `get_ticker(**kwargs)` / `get_ticker_price(**kwargs)` / `get_orderbook_ticker(**kwargs)`

Account (signed):

- `get_account(**kwargs)` / `get_trades(**kwargs)`
- `get_commission(**kwargs)` — current account commission rates
- `get_order_rate_limit(**kwargs)` — current unfilled order count per order rate limit
- `get_prevented_matches(**kwargs)` — orders expired by self-trade prevention
- `get_allocations(**kwargs)` — allocations resulting from SOR order placement

Trading (signed):

- `create_order(**kwargs)` / `create_test_order(**kwargs)`
- `get_order(**kwargs)` / `get_open_orders(**kwargs)` / `get_all_orders(**kwargs)`
- `cancel_order(**kwargs)` / `cancel_all_orders(**kwargs)`
- `cancel_replace_order(**kwargs)` — cancel an order and place a new one atomically
- `amend_order(**kwargs)` — reduce an open order's quantity, keeping its priority
- `create_sor_order(**kwargs)` — place an order using Smart Order Routing
- `create_oco(**kwargs)` / `create_oto(**kwargs)` / `create_otoco(**kwargs)`
- `cancel_oco(**kwargs)` / `get_oco(**kwargs)` / `get_all_oco(**kwargs)` / `get_open_oco(**kwargs)`

All take keyword arguments matching the [Binance WebSocket API](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api) parameters and return the parsed `result`.

### await client.sync_time() -> int

Syncs the local clock offset against Binance server time by issuing the WebSocket-API `time` request (`get_server_time()`) and storing `server_time - local_time` (ms). This offset is added to the `timestamp` of every signed request, preventing `-1021` ("Timestamp for this request is outside of the recvWindow") rejections caused by clock drift.

You do not need to call this manually under normal conditions:

- The offset is applied automatically before the **first** signed request.
- Whenever a `-1021` error is received, the client re-arms and re-syncs before the **next** signed request.

Call `await client.sync_time()` explicitly if you want to warm up the offset before trading begins or if you run a periodic resync loop. Returns the new offset in milliseconds.

### client.key(api_key) -> self

Define or change api key. This method is unnecessary if we only request APIs of [`SecurityType.NONE`](https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#endpoint-security-type)

### client.secret(api_secret) -> self

Define or change api secret, especially when we have not define api secret in `Client` constructor.

`api_secret` is not always required for using binance-sdk. See [Endpoint security type](https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#endpoint-security-type)

### await client.get(uri, **kwargs)
### await client.post(uri, **kwargs)
### await client.put(uri, **kwargs)
### await client.delete(uri, **kwargs)

- **uri** `str` the absolute request URL
- **security_type?** `SecurityType` endpoint security type. Defaults to `SecurityType.NONE`.

Generic REST escape hatch — sends a GET/POST/PUT/DELETE HTTPS request over the shared `aiohttp` session. Use this for arbitrary or unwrapped endpoints (e.g. `/sapi/` paths) that are not yet covered by the named WS-API methods. Errors from this path surface as `RateLimitException` (HTTP 429), `IPBannedException` (HTTP 418), or `StatusException` (other non-2xx).

### await client.subscribe(subtype, *subtype_params) -> None
### await client.subscribe(*subscriptions) -> None

- **subtype** `str` subscription type, should be one of `SubType.*`s. For details, see [SubType](#subtype)
- **subtype_params** `List` params for a certain `subtype`
- **subscriptions** `List[Tuple]` a pack of subscriptions each of which is a tuple of `subtype` and `*subtype_params`.

Subscribe to a stream or multiple streams. If no websocket connection is made up, `client.subscribe` will also create a websocket connection.

```py
from binance import SubType, TimeFrame

await client.subscribe(SubType.TICKER, 'BNBUSDT')
await client.subscribe(SubType.BOOK_TICKER, 'BNBUSDT')
await client.subscribe(SubType.AVG_PRICE, 'BNBUSDT')
await client.subscribe(SubType.WINDOW_TICKER, 'BNBUSDT', TimeFrame.H1)
await client.subscribe(SubType.PARTIAL_ORDER_BOOK, 'BNBUSDT', 20)
await client.subscribe(SubType.PARTIAL_ORDER_BOOK, 'BNBUSDT', 20, 100)

# SubType.ALL_MARKET_MINI_TICKERS
await client.subscribe(SubType.ALL_MARKET_MINI_TICKERS)

# SubType.ALL_MARKET_WINDOW_TICKERS with window 4h
await client.subscribe(SubType.ALL_MARKET_WINDOW_TICKERS, TimeFrame.H4)

# Subcribe to multiple types
await client.subscribe(
    (SubType.KLINE, 'BTC_USDT', TimeFrame.D1),
    (SubType.KLINE_UTC8, 'BTC_USDT', TimeFrame.D1),
    (SubType.TICKER, 'BNBUSDT'),
    (
        [
            SubType.ORDER_BOOK,
            SubType.TRADE
        ],
        ['BNBUSDT', 'BTCUSDT']
    ),
    (SubType.ALL_MARKET_MINI_TICKERS,) # <-- PAY ATTENTION to the `,` here
)
```

Possible exceptions:
- `InvalidSubParamsException`
- `UnsupportedSubTypeException`
- `InvalidSubTypeParamException`
- `StreamAbandonedException`

### client.start() -> self

Start receiving streams

### client.stop() -> self

Stop receiving streams

### await client.close(code=4999) -> None

- **code** `int=4999` the custom close code for websocket. It should be in the [range 4000 - 4999](https://tools.ietf.org/html/rfc6455#section-7.4.2)

Close stream connection, clear all stream subscriptions and clear all handlers.

### client.handler(*handlers) -> self

- **handlers** `List[Union[HandlerExceptionHandlerBase,TradeHandlerBase,...]]`

Register message handlers for streams. If we've subscribed to a stream of a certain `subtype` with no corresponding handler provided, the messages of `subtype` will not be handled.

Except for `HandlerExceptionHandlerBase`, handlers each of whose name ends with `Base` should be inherited before use.

Typically, we need to override the `def receive(self, payload)` method.

```py
class MyTradeHandler(TradeHandlerBase):
    async def receive(self, payload):
        # `payload` is a StockDataFrame.
        df = super().receive(payload)
        await saveTrade(df)

client.handler(MyTradeHandler())
```

We could also register multiple handlers at one time

```py
client.handler(MyTradeHandler(), MyTickerHandler())
```

If we register an invalid handler, an `InvalidHandlerException` exception will be raised.

## SubType

In this section, we will note the parameters for each `subtypes`

### `SubType` with parameters `symbol` and `interval`

- `SubType.KLINE`
- `SubType.KLINE_UTC8`

And `interval` should be one of the `TimeFrame` enumerables

### `SubType`s with a param `symbol`

- `SubType.TRADE`
- `SubType.AGG_TRADE`
- `SubType.BLOCK_TRADE`
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

## RetryPolicy

Retry policy is used by binance-sdk to determine what to do next after the client fails to do some certain thing.

```py
abandon, delay = stream_retry_policy(info)

# `info.fails` is the counter number of
#   how many times has the stream encountered the connection failure.
# If the stream is disconnected just now for the first time, `info.fails` will be `1`

# `info.exception` is the exception that raised which caused the failure

# If abandon is `True`, then the client will give up reconnecting.
# Otherwise:
# - The client will asyncio.sleep `delay` seconds before reconnecting.
```

Since `3.2.0` the default policy is a bounded, jittered exponential backoff (≈0.5s → 30s, never abandoning). See [Rate Limits](#rate-limits) for why.

## Rate Limits

`binance-sdk` is built to respect [Binance's documented rate limits](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/limits) and to avoid the `429` → `418` IP-ban escalation (which can take a live trading system offline for up to 3 days). Since `3.3.0` every limit — REST and WebSocket — is tracked by a single unified rate-limit core, and you can read its live state through `client.rate_limit_snapshot()`.

### The pools

Binance enforces several independent pools; the core models each one:

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

Trading, account, and market-data calls now go over the WS-API connection. Server errors on this path surface as:

- `StreamSubscribeException` — any server-side error (`code`/`msg` fields match the Binance WS-API error object).
- `StreamRateLimitException` (subclass of `StreamSubscribeException`) — rate-limit rejection (code `-1003`, status `429`/`418`); carries `retry_after` in milliseconds.

```py
from binance import StreamSubscribeException, StreamRateLimitException

try:
    await client.create_order(symbol='BTCUSDT', side='BUY', ...)
except StreamRateLimitException as e:
    # WS-API rate-limited; e.retry_after is milliseconds
    await asyncio.sleep(e.retry_after / 1000)
except StreamSubscribeException as e:
    print(f'WS-API error {e.code}: {e.msg}')
```

### REST escape hatch: typed errors

The generic `client.get/post/put/delete` REST path raises typed HTTP exceptions:

```py
from binance import RateLimitException, IPBannedException, StatusException

try:
    await client.get('https://api.binance.com/sapi/v1/...')
except IPBannedException as e:
    # HTTP 418 — your IP is banned; wait it out
    await asyncio.sleep(e.retry_after)
except RateLimitException as e:
    # HTTP 429 — too many requests; back off
    await asyncio.sleep(e.retry_after)
except StatusException as e:
    print(f'HTTP {e.status_code}: {e.response}')
```

Both `RateLimitException` and `IPBannedException` subclass `StatusException`. The client **never auto-retries** — it surfaces `retry_after` and lets your strategy decide.

### REST: used-weight visibility

The rate-limit headers on every REST response are captured. After any REST escape-hatch call you can read the latest values:

```py
await client.get('https://api.binance.com/api/v3/exchangeInfo')

client.used_weight   # e.g. {'1m': 20}   (from X-MBX-USED-WEIGHT-*)
client.order_count   # e.g. {'10s': 3}   (from X-MBX-ORDER-COUNT-*)
```

### REST: proactive throttle

By default (`rate_limit_guard=True`) the client throttles *before* sending a request that would breach the IP request-weight, IP raw-request, or account-order pools. Recommended for live trading:

```py
client = Client(api_key, api_secret, rate_limit_guard=True)
```

The per-endpoint weight table is a conservative pre-throttle; the authoritative truth is always the `X-MBX-USED-WEIGHT-*` / `X-MBX-ORDER-COUNT-*` response headers, which the core reconciles after every call (`used = max(client_estimate, header)`). With `rate_limit_guard=False`, usage is still tracked (so monitoring works) but requests are never delayed.

Whenever a response carries Binance's `rateLimits` array (e.g. from `get_exchange_info()`), the core auto-configures its pool *limits* from it — so on a higher VIP tier the budgets track your account's real caps instead of the conservative defaults.

### Monitoring: `client.rate_limit_snapshot()`

`rate_limit_snapshot()` returns a read-only, local (no network) `RateLimitSnapshot` you can poll from a monitoring loop or risk gate:

```py
snap = client.rate_limit_snapshot()

snap.max_utilization   # 0.0–1.0+, the busiest pool right now
snap.throttled         # True if anything is queued/sleeping or a retry-after is active
snap.retry_after       # seconds remaining on a 429/418 ban, or None
snap.pending           # total calls currently waiting on a pool

for w in snap.windows:
    print(w.scope, w.type, w.interval, f'{w.used}/{w.limit}', w.utilization, w.source)
    # e.g. ip request_weight 1m 5400/5400 1.0 header
```

A `RateLimitWindow` describes one pool: `scope` (`ip`/`account`/`connection`), `type` (`request_weight`/`raw_requests`/`orders`/`ws_connections`/`ws_messages`/`ws_streams`), `interval` (`1m`, `10s`, …), `used`, `limit` (the effective, safety-adjusted cap), `remaining`, `utilization` (`used/limit`), `pending`, and `source` — `header` when reconciled from an authoritative Binance header, otherwise `client` (a local estimate). `RateLimitSnapshot` exposes `windows`, `pending`, `retry_after`, `throttled`, `at` (epoch seconds), and the `max_utilization` property. Both types are importable from `binance`.

### WebSocket: connection, message, and stream limits

- **Connections** are gated to stay under Binance's 300 attempts / 5 min / IP limit (a shared limiter, default cap 290/5min), independent of your `stream_retry_policy`.
- **Outgoing messages** are limited to 5/second (Binance's documented limit, including ping/pong and subscribe/unsubscribe).
- **Streams per connection** are capped at Binance's 1024 limit; exceeding it raises `TooManyStreamsException` (carrying `requested`/`limit`) instead of failing opaquely.
- **`serverShutdown`** events (sent ~10 min before Binance's 24h forced disconnect) trigger a proactive reconnect.

WebSocket-API (user stream) rate-limit errors (code `-1003`, status `418`/`429`) raise `StreamRateLimitException` (a subclass of `StreamSubscribeException`) carrying `retry_after`.

### Behavioral changes in 3.4.0

- **Asymmetric signing**: `Client(private_key=..., private_key_pass=...)` now signs requests with Ed25519 or RSA (Binance's recommended key types; HMAC via `api_secret` remains the default fallback).
- **Server-time sync**: signed requests auto-sync a clock offset (and re-sync on `-1021`) to avoid timestamp-out-of-recvWindow rejections; call `await client.sync_time()` to warm it up.
- **Per-symbol order book depth**: `OrderBookHandlerBase.orderbook(symbol, limit=...)` overrides the snapshot depth for a single symbol.
- **Removed the dead `wapi`/`sapi` API surface** (`get_deposit_history`, `withdraw`, sub-account helpers, etc.): every endpoint returned 404 from Binance. Proper `/sapi/` support, if needed, will be a separate addition.
- **WebSocket keepalive simplified**: the redundant `websockets` client-side ping is disabled (the library still auto-replies pong to Binance server pings); `stream_timeout` now defaults to 30s.

### Behavioral changes in 3.3.0

- All rate limiting — REST weight/raw/orders and WS connections/messages/streams — now flows through one unified core (`binance.rate_limit`), the single source of truth.
- New `client.rate_limit_snapshot()` returns a `RateLimitSnapshot` for live monitoring; `RateLimiter`, `RateLimitSnapshot`, and `RateLimitWindow` are now exported from `binance`.
- The account **orders** pool is now enforced (100/10s and 200000/1d), failing fast with `RateLimitReachedException` rather than sleeping.
- Responses carrying a `rateLimits` array auto-configure the pool limits.
- The previously documented `stream_message_rate` constructor argument has been removed; the 5/s outgoing-message limit is now managed by the core per connection.

### Behavioral changes in 3.2.0

- **Reconnect backoff is now bounded and jittered** (≈0.5s → 30s, never abandoning) instead of the previous near-zero-delay loop. Reconnection is intentionally slower but cannot trigger an IP ban; override with your own `stream_retry_policy` if you need different behavior.
- `429`/`418` now raise `RateLimitException`/`IPBannedException` (subclasses of `StatusException`).
- The WebSocket outgoing-message rate now defaults to the documented 5/s.

## OrderBookHandlerBase(**kwargs)

- **kwargs**
  - **limit?** `int=1000` the limit of the depth snapshot (default 1000, max 5000 — any value; Binance caps at 5000)
  - **retry_policy?** `RetryPolicy=`

By default, binance-sdk maintains the orderbook for you according to the rules of [the official documentation](https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly).

Specifically, `OrderBookHandlerBase` does the job.

We can get the managed `OrderBook` object by method `handler.orderbook(symbol)`. The handler-level `limit` (default `1000`) applies to every symbol; to use a different snapshot depth for a single symbol, pass `handler.orderbook(symbol, limit=...)` before subscribing (default 1000, max 5000; any value is accepted and Binance caps it at 5000).

```py
async def main():
    client = Client(api_key)

    # Unlike other handlers, we usually do not need to inherit `OrderBookHandlerBase`,
    #   unless we need to receive the raw payload of 'depthUpdate' message
    handler = OrderBookHandlerBase()

    client.handler(handler)
    await client.subscribe(SubType.ORDER_BOOK, 'BTCUSDT')

    # Get the reference of OrderBook object for 'BTCUSDT'
    orderbook = handler.orderbook('BTCUSDT')

    while True:
        # If the `retry_policy` never abandon a retry,
        #   the 'try' block could be emitted
        try:
            await orderbook.updated()
        except Exception as e:
            print('exception occurred')
        else:
            await doSomethingWith(orderbook.asks, orderbook.bids)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

loop.run_forever()
```

## OrderBook(symbol, **kwargs)

- **symbol** `str` the symbol name
- **kwargs**
  - **limit?** `int=1000` limit of the orderbook snapshot (default 1000, max 5000 — any value; Binance caps at 5000)
  - **client** `Client=None` the instance of `binance.Client`
  - **retry_policy?** `Callable[[int, Exception], (bool, int, bool)]` retry policy for depth snapshot which has the same mechanism as `Client::stream_retry_policy`

`OrderBook` is another public class that we could import from binance-sdk and you could also construct your own `OrderBook` instance.

```py
async def main():
    # PAY attention that `orderbook` should be run in an event loop
    orderbook = OrderBook('BTCUSDT', client=client)

    await orderbook.updated()

    print(orderbook.asks)
```

### orderbook.set_client(client) -> None

- **client** `Client` the instance of `binance.Client`

Set the client. If `client` is not specified in the constructor, then executing this method will make the orderbook to fetch the snapshot for the first time.

### orderbook.set_limit(limit) -> None

- **limit** `int`

Set depth limit which is used by the [Binance WebSocket API `depth` request](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api/market-data-requests#order-book).

### orderbook.set_retry_policy(retry_policy) -> None

- **retry_policy** `RetryPolicy`

Set retry policy of the certain orderbook

### property `orderbook.ready` -> bool

There is a property getter in `orderbook` to detect whether the asks and bids are updated in the orderbook.

If there is a network malfunction of the stream which causing the gap between two depth update messages, `orderbook` will fetch a new snapshot from the server, and during that time and before we merge the snapshot, `orderbook.ready` is `False`.

### property `orderbook.asks` -> list
### property `orderbook.bids` -> list

Get asks and bids in ascending order.

### orderbook.update(payload) -> bool

- **payload** `dict` the data payload of the `depthUpdate` stream message

Returns `True` if the payload is valid and is updated to the orderbook, otherwise `False`

If the return value is `False`, the orderbook will automatically start fetching the snapshot

### await orderbook.fetch() -> None

Manually fetch the snapshot.

For most scenarios, you need **NOT** to call this method because once
there is an invalid payload, the orderbook will fetch the snapshot itself.

### await orderbook.updated() -> None

Wait for the next update of the orderbook.

We could also await `orderbook.updated()` to make sure the orderbook is ready.

If the orderbook fails to fetch depth snapshot for so many times which means the fetching is abanboned by the `retry_policy`, an `aiohttp` exception will be raised.

#### Listen to the updates of `orderbook`

```py
async def start_listening_updates(orderbook):
    # This is an infinite loop
    while True:
        await orderbook.updated()
        # do something

def start():
    return asyncio.create_task(start_listening_updates(orderbook))

task = start()

# If we want to stop listening
task.cancel()
```

## License

[MIT](../LICENSE)
