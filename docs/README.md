# binance-sdk

`binance-sdk` is an another unofficial Binance SDK for python 3.7+, which:

- Based on [Binance Official API Docs v3](https://github.com/binance/binance-official-api-docs).
- Uses Binance's new websocket stream which supports live pub/sub so that we only need **ONE** websocket connection.
- Returns `StockDataFrame` (from `stock-pandas`) for stream payloads with renamed columns.
- Based on python `async`/`await`
- Manages the order book for you (handled by `OrderBookHandlerBase`), so that you need not to worry about websocket reconnection and message losses. For details, see the section [`OrderBookHandlerBase`](#orderbookhandlerbasekwargs)
- Supports to change API endpoints, so that we could use faster API hosts.

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
- `AccountInfoHandlerBase`
- `AccountPositionHandlerBase`
- `BalanceUpdateHandlerBase`
- `OrderUpdateHandlerBase`
- `OrderListStatusHandlerBase`
- `ExternalLockUpdateHandlerBase`
- `EventStreamTerminatedHandlerBase`
- `HandlerExceptionHandlerBase` a special handler to handle stream exceptions

```py
client.handler(MyTradeHandler(), MyOrderBookHandler(), MyKlineHandler())
```

### Subscribe to user streams

```py
# Before subscribe to user stream, you need to provide `api_secret` (and also `api_key`)
client.secret(api_secret)

# Or, you should provide `api_secret` when initialize the client
# ```
# client = Client(api_key, api_secret)
# ```

# binance-sdk handles user stream subscription internally via
# WebSocket API `userDataStream.subscribe.signature`
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

# APIs

## Client(**kwargs)

All arguments of the constructor Client are keyworded arguments and all optional.

- **api_key?** `str=None` binance api key
- **api_secret?** `str=None` binance api secret for HMAC-SHA256 signing (deprecated by Binance; prefer asymmetric keys)
- **private_key?** `str|bytes=None` Ed25519 or RSA PEM private key (PEM content or file path). When provided, used for request signing instead of `api_secret`. Binance recommends Ed25519 (fastest) or RSA over the deprecated HMAC keys.
- **private_key_pass?** `str|bytes=None` password to decrypt an encrypted PEM private key; `None` for unencrypted keys
- **request_params?** `dict=None` global request params for aiohttp
- **stream_retry_policy?** `Callable[[int, Exception], Tuple[bool, int, bool]]` retry policy for websocket stream. For details, see [RetryPolicy](#retrypolicy)
- **stream_timeout?** `int=5` seconds util the stream reach an timeout error
- **rate_limit_guard?** `bool=True` when `True`, the client proactively throttles REST requests with a client-side weight/raw/order budget to stay under Binance's per-IP and per-account caps. When `False`, usage is still tracked (so monitoring works) but requests are never delayed. On by default. See [Rate Limits](#rate-limits).
- **api_host?** `str='https://api.binance.com'` to specify another API host for rest API requests. 这个参数的存在意义，使用方法，不累述，你懂的。
- **stream_host?** `str='wss://stream.binance.com'` to specify another stream host for websocket connections.
- **ws_api_host?** `str='wss://ws-api.binance.com/ws-api/v3'` to specify WebSocket API host for user stream subscription.

Create a binance client.

Each API method accepts only keyworded arguments (kwargs) and has verbosed Python doc strings (Google style) which you could check out when you are coding.

The following example shows how to create a new order.

```py
from binance import (
    OrderSide,
    OrderType,
    TimeInForce
)

# All arguments are keyworded arguments.
await client.create_order(
    symbol='BTCUSDT',

    # You could use string `BUY` (NOT recommended) instead of
    # the built-in enum types of Binance-sdk.

    # But it is a good practise to use enums which could help
    # us to avoid spelling mistakes, and save our money.
    side=OrderSide.BUY,
    type=OrderType.LIMIT,
    timeInForce=TimeInForce.GTC,

    # Binance-sdk will not handle Decimals for you,
    # so you'd better to know how to deal with python float precisions.
    # Or you could use string-type quantity.
    quantity=10.,

    # It is better to use string type instead of float.
    # The same as `quantity`
    price='7000.1'
)
```

### await client.sync_time() -> int

Syncs the local clock offset against Binance server time by calling `GET /api/v3/time` and storing `server_time - local_time` (ms). This offset is added to the `timestamp` of every signed request, preventing `-1021` ("Timestamp for this request is outside of the recvWindow") rejections caused by clock drift.

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

- **uri** `str` the request url
- **security_type?** `SecurityType` endpoint security type. Defaults to `SecurityType.NONE`.

Send a GET/POST/PUT/DELETE HTTPs request.

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

### REST: typed errors and used-weight visibility

The rate-limit headers on every REST response are captured. After any call you can read the latest values:

```py
await client.get_exchange_info()

client.used_weight   # e.g. {'1m': 20}   (from X-MBX-USED-WEIGHT-*)
client.order_count   # e.g. {'10s': 3}   (from X-MBX-ORDER-COUNT-*)
```

When Binance throttles you, the client raises a typed exception carrying `retry_after` (seconds) so your strategy can back off precisely:

```py
from binance import RateLimitException, IPBannedException

try:
    await client.create_order(...)
except IPBannedException as e:
    # HTTP 418 — your IP is banned; wait it out
    await asyncio.sleep(e.retry_after)
except RateLimitException as e:
    # HTTP 429 — too many requests; back off
    await asyncio.sleep(e.retry_after)
```

Both subclass `StatusException`, so existing `except StatusException` handlers keep working. The client **never auto-retries** (a blind retry of an order could double-fill) — it surfaces `retry_after` and lets you decide.

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
  - **limit?** `int=100` the limit of the depth snapshot
  - **retry_policy?** `RetryPolicy=`

By default, binance-sdk maintains the orderbook for you according to the rules of [the official documentation](https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly).

Specifically, `OrderBookHandlerBase` does the job.

We could get the managed `OrderBook` object by method `handler.orderbook(symbol)`.

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
  - **limit?** `int=100` limit of the orderbook
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

Set depth limit which is used by [binance reset api](https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#order-book).

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
