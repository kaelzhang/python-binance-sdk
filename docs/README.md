[![](https://github.com/kaelzhang/python-binance-sdk/actions/workflows/python.yml/badge.svg)](https://github.com/kaelzhang/python-binance-sdk/actions/workflows/python.yml)
[![](https://codecov.io/gh/kaelzhang/python-binance-sdk/branch/master/graph/badge.svg)](https://codecov.io/gh/kaelzhang/python-binance-sdk)
[![](https://img.shields.io/pypi/v/binance-sdk.svg)](https://pypi.org/project/binance-sdk/)

# binance-sdk

`binance-sdk` is an unofficial async Binance SDK for Python 3.11+, supporting three markets: **Spot**, **USDⓈ-M Futures** (`UMFuturesClient`), and **COIN-M Futures** (`CMFuturesClient`). It:

- Routes every Spot request/response call over the **Binance WebSocket API** (`wss://ws-api.binance.com`) for low latency, while keeping a generic `get`/`post`/`put`/`delete` REST escape hatch for arbitrary endpoints.
- Provides full-surface **USDⓈ-M Futures** (`UMFuturesClient`) and **COIN-M Futures** (`CMFuturesClient`) clients: market-data (open interest, funding rates, mark price), trading (orders, batch orders, position config), account management, WebSocket market-data streams (mark price, liquidations), and futures user-data streams.
- Futures trading endpoints route over the **Futures WebSocket API** (ws-fapi / ws-dapi) where available; REST otherwise. All signed endpoints require `Credentials`.
- Returns `StockDataFrame` (from `stock-pandas`) for stream payloads with renamed columns.
- Uses a first-class `Credentials` object (`SpotClient`, `UMFuturesClient`, `CMFuturesClient` all accept the same `Credentials` instance).
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
from binance import Credentials, SpotClient, UMFuturesClient, CMFuturesClient

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
um   = UMFuturesClient(creds)   # USDⓈ-M Futures: trading + account + streams
cm   = CMFuturesClient(creds)   # COIN-M Futures:  trading + account + streams
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

Every method is an `await`-able coroutine. All take keyword arguments matching the [Binance WebSocket API](https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/general-api-information) parameters and return the parsed `result`.

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

The client automatically keeps the server-time offset in sync internally — it syncs before the first signed request and re-syncs after any `-1021` response — so users never need to manage it.

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

`UMFuturesClient` provides the full USDⓈ-M Perpetual Futures surface: market-data, trading, account management, market-data streams, and a futures user-data stream.

```python
from binance import UMFuturesClient, Credentials, SubType
from binance.futures.enums import FuturesOrderType, FuturesTimeInForce

# Public market data — no credentials needed:
um = UMFuturesClient()
oi  = await um.get_open_interest(symbol='BTCUSDT')
fr  = await um.get_funding_rate(symbol='BTCUSDT', limit=10)
mp  = await um.get_premium_index(symbol='BTCUSDT')

# Trading and account — credentials required:
um = UMFuturesClient(Credentials(api_key='KEY', api_secret='SECRET'))
order = await um.create_order(
    symbol='BTCUSDT',
    side='BUY',
    type=FuturesOrderType.LIMIT,
    timeInForce=FuturesTimeInForce.GTC,
    quantity='0.001',
    price='30000',
)
balance = await um.get_balance()
```

### REST market-data methods

All are public (`SecurityType.NONE`), no credentials required:

- `get_open_interest(**kwargs)` — present open interest for a symbol (weight 1). Required: `symbol`.
- `get_open_interest_hist(**kwargs)` — historical open interest stats (weight 1). Required: `symbol`, `period` (one of `'5m'`/`'15m'`/`'30m'`/`'1h'`/`'2h'`/`'4h'`/`'6h'`/`'12h'`/`'1d'`). Optional: `limit` (default 30, max 500), `startTime`, `endTime`.
- `get_funding_rate(**kwargs)` — historical funding rate data (weight 1). Optional: `symbol`, `startTime`, `endTime`, `limit` (default 100, max 1000).
- `get_funding_info(**kwargs)` — funding rate cap/floor and interval for all symbols (weight 1).
- `get_premium_index(**kwargs)` — mark price, index price, and funding rate (weight 1 with `symbol`; 10 for all symbols). Optional: `symbol`.

### Trading & account methods

All signed endpoints require `Credentials`. Order endpoints (`create_order`, `modify_order`, `cancel_order`, `get_order`, `get_account`, `get_balance`) route over the **Futures WebSocket API** (`wss://ws-fapi.binance.com/ws-fapi/v1`). The remaining trading/account/position endpoints use REST (`https://fapi.binance.com`).

> **v1 → v2 upgrade:** `get_account` / `get_balance` now use the v2 WS-API methods (`v2/account.status`, `v2/account.balance`) which return a richer field set. The v1 methods have been dropped.

**Orders:**

- `create_order(**kwargs)` — place a new order (WS-API, weight 1).
- `create_test_order(**kwargs)` — test-place an order without submitting it (REST, weight 1).
- `modify_order(**kwargs)` — modify an existing order (WS-API, weight 1).
- `cancel_order(**kwargs)` — cancel an active order (WS-API, weight 1).
- `cancel_all_orders(**kwargs)` — cancel all open orders for a symbol (REST, weight 1). Required: `symbol`.
- `get_order(**kwargs)` — check order status (WS-API, weight 1).
- `get_open_orders(**kwargs)` — list open orders (REST, weight 1 with `symbol`, 40 without).
- `get_all_orders(**kwargs)` — list all orders (active, cancelled, filled) for a symbol (REST, weight 5). Required: `symbol`.
- `create_batch_orders(**kwargs)` — place multiple orders in one request (REST, weight 5). Required: `batchOrders` list.
- `cancel_batch_orders(**kwargs)` — cancel multiple orders in one request (REST, weight 5). Required: `symbol`, plus `orderIdList` or `origClientOrderIdList`.

**Account:**

- `get_account(**kwargs)` — account status including balances and positions (WS-API `v2/account.status`, weight 5).
- `get_balance(**kwargs)` — per-asset balance records (WS-API `v2/account.balance`, weight 5).
- `get_position(**kwargs)` — position information (WS-API `account.position`, weight 5).
- `get_position_risk(**kwargs)` — position risk information (REST, weight 5). Optional: `symbol`.
- `get_user_trades(**kwargs)` — trade history for a symbol (REST, weight 5). Required: `symbol`.
- `get_commission(**kwargs)` — commission rates for a symbol (REST, weight 20). Required: `symbol`.
- `get_income(**kwargs)` — income history (REST, weight 30). Optional: `symbol`, `incomeType`, `startTime`, `endTime`, `limit`.
- `get_leverage_bracket(**kwargs)` — leverage bracket info (REST, weight 1). Optional: `symbol`.

**Position configuration:**

- `set_leverage(**kwargs)` — change initial leverage (REST, weight 1). Required: `symbol`, `leverage`.
- `set_margin_type(**kwargs)` — change margin type (`ISOLATED`/`CROSSED`) for a symbol (REST, weight 1). Required: `symbol`, `marginType`.
- `set_position_margin(**kwargs)` — adjust isolated position margin (REST, weight 1). Required: `symbol`, `amount`, `type` (1=add, 2=reduce).
- `get_position_mode(**kwargs)` — get current position mode (one-way vs hedge) (WS-API `positionSide.dual.get`, weight 30).
- `set_position_mode(**kwargs)` — change position mode (REST, weight 1). Required: `dualSidePosition` bool.
- `get_multi_assets_mode(**kwargs)` — get multi-assets margin mode (REST, weight 30). USDⓈ-M only.
- `set_multi_assets_mode(**kwargs)` — change multi-assets margin mode (REST, weight 1). USDⓈ-M only.

**Algo orders (USDⓈ-M only):**

- `create_algo_order(**kwargs)` — place a TWAP or VP algo order (WS-API `algoOrder.place`, weight 0).
- `cancel_algo_order(**kwargs)` — cancel an active algo order (WS-API `algoOrder.cancel`, weight 0).

### Futures user-data stream

Subscribe to account events (fills, balance changes, margin calls, config changes):

```python
from binance import (
    UMFuturesClient, Credentials, SubType,
    FuturesAccountUpdateHandlerBase,
    FuturesOrderUpdateHandlerBase,
    FuturesMarginCallHandlerBase,
    FuturesAccountConfigUpdateHandlerBase,
    FuturesListenKeyExpiredHandlerBase,
    FuturesEventStreamTerminatedHandlerBase,
)

um = UMFuturesClient(Credentials(api_key='KEY', api_secret='SECRET'))

class MyOrderUpdate(FuturesOrderUpdateHandlerBase):
    def receive(self, payload):
        # payload is the raw Binance ORDER_TRADE_UPDATE dict
        print(payload['o']['s'], payload['o']['X'])

um.handler(MyOrderUpdate())
await um.subscribe(SubType.USER)
```

The futures user-data stream uses the **listenKey flow**: the SDK calls `userDataStream.start` over the WS-API connection to obtain a listenKey, then opens a dedicated `fstream` connection for the events (USDⓈ-M: `wss://fstream.binance.com/private/ws/<listenKey>`; COIN-M: `wss://dstream.binance.com/ws/<listenKey>`), sends periodic `userDataStream.ping` keepalives (every 50 min), and automatically re-obtains the key and reconnects on `listenKeyExpired`. All of this is managed internally. USDⓈ-M's per-category URL split (`/public/stream` for depth/bookTicker, `/market/stream` for everything else, `/private/ws/<listenKey>` for user-data) was mandated by Binance's 2026-04-23 decommission of the legacy `/ws` and `/stream` paths and is fully transparent to client code.

Handler bases to subclass (from `binance`):

- `FuturesAccountUpdateHandlerBase` — `ACCOUNT_UPDATE` (balance + position snapshot on order/transfer)
- `FuturesOrderUpdateHandlerBase` — `ORDER_TRADE_UPDATE` (order lifecycle: new/partial/filled/cancelled)
- `FuturesMarginCallHandlerBase` — `MARGIN_CALL` (position risk ratio exceeds maintenance margin)
- `FuturesAccountConfigUpdateHandlerBase` — `ACCOUNT_CONFIG_UPDATE` (leverage or multi-assets mode change)
- `FuturesListenKeyExpiredHandlerBase` — `listenKeyExpired` (key expired; SDK auto-recovers)
- `FuturesEventStreamTerminatedHandlerBase` — SDK-synthesized event when the dedicated fstream drops
- `FuturesTradeLiteHandlerBase` — `TRADE_LITE` (low-latency fill; fewer fields than `ORDER_TRADE_UPDATE`; **UM only**)
- `FuturesStrategyUpdateHandlerBase` — `STRATEGY_UPDATE` (algo/TWAP strategy lifecycle; UM + CM)
- `FuturesAlgoUpdateHandlerBase` — `ALGO_UPDATE` (algo order status change; **UM only**; carries conditional-order rejection reasons in `o.rm` since 2025-12-10)

> **Removed (2025-12-10):** `FuturesConditionalOrderTriggerRejectHandlerBase` /
> `CONDITIONAL_ORDER_TRIGGER_REJECT`.  Binance migrated conditional orders to
> the Algo Service; rejection reasons now arrive inside `ALGO_UPDATE`'s
> `o.rm` (reject_message) field.  No backward-compatibility shim — the class
> is gone from `binance.futures.user_handlers` and the top-level `binance`
> package.  See [the derivatives change-log](https://developers.binance.com/docs/derivatives/change-log).

> **Removed (Round-8 M-5, 2026-05-30):** `FuturesGridUpdateHandlerBase` /
> `GRID_UPDATE`.  The Binance docs explicitly mark the event as *Deprecated*
> on both UM and CM pages
> ([UM Event-GRID-UPDATE](https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-GRID-UPDATE),
> [CM Event-GRID-UPDATE](https://developers.binance.com/docs/derivatives/coin-margined-futures/user-data-streams/Event-GRID-UPDATE)).
> Per the zero-backward-compatibility policy the class is gone from
> `binance.futures.user_handlers` and the top-level `binance` package.
> Callers that still need grid-strategy lifecycle visibility should
> subscribe to `STRATEGY_UPDATE` (`FuturesStrategyUpdateHandlerBase`)
> instead — that event remains documented and live on both UM and CM.

### Futures market-data streams

Both `UMFuturesClient` and `CMFuturesClient` support a full set of market-data streams. All handler bases are importable from `binance`.

**Quick example — UM kline stream:**

```python
from binance import (
    UMFuturesClient, SubType,
    FuturesKlineHandlerBase
)

um = UMFuturesClient()

class MyKline(FuturesKlineHandlerBase):
    def receive(self, payload):
        df = super().receive(payload)
        # df columns: type, event_time, open_time, close_time, symbol, interval,
        #             open, high, low, close, volume, quote_volume,
        #             taker_volume, taker_quote_volume, total_trades, is_closed
        if df['is_closed']:
            print(df['symbol'], df['close'])

um.handler(MyKline())
await um.subscribe(SubType.KLINE, 'btcusdt', '1m')
```

**Shared streams (available on both UM and CM):**

| SubType | Wire stream | Handler base | Notes |
|---------|-------------|--------------|-------|
| `SubType.MARK_PRICE` | `<symbol>@markPrice[@1s]` | `MarkPriceHandlerBase` | Columns: `mark_price`, `mark_price_avg` (UM only), `index_price`, `funding_rate`, `next_funding_time` |
| `SubType.FORCE_ORDER` | `<symbol>@forceOrder` | `ForceOrderHandlerBase` | Liquidation orders; CM adds `pair` column |
| `SubType.AGG_TRADE` | `<symbol>@aggTrade` | `FuturesAggTradeHandlerBase` | No buyer/seller order IDs (unlike Spot) |
| `SubType.KLINE` | `<symbol>@kline_<interval>` | `FuturesKlineHandlerBase` | Accepts `TimeFrame` or interval string |
| `SubType.CONTINUOUS_KLINE` | `<pair>_<ct>@continuousKline_<interval>` | `FuturesContinuousKlineHandlerBase` | Requires `pair`, `contract_type`, `interval` params |
| `SubType.MINI_TICKER` | `<symbol>@miniTicker` | `FuturesMiniTickerHandlerBase` | |
| `SubType.TICKER` | `<symbol>@ticker` | `FuturesTickerHandlerBase` | |
| `SubType.BOOK_TICKER` | `<symbol>@bookTicker` | `FuturesBookTickerHandlerBase` | No `e` event field; routing by stream name |
| `SubType.PARTIAL_ORDER_BOOK` | `<symbol>@depth<N>[@speed]` | `FuturesPartialOrderBookHandlerBase` | Levels: 5/10/20; speed: 100/500 ms; `receive` returns `(bids_df, asks_df)` |
| `SubType.ORDER_BOOK` | `<symbol>@depth[@speed]` | `FuturesOrderBookHandlerBase` | Diff depth; speed: 100/500 ms |
| `SubType.CONTRACT_INFO` | `!contractInfo` | `FuturesContractInfoHandlerBase` | No params; contract spec changes |
| `SubType.ALL_MARKET_MARK_PRICE` | `!markPrice@arr[@1s]` | `FuturesAllMarketMarkPriceHandlerBase` | Optional `'1s'` param; UM subclass adds `mark_price_avg` |
| `SubType.ALL_MARKET_LIQUIDATION` | `!forceOrder@arr` | `FuturesAllMarketLiquidationHandlerBase` | All-market liquidation array; no params |
| `SubType.ALL_MARKET_MINI_TICKERS` | `!miniTicker@arr` | `FuturesAllMarketMiniTickersHandlerBase` | No params |
| `SubType.ALL_MARKET_TICKERS` | `!ticker@arr` | `FuturesAllMarketTickersHandlerBase` | No params |
| `SubType.ALL_MARKET_BOOK_TICKER` | `!bookTicker` | `FuturesAllMarketBookTickerHandlerBase` | No `@arr` suffix (unlike Spot); no params |

**UM-only streams:**

| SubType | Wire stream | Handler base | Notes |
|---------|-------------|--------------|-------|
| `SubType.COMPOSITE_INDEX` | `<symbol>@compositeIndex` | `CompositeIndexHandlerBase` | Index composition for composite-index symbols |
| `SubType.ASSET_INDEX` | `<asset>@assetIndex` or `!assetIndex@arr` | `AssetIndexHandlerBase` / `AllAssetIndexHandlerBase` | Multi-assets mode; no params for `!assetIndex@arr` |
| `SubType.TRADING_SESSION` | `tradingSession` | `TradingSessionHandlerBase` | US equity/commodity session events; no params |

**CM-only streams:**

| SubType | Wire stream | Handler base | Notes |
|---------|-------------|--------------|-------|
| `SubType.INDEX_PRICE` | `<pair>@indexPrice[@1s]` | `IndexPriceHandlerBase` | Spot index price for the underlying pair |
| `SubType.INDEX_PRICE_KLINE` | `<pair>@indexPriceKline_<interval>` | `IndexPriceKlineHandlerBase` | Index price klines |
| `SubType.MARK_PRICE_KLINE` | `<symbol>@markPriceKline_<interval>` | `MarkPriceKlineHandlerBase` | Mark price klines |

Note: COIN-M symbols contain underscores (e.g. `'btcusd_perp'`); pass them lowercase to subscribe calls.

`UMFuturesClient` accepts the same constructor kwargs as `SpotClient` (see above), plus an optional `Credentials` first argument.

## CMFuturesClient (COIN-M Futures)

`CMFuturesClient` provides the full COIN-M (coin-margined) Futures surface. The API surface mirrors `UMFuturesClient` exactly — same method names, same enums — with the following market-specific differences:

- REST host: `https://dapi.binance.com` (dapi, not fapi).
- Stream host: `wss://dstream.binance.com` (dstream, not fstream).
- WS-API host: `wss://ws-dapi.binance.com/ws-dapi/v1`.
- Symbols are contract codes like `'BTCUSD_PERP'` (not `'BTCUSDT'`).
- `get_open_interest_hist` uses `pair` + `contractType` instead of `symbol`.
- `get_funding_rate` **requires** `symbol` (UM treats it as optional).
- `get_premium_index` accepts `symbol` or `pair`; always returns a list.
- `get_position_risk` filters by `marginAsset` or `pair` (not `symbol`).
- **No `get_multi_assets_mode` / `set_multi_assets_mode`** — USDⓈ-M only.
- **No `create_test_order`** — COIN-M does not document a "Test New Order" endpoint; that endpoint is UM-Futures-only and Spot-only.
- No 10-second ORDERS pool (only 1-minute ORDERS pool).

```python
from binance import CMFuturesClient, Credentials, SubType
from binance.futures.enums import FuturesOrderType, FuturesTimeInForce

# Public market data — no credentials needed:
cm = CMFuturesClient()
oi  = await cm.get_open_interest(symbol='BTCUSD_PERP')
fr  = await cm.get_funding_rate(symbol='BTCUSD_PERP', limit=10)
oih = await cm.get_open_interest_hist(
    pair='BTCUSD', contractType='PERPETUAL', period='1h'
)
mp  = await cm.get_premium_index(symbol='BTCUSD_PERP')

# Trading and account — credentials required:
cm = CMFuturesClient(Credentials(api_key='KEY', api_secret='SECRET'))
order = await cm.create_order(
    symbol='BTCUSD_PERP',
    side='BUY',
    type=FuturesOrderType.LIMIT,
    timeInForce=FuturesTimeInForce.GTC,
    quantity='1',       # number of contracts, not base-asset quantity
    price='30000',
)
balance  = await cm.get_balance()
position = await cm.get_position_risk(pair='BTCUSD')
```

### COIN-M trading & account methods

Same method names as USDⓈ-M (see above). Key differences:

- Order endpoints route over `wss://ws-dapi.binance.com/ws-dapi/v1`.
- REST endpoints use `https://dapi.binance.com/dapi/v1/…`.
- `quantity` counts contracts, not base-asset units.
- `get_account` / `get_balance` use v1 WS-API methods (`account.status`, `account.balance`) — COIN-M does not have the v2 variants.
- `get_position` is available via WS-API `account.position`.
- `get_position_mode` uses REST (no `positionSide.dual.get` WS-API equivalent on COIN-M).
- No `create_algo_order` / `cancel_algo_order` (algo orders are USDⓈ-M only).
- No `get_multi_assets_mode` / `set_multi_assets_mode` (USDⓈ-M only).

### COIN-M user-data stream

```python
from binance import (
    CMFuturesClient, Credentials, SubType,
    FuturesAccountUpdateHandlerBase,
    FuturesOrderUpdateHandlerBase,
    FuturesMarginCallHandlerBase,
    FuturesAccountConfigUpdateHandlerBase,
    FuturesListenKeyExpiredHandlerBase,
    FuturesEventStreamTerminatedHandlerBase,
)

cm = CMFuturesClient(Credentials(api_key='KEY', api_secret='SECRET'))

class MyCMOrderUpdate(FuturesOrderUpdateHandlerBase):
    def receive(self, payload):
        print(payload['o']['s'], payload['o']['X'])

cm.handler(MyCMOrderUpdate())
await cm.subscribe(SubType.USER)
```

Same handler bases and same listenKey flow as USDⓈ-M; events arrive on `wss://dstream.binance.com/ws/<listenKey>`.

`CMFuturesClient` accepts the same constructor kwargs as `SpotClient`.

See the [Futures market-data streams](#futures-market-data-streams) table in the USDⓈ-M section for the full stream list; all shared streams and CM-only streams apply. The CM-only streams (`SubType.INDEX_PRICE`, `SubType.INDEX_PRICE_KLINE`, `SubType.MARK_PRICE_KLINE`) are available exclusively on `CMFuturesClient`.

## Futures enums

Import from `binance` or `binance.futures.enums`. Use these for futures order parameters. The shared `OrderSide` (`BUY`/`SELL`) is at the top level (`from binance import OrderSide`).

### `PositionSide`

Position direction for futures orders and positions.

- `PositionSide.BOTH` — one-way (non-hedge) mode: the single net position.
- `PositionSide.LONG` — hedge mode long position.
- `PositionSide.SHORT` — hedge mode short position.

### `FuturesOrderType`

Futures order execution type (extends Spot `OrderType`).

- `FuturesOrderType.LIMIT`
- `FuturesOrderType.MARKET`
- `FuturesOrderType.STOP` — stop-limit (triggered by `stopPrice`; requires `price`)
- `FuturesOrderType.STOP_MARKET` — stop-market (triggered by `stopPrice`)
- `FuturesOrderType.TAKE_PROFIT` — take-profit limit (triggered by `stopPrice`; requires `price`)
- `FuturesOrderType.TAKE_PROFIT_MARKET` — take-profit market (triggered by `stopPrice`)
- `FuturesOrderType.TRAILING_STOP_MARKET` — trailing-stop market (activated by `callbackRate`)

### `FuturesTimeInForce`

Time-in-force values for futures orders (extends Spot `TimeInForce`).

- `FuturesTimeInForce.GTC` — Good Till Cancelled
- `FuturesTimeInForce.IOC` — Immediate Or Cancel
- `FuturesTimeInForce.FOK` — Fill Or Kill
- `FuturesTimeInForce.GTX` — Good Till Crossing (post-only)
- `FuturesTimeInForce.GTD` — Good Till Date (expires at `goodTillDate`)
- `FuturesTimeInForce.RPI` — Retail Price Improvement

### `WorkingType`

Price type used to trigger conditional (stop/take-profit) orders.

- `WorkingType.MARK_PRICE` — use the mark price (default for most stop orders)
- `WorkingType.CONTRACT_PRICE` — use the last traded price

### `MarginType`

Margin mode for a futures position.

- `MarginType.ISOLATED` — each position has its own isolated margin
- `MarginType.CROSSED` — all available balance is used as shared margin

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

### `SubType`s with no param

- `SubType.ALL_MARKET_MINI_TICKERS`
- `SubType.USER`

> **`!ticker@arr` (`SubType.ALL_MARKET_TICKERS`) is not supported on Spot.**
> The Binance Spot WebSocket Streams docs do NOT document a standalone
> `!ticker@arr` (only `!miniTicker@arr` and `!ticker_<window-size>@arr`).
> Use `SubType.ALL_MARKET_WINDOW_TICKERS` for the documented all-market
> full-ticker variant on Spot. The same SubType is supported on Futures
> (see the Futures all-market streams section), where Binance documents
> `!ticker@arr` for both USDⓈ-M and COIN-M.

> **`!bookTicker@arr` is not supported.** Binance has deprecated this stream; use per-symbol `SubType.BOOK_TICKER` instead.

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

## SubType (Futures)

Futures streams are available on both `UMFuturesClient` and `CMFuturesClient`.

### Per-symbol streams (UM + CM)

- `SubType.MARK_PRICE` — `<symbol>@markPrice[@1s]`; optional `'1s'` param; handler: `MarkPriceHandlerBase` (adds `mark_price_avg` on UM)
- `SubType.FORCE_ORDER` — `<symbol>@forceOrder`; handler: `ForceOrderHandlerBase` (adds `pair` on CM)
- `SubType.AGG_TRADE` — `<symbol>@aggTrade`; handler: `FuturesAggTradeHandlerBase`
- `SubType.KLINE` — `<symbol>@kline_<interval>`; params: `symbol`, `interval`; handler: `FuturesKlineHandlerBase`
- `SubType.CONTINUOUS_KLINE` — `<pair>_<ct>@continuousKline_<interval>`; params: `pair`, `contract_type`, `interval`; handler: `FuturesContinuousKlineHandlerBase`
- `SubType.MINI_TICKER` — `<symbol>@miniTicker`; handler: `FuturesMiniTickerHandlerBase`
- `SubType.TICKER` — `<symbol>@ticker`; handler: `FuturesTickerHandlerBase`
- `SubType.BOOK_TICKER` — `<symbol>@bookTicker`; handler: `FuturesBookTickerHandlerBase` (no `e` field)
- `SubType.PARTIAL_ORDER_BOOK` — `<symbol>@depth<N>[@<speed>ms]`; params: `symbol`, `level` (5/10/20), optional `speed` (100/500); handler: `FuturesPartialOrderBookHandlerBase`
- `SubType.ORDER_BOOK` — `<symbol>@depth[@<speed>ms]`; params: `symbol`, optional `speed` (100/500); handler: `FuturesOrderBookHandlerBase`

### All-market streams (UM + CM, no params)

- `SubType.ALL_MARKET_MARK_PRICE` — `!markPrice@arr[@1s]`; optional `'1s'` param; handler: `FuturesAllMarketMarkPriceHandlerBase`
- `SubType.ALL_MARKET_LIQUIDATION` — `!forceOrder@arr`; handler: `FuturesAllMarketLiquidationHandlerBase`
- `SubType.ALL_MARKET_MINI_TICKERS` — `!miniTicker@arr`; handler: `FuturesAllMarketMiniTickersHandlerBase`
- `SubType.ALL_MARKET_TICKERS` — `!ticker@arr`; handler: `FuturesAllMarketTickersHandlerBase`
- `SubType.ALL_MARKET_BOOK_TICKER` — `!bookTicker` (no `@arr` suffix); handler: `FuturesAllMarketBookTickerHandlerBase`
- `SubType.CONTRACT_INFO` — `!contractInfo`; handler: `FuturesContractInfoHandlerBase`

### UM-only streams

- `SubType.COMPOSITE_INDEX` — `<symbol>@compositeIndex`; handler: `CompositeIndexHandlerBase`
- `SubType.ASSET_INDEX` — `<asset>@assetIndex` or `!assetIndex@arr`; handlers: `AssetIndexHandlerBase` / `AllAssetIndexHandlerBase`
- `SubType.TRADING_SESSION` — `tradingSession`; no params; handler: `TradingSessionHandlerBase`

### CM-only streams

- `SubType.INDEX_PRICE` — `<pair>@indexPrice[@1s]`; handler: `IndexPriceHandlerBase`
- `SubType.INDEX_PRICE_KLINE` — `<pair>@indexPriceKline_<interval>`; handler: `IndexPriceKlineHandlerBase`
- `SubType.MARK_PRICE_KLINE` — `<symbol>@markPriceKline_<interval>`; handler: `MarkPriceKlineHandlerBase`

### `SubType.USER` — futures user-data stream

- No params required.
- Available on `UMFuturesClient` and `CMFuturesClient` when `Credentials` are provided.
- Uses the listenKey flow (see [Futures user-data stream](#futures-user-data-stream) above); not the Spot `userDataStream.subscribe` flow.

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

`OrderBookHandlerBase` maintains the order book according to [the official documentation](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly), handling reconnection and message-loss automatically.

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

## Testing

The default `pytest test/` suite is fully hermetic — it only talks to local mock servers and never reaches the real Binance endpoints, so it is safe to run on any network (including CI) without credentials.

### Live smoke tests

A small opt-in suite at `test/test_live.py` exercises real Binance endpoints (Spot / UM / CM stream + WS-API time, plus a Spot `get_account` if `API_KEY` / `API_SECRET` are configured in `.env` / `.env.*`). All live tests are skipped by default — set `BINANCE_LIVE=1` to enable:

```sh
BINANCE_LIVE=1 pytest test/test_live.py -v
```

Live tests do NOT place orders or modify any account state. Binance geo-blocks some regions / cloud-provider IPs (HTTP 451) — the suite fails fast on unreachable hosts so you can diagnose connectivity early. On a network that requires an HTTP/SOCKS proxy to reach Binance, also set `BINANCE_LIVE_TEST=1` so the test session preserves your proxy environment variables (see `test/conftest.py`).

## License

[MIT](../LICENSE)
