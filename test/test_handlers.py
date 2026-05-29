import pytest

from binance import (
    SpotClient,
    Credentials,
    TickerHandlerBase,
    ReuseHandlerException,

    KlineHandlerBase,
    BookTickerHandlerBase,
    PartialOrderBookHandlerBase,
    AvgPriceHandlerBase,
    WindowTickerHandlerBase,
    AccountPositionHandlerBase,
    BalanceUpdateHandlerBase,
    OrderUpdateHandlerBase,
    OrderListStatusHandlerBase,
    ExternalLockUpdateHandlerBase,
    EventStreamTerminatedHandlerBase,
    AllMarketMiniTickersHandlerBase,
    AllMarketWindowTickersHandlerBase,
    BlockTradeHandlerBase,
    ReferencePriceHandlerBase,
)
from binance.core.common.utils import create_future

@pytest.fixture
def client():
    return SpotClient(Credentials('api_key')).start()


ACCOUNT_POSITION = {
    'e': 'outboundAccountPosition',
    'E': 1564034571105,
    'u': 1564034571073,
    'B': [
        {
            'a': 'ETH',
            'f': '10000.000000',
            'l': '0.000000'
        }
    ]
}


async def run_handler(
    client,
    HandlerBase,
    payload,
    expect_payload=None,
    stream='fake'
):
    future = create_future()

    if expect_payload is None:
        expect_payload = payload

    class Handler(HandlerBase):
        def receive(self, p):
            p = super().receive(p)

            if future.done():
                return
            future.set_result(p)

    client.start()
    client.handler(Handler())

    await client._receive({
        'data': ACCOUNT_POSITION,
        'stream': 'fake'
    })

    await client._receive({
        'data': payload,
        'stream': stream
    })

    await client._receive([])
    await client._receive({})

    received = await future

    if callable(expect_payload):
        expect_payload(received)
    else:
        assert received == expect_payload


@pytest.mark.asyncio
async def test_account_pos(client):
    await run_handler(client, AccountPositionHandlerBase, ACCOUNT_POSITION)


@pytest.mark.asyncio
async def test_balance_update(client):
    await run_handler(client, BalanceUpdateHandlerBase, {
        'e': 'balanceUpdate',
        'E': 1573200697110,
        'a': 'BTC',
        'd': '100.00000000',
        'T': 1573200697068
    })


@pytest.mark.asyncio
async def test_order_update(client):
    await run_handler(client, OrderUpdateHandlerBase, {
        'e': 'executionReport',
        'E': 1499405658658,
        's': 'ETHBTC',
        'c': 'mUvoqJxFIILMdfAW5iGSOW',
        'S': 'BUY',
        'o': 'LIMIT'
    })


@pytest.mark.asyncio
async def test_order_list_status(client):
    await run_handler(client, OrderListStatusHandlerBase, {
        'e': 'listStatus',
        'E': 1564035303637,
        's': 'ETHBTC',
        'g': 2,
        'c': 'OCO',
        'l': 'EXEC_STARTED',
        'L': 'EXECUTING'
    })


@pytest.mark.asyncio
async def test_external_lock_update(client):
    await run_handler(client, ExternalLockUpdateHandlerBase, {
        'e': 'externalLockUpdate',
        'E': 1700000000000,
        'a': 'USDT',
        'd': '1.23000000',
        'T': 1700000000123
    })


@pytest.mark.asyncio
async def test_event_stream_terminated(client):
    await run_handler(client, EventStreamTerminatedHandlerBase, {
        'e': 'eventStreamTerminated',
        'E': 1700000000000
    })


def test_event_stream_terminated_docstring_marks_event_as_server_pushed():
    """EventStreamTerminatedHandlerBase docstring must mark ``eventStreamTerminated`` as server-pushed by Binance, NOT SDK-synthesized.

    Spot WS-API source: https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream
    """
    doc = EventStreamTerminatedHandlerBase.__doc__ or ''
    # Stale wording must be gone.
    assert 'synthesized by the SDK' not in doc
    assert 'SDK-synthesized' not in doc
    # The docstring must say the event is pushed by Binance (server-pushed).
    assert 'server-pushed' in doc or 'pushed by Binance' in doc


@pytest.mark.asyncio
async def test_kline_handler(client):
    E = 123456789

    k = {
        't': 123400000,
        'T': 123460000,
        's': 'BNBBTC',
        'i': '1m',
        'f': 100,
        'L': 200,
        'o': '0.0010',
        'c': '0.0020'
    }

    payload = {
        'e': 'kline',
        'E': 123456789,
        's': 'BNBBTC',
        'k': k
    }

    def expect(received):
        row = received.iloc[0]
        assert row['symbol'] == 'BNBBTC'
        assert row['event_time'] == E

    await run_handler(client, KlineHandlerBase, payload, expect)


def expect_ticker(payload):
    row = payload.iloc[0]
    assert row['symbol'] == 'BNBBTC'
    assert row['event_time'] == 123456789


def expect_symbol(payload):
    row = payload.iloc[0]
    assert row['symbol'] == 'BNBBTC'


def expect_book_ticker(payload):
    row = payload.iloc[0]
    assert row['symbol'] == 'BNBBTC'
    assert row['best_bid_price'] == '25.35190000'


def expect_partial_order_book(payload):
    # Per developers.binance.com (Spot Web-Socket Streams "Partial Book Depth"),
    # the snapshot payload carries `lastUpdateId` alongside bids/asks so
    # consumers can reconcile against the diff-depth stream.  The handler
    # surfaces the triple ``(last_update_id, bids_df, asks_df)``.
    last_update_id, bids, asks = payload

    assert last_update_id == 160

    bid_row = bids.iloc[0]
    ask_row = asks.iloc[0]

    assert bid_row['price'] == '0.0024'
    assert bid_row['quantity'] == '10'
    assert ask_row['price'] == '0.0026'
    assert ask_row['quantity'] == '100'


@pytest.mark.asyncio
async def test_book_ticker_handler(client):
    await run_handler(client, BookTickerHandlerBase, {
        'u': 400900217,
        's': 'BNBBTC',
        'b': '25.35190000',
        'B': '31.21000000',
        'a': '25.36520000',
        'A': '40.66000000'
    }, expect_book_ticker, 'bnbbtc@bookTicker')


@pytest.mark.asyncio
async def test_avg_price_handler(client):
    await run_handler(client, AvgPriceHandlerBase, {
        'e': 'avgPrice',
        'E': 1693907033000,
        's': 'BNBBTC',
        'i': '5m',
        'w': '0.00150000',
        'T': 1693907032213
    }, expect_symbol)


@pytest.mark.asyncio
async def test_partial_order_book_handler(client):
    await run_handler(client, PartialOrderBookHandlerBase, {
        'lastUpdateId': 160,
        'bids': [
            ['0.0024', '10']
        ],
        'asks': [
            ['0.0026', '100']
        ]
    }, expect_partial_order_book, 'bnbbtc@depth20')


def test_spot_partial_order_book_exposes_last_update_id_explicitly():
    """Per docs the Spot partial-book payload includes ``lastUpdateId``
    so consumers can reconcile snapshots against the diff-depth stream's
    ``U`` / ``u`` cursor.  The handler's ``_receive`` MUST surface it
    as the first element of the returned tuple."""
    handler = PartialOrderBookHandlerBase()
    result = handler._receive({
        'lastUpdateId': 1234,
        'bids': [['50000.0', '1.0'], ['49999.0', '2.0']],
        'asks': [['50001.0', '0.5'], ['50002.0', '1.5']],
    })
    last_update_id, bids, asks = result
    assert last_update_id == 1234
    assert bids.iloc[0]['price'] == '50000.0'
    assert bids.iloc[1]['price'] == '49999.0'
    assert asks.iloc[0]['price'] == '50001.0'
    assert asks.iloc[1]['quantity'] == '1.5'


@pytest.mark.asyncio
async def test_window_ticker_handler(client):
    await run_handler(client, WindowTickerHandlerBase, {
        'e': '1hTicker',
        'E': 1672515782136,
        's': 'BNBBTC',
        'p': '0.0015',
        'P': '250.00',
        'o': '0.0010',
        'h': '0.0025',
        'l': '0.0010',
        'c': '0.0025',
        'w': '0.0018',
        'v': '10000',
        'q': '18',
        'O': 0,
        'C': 86400000,
        'F': 0,
        'L': 18150,
        'n': 18151
    }, expect_symbol)


@pytest.mark.asyncio
async def test_all_market_miniticker(client):
    ticker = {
        'e': '24hrMiniTicker',
        'E': 123456789,
        's': 'BNBBTC',
        'c': '0.0025',
        'o': '0.0010',
        'h': '0.0025',
        'l': '0.0010',
        'v': '10000',
        'q': '18'
    }

    await run_handler(client, AllMarketMiniTickersHandlerBase, [
        ticker
    ], expect_ticker, '!miniTicker@arr')


@pytest.mark.asyncio
async def test_all_market_window_ticker(client):
    ticker = {
        'e': '1hTicker',
        'E': 123456789,
        's': 'BNBBTC',
        'p': '0.0015',
        'P': '250.00',
        'w': '0.0018',
        'c': '0.0025',
        'o': '0.0010',
        'h': '0.0025',
        'l': '0.0010',
        'v': '10000',
        'q': '18',
        'O': 0,
        'C': 86400000,
        'F': 0,
        'L': 18150,
        'n': 18151
    }

    await run_handler(client, AllMarketWindowTickersHandlerBase, [
        ticker
    ], expect_ticker, '!ticker_1h@arr')


@pytest.mark.asyncio
async def test_block_trade_handler(client):
    await run_handler(client, BlockTradeHandlerBase, {
        'e': 'blockTrade',
        'E': 1772506983582,
        's': 'BNBBTC',
        't': 582,
        'p': '0.052',
        'q': '5838',
        'T': 1772506983321,
        'm': True
    }, expect_symbol)


@pytest.mark.asyncio
async def test_reference_price_handler(client):
    await run_handler(client, ReferencePriceHandlerBase, {
        'e': 'referencePrice',
        's': 'BNBBTC',
        'r': '1.00',
        't': 1770313263917
    }, expect_symbol)


def test_handler_reuse():
    client = SpotClient(Credentials('api_key'))
    client2 = SpotClient(Credentials('api_key'))

    handler = TickerHandlerBase()

    with pytest.raises(ReuseHandlerException, match='more than one'):
        client.handler(handler)
        client2.handler(handler)
