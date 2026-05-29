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
    # The handler MUST apply EXECUTION_REPORT_COLUMNS_MAP to rename keys.
    def expect(p):
        assert p['type'] == 'executionReport'
        assert p['event_time'] == 1499405658658
        assert p['symbol'] == 'ETHBTC'
        assert p['client_order_id'] == 'mUvoqJxFIILMdfAW5iGSOW'
        assert p['side'] == 'BUY'
        assert p['order_type'] == 'LIMIT'

    await run_handler(client, OrderUpdateHandlerBase, {
        'e': 'executionReport',
        'E': 1499405658658,
        's': 'ETHBTC',
        'c': 'mUvoqJxFIILMdfAW5iGSOW',
        'S': 'BUY',
        'o': 'LIMIT'
    }, expect)


def test_execution_report_columns_map_covers_all_documented_fields():
    """Per developers.binance.com (executionReport section), the COLUMNS_MAP
    MUST cover every documented field — standard + conditional — so callers
    that introspect the map know the full surface.
    Docs: https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream
    """
    from binance.spot.user_handlers import EXECUTION_REPORT_COLUMNS_MAP
    # Standard fields
    expected_standard = {
        'e', 'E', 's', 'c', 'S', 'o', 'f', 'q', 'p', 'P', 'F', 'g', 'C',
        'x', 'X', 'r', 'i', 'l', 'z', 'L', 'n', 'N', 'T', 't', 'I', 'w',
        'm', 'M', 'O', 'Z', 'Y', 'Q', 'V',
    }
    # Conditional fields documented in 2025-08-12 CHANGELOG and earlier
    expected_conditional = {
        'd', 'D', 'j', 'J', 'v', 'A', 'B', 'u', 'U', 'Cs',
        'pl', 'pL', 'pY', 'W', 'b', 'a', 'k', 'uS',
        'gP', 'gOT', 'gOV', 'gp', 'eR',
    }
    expected = expected_standard | expected_conditional
    missing = expected - set(EXECUTION_REPORT_COLUMNS_MAP.keys())
    assert not missing, f'EXECUTION_REPORT_COLUMNS_MAP missing keys: {missing}'


def test_execution_report_columns_map_documented_ignore_M():
    """`M` is explicitly marked "Ignore" in the docs — but it IS documented.
    Per strict coverage it must appear in the COLUMNS_MAP with an
    ``_ignore`` marker so downstream code knows the field exists.
    """
    from binance.spot.user_handlers import EXECUTION_REPORT_COLUMNS_MAP
    assert EXECUTION_REPORT_COLUMNS_MAP.get('M', '').startswith('_ignore')


@pytest.mark.asyncio
async def test_order_update_full_payload_every_field_preserved(client):
    """Docs-shaped executionReport payload — assert every documented field
    is preserved in the renamed handler output."""
    payload = {
        'e': 'executionReport',
        'E': 1499405658658,
        's': 'ETHBTC',
        'c': 'mUvoqJxFIILMdfAW5iGSOW',
        'S': 'BUY',
        'o': 'LIMIT',
        'f': 'GTC',
        'q': '1.00000000',
        'p': '0.10264410',
        'P': '0.00000000',
        'F': '0.00000000',
        'g': -1,
        'C': '',
        'x': 'NEW',
        'X': 'NEW',
        'r': 'NONE',
        'i': 4293153,
        'l': '0.00000000',
        'z': '0.00000000',
        'L': '0.00000000',
        'n': '0',
        'N': None,
        'T': 1499405658657,
        't': -1,
        'I': 8641984,
        'w': True,
        'm': False,
        'M': False,
        'O': 1499405658657,
        'Z': '0.00000000',
        'Y': '0.00000000',
        'Q': '0.00000000',
        'V': 'NONE',
        # Conditional fields per docs
        'd': 5000,
        'D': 1499405658657,
        'j': 123,
        'J': 456,
        'v': 100,
        'A': '0.5',
        'B': '0.25',
        'u': 12345,
        'U': 67890,
        'Cs': 'BNBETH',
        'pl': '0.1',
        'pL': '0.2',
        'pY': '0.3',
        'W': 1499405658658,
        'b': 'ONE_PARTY_TRADE_REPORT',
        'a': 5,
        'k': 'SOR',
        'uS': True,
        'gP': 'PRIMARY_PEG',
        'gOT': 'PRICE_LEVEL',
        'gOV': 1,
        'gp': '0.10000000',
        'eR': 'OCO_TRIGGER',
    }

    def expect(p):
        # Renamed scalar fields
        assert p['type'] == 'executionReport'
        assert p['event_time'] == 1499405658658
        assert p['symbol'] == 'ETHBTC'
        assert p['client_order_id'] == 'mUvoqJxFIILMdfAW5iGSOW'
        assert p['side'] == 'BUY'
        assert p['order_type'] == 'LIMIT'
        assert p['time_in_force'] == 'GTC'
        assert p['orig_quantity'] == '1.00000000'
        assert p['orig_price'] == '0.10264410'
        assert p['stop_price'] == '0.00000000'
        assert p['iceberg_quantity'] == '0.00000000'
        assert p['order_list_id'] == -1
        assert p['orig_client_order_id'] == ''
        assert p['execution_type'] == 'NEW'
        assert p['order_status'] == 'NEW'
        assert p['reject_reason'] == 'NONE'
        assert p['order_id'] == 4293153
        assert p['last_filled_qty'] == '0.00000000'
        assert p['cumulative_filled_qty'] == '0.00000000'
        assert p['last_filled_price'] == '0.00000000'
        assert p['commission_amount'] == '0'
        assert p['commission_asset'] is None
        assert p['transaction_time'] == 1499405658657
        assert p['trade_id'] == -1
        assert p['execution_id'] == 8641984
        assert p['is_on_book'] is True
        assert p['is_maker'] is False
        # Docs explicitly mark `M` as Ignore — preserved as _ignore_M.
        assert p['_ignore_M'] is False
        assert p['order_creation_time'] == 1499405658657
        assert p['cumulative_quote_qty'] == '0.00000000'
        assert p['last_quote_qty'] == '0.00000000'
        assert p['quote_order_qty'] == '0.00000000'
        assert p['stp_mode'] == 'NONE'
        # Conditional fields
        assert p['trailing_delta'] == 5000
        assert p['trailing_time'] == 1499405658657
        assert p['strategy_id'] == 123
        assert p['strategy_type'] == 456
        assert p['prevented_match_id'] == 100
        assert p['prevented_quantity'] == '0.5'
        assert p['last_prevented_quantity'] == '0.25'
        assert p['trade_group_id'] == 12345
        assert p['counter_order_id'] == 67890
        assert p['counter_symbol'] == 'BNBETH'
        assert p['prevented_execution_qty'] == '0.1'
        assert p['prevented_execution_price'] == '0.2'
        assert p['prevented_execution_quote_qty'] == '0.3'
        assert p['working_time'] == 1499405658658
        assert p['match_type'] == 'ONE_PARTY_TRADE_REPORT'
        assert p['allocation_id'] == 5
        assert p['working_floor'] == 'SOR'
        assert p['used_sor'] is True
        assert p['pegged_price_type'] == 'PRIMARY_PEG'
        assert p['pegged_offset_type'] == 'PRICE_LEVEL'
        assert p['pegged_offset_value'] == 1
        assert p['pegged_price'] == '0.10000000'
        assert p['expiry_reason'] == 'OCO_TRIGGER'

    await run_handler(client, OrderUpdateHandlerBase, payload, expect)


@pytest.mark.asyncio
async def test_order_update_surfaces_subscription_id(client):
    """The 2025-08-12 Spot CHANGELOG added a top-level ``subscriptionId``
    field to user-data events delivered via the WS-API; the SDK MUST
    preserve and surface it so multi-subscription routing works.
    Docs: https://developers.binance.com/docs/binance-spot-api-docs/CHANGELOG
    """
    from binance.core.common.utils import create_future

    future = create_future()

    class Handler(OrderUpdateHandlerBase):
        def receive(self, p):
            p = super().receive(p)
            if not future.done():
                future.set_result(p)

    client.start()
    client.handler(Handler())

    # WS-API user-data envelope: top-level subscriptionId + event dict.
    await client._receive({
        'subscriptionId': 7,
        'event': {
            'e': 'executionReport',
            'E': 1499405658658,
            's': 'ETHBTC',
            'c': 'cid',
            'S': 'BUY',
            'o': 'LIMIT',
        },
    })

    received = await future
    assert received['subscription_id'] == 7
    assert received['type'] == 'executionReport'
    assert received['symbol'] == 'ETHBTC'


# ===========================================================================
# Spot "Ignore" fields (strict-coverage acknowledgement)
# Per developers.binance.com (Spot Web-Socket Streams) the docs explicitly
# mark these fields "Ignore" but they ARE documented:
# - `M` in <symbol>@trade
# - `M` in <symbol>@aggTrade
# - `B` in <symbol>@kline_<interval>'s nested `k` object
# The SDK COLUMNS_MAP must include them (renamed to `_ignore_<key>`) so
# downstream code SEES the field exists while knowing to drop it.
# Docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
# ===========================================================================

def test_spot_trade_columns_map_includes_ignore_M():
    """Per docs, <symbol>@trade payload includes ``M`` marked "Ignore".
    The COLUMNS_MAP MUST surface it as ``_ignore_M`` so downstream code
    SEES the field exists.
    """
    from binance.spot.handlers import TRADE_COLUMNS_MAP
    assert TRADE_COLUMNS_MAP.get('M') == '_ignore_M'


def test_spot_agg_trade_columns_map_includes_ignore_M():
    """Per docs, <symbol>@aggTrade payload includes ``M`` marked "Ignore"."""
    from binance.spot.handlers import AGG_TRADE_COLUMNS_MAP
    assert AGG_TRADE_COLUMNS_MAP.get('M') == '_ignore_M'


def test_spot_kline_columns_map_includes_ignore_B():
    """Per docs, the nested ``k`` object on <symbol>@kline_<interval>
    payload includes ``B`` marked "Ignore"."""
    from binance.spot.handlers import KLINE_COLUMNS_MAP
    assert KLINE_COLUMNS_MAP.get('B') == '_ignore_B'


@pytest.mark.asyncio
async def test_spot_trade_handler_surfaces_ignore_M(client):
    from binance import TradeHandlerBase

    payload = {
        'e': 'trade',
        'E': 123456789,
        's': 'BNBBTC',
        't': 12345,
        'p': '0.001',
        'q': '100',
        'b': 88,
        'a': 50,
        'T': 123456785,
        'm': True,
        'M': True,  # docs say "Ignore"
    }

    def expect(received):
        row = received.iloc[0]
        assert row['symbol'] == 'BNBBTC'
        assert bool(row['_ignore_M']) is True

    await run_handler(client, TradeHandlerBase, payload, expect, 'bnbbtc@trade')


@pytest.mark.asyncio
async def test_spot_agg_trade_handler_surfaces_ignore_M(client):
    from binance import AggTradeHandlerBase

    payload = {
        'e': 'aggTrade',
        'E': 123456789,
        's': 'BNBBTC',
        'a': 12345,
        'p': '0.001',
        'q': '100',
        'f': 100,
        'l': 105,
        'T': 123456785,
        'm': True,
        'M': True,  # docs say "Ignore"
    }

    def expect(received):
        row = received.iloc[0]
        assert row['symbol'] == 'BNBBTC'
        assert bool(row['_ignore_M']) is True

    await run_handler(client, AggTradeHandlerBase, payload, expect, 'bnbbtc@aggTrade')


@pytest.mark.asyncio
async def test_spot_kline_handler_surfaces_ignore_B(client):
    payload = {
        'e': 'kline',
        'E': 123456789,
        's': 'BNBBTC',
        'k': {
            't': 123400000,
            'T': 123460000,
            's': 'BNBBTC',
            'i': '1m',
            'f': 100,
            'L': 200,
            'o': '0.0010',
            'c': '0.0020',
            'h': '0.0025',
            'l': '0.0009',
            'v': '1000',
            'n': 100,
            'x': False,
            'q': '1.0000',
            'V': '500',
            'Q': '0.5',
            'B': '123',  # docs say "Ignore"
        },
    }

    def expect(received):
        row = received.iloc[0]
        assert row['symbol'] == 'BNBBTC'
        assert row['_ignore_B'] == '123'

    await run_handler(client, KlineHandlerBase, payload, expect)


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
