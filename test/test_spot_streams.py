"""Tests for Spot stream gap fills: AllMarketTickersProcessor / AllMarketTickersHandlerBase.

Gap filled:
- S-1: !ticker@arr  — all-market 24hr full ticker array (SubType.ALL_MARKET_TICKERS)

S-2 (!bookTicker@arr) is deliberately NOT implemented: Binance deprecated and removed
this stream in 2021.  The deprecation comment lives in binance/spot/streams.py.

S-3 (!serverShutdown) is handled at the transport layer in
binance/core/transport/subscription.py (EVENT_SERVER_SHUTDOWN → recycle);
it requires no SubType or stream processor.
"""

import pytest

from binance import SpotClient, Credentials, SubType, AllMarketTickersHandlerBase
from binance.core.common.utils import create_future
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.spot.processors import AllMarketTickersProcessor


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return SpotClient(Credentials('api_key')).start()


# ---------------------------------------------------------------------------
# subscribe_param
# ---------------------------------------------------------------------------

def test_all_market_tickers_processor_subscribe_param():
    processor = AllMarketTickersProcessor(None)
    result = processor.subscribe_param(None, SubType.ALL_MARKET_TICKERS)
    assert result == '!ticker@arr'


def test_all_market_tickers_processor_subscribe_param_rejects_extra_args():
    processor = AllMarketTickersProcessor(None)
    with pytest.raises(InvalidSubTypeParamException, match='expects no parameters'):
        processor.subscribe_param(None, SubType.ALL_MARKET_TICKERS, 'BTCUSDT')


# ---------------------------------------------------------------------------
# is_message_type routing
# ---------------------------------------------------------------------------

def test_all_market_tickers_is_message_type_matches():
    processor = AllMarketTickersProcessor(None)
    payload = [{'e': '24hrTicker', 's': 'BTCUSDT'}]
    is_match, got_payload = processor.is_message_type({
        'stream': '!ticker@arr',
        'data': payload
    })
    assert is_match
    assert got_payload == payload


def test_all_market_tickers_is_message_type_no_match_miniTicker():
    processor = AllMarketTickersProcessor(None)
    is_match, _ = processor.is_message_type({
        'stream': '!miniTicker@arr',
        'data': []
    })
    assert not is_match


def test_all_market_tickers_is_message_type_no_match_window_ticker():
    processor = AllMarketTickersProcessor(None)
    is_match, _ = processor.is_message_type({
        'stream': '!ticker_1h@arr',
        'data': []
    })
    assert not is_match


def test_all_market_tickers_is_message_type_no_stream():
    processor = AllMarketTickersProcessor(None)
    is_match, _ = processor.is_message_type({'data': []})
    assert not is_match


# ---------------------------------------------------------------------------
# Handler: column mapping via client._receive
# ---------------------------------------------------------------------------

FULL_TICKER = {
    'e': '24hrTicker',
    'E': 123456789,
    's': 'BNBBTC',
    'p': '0.0015',
    'P': '25.00',
    'w': '0.0060',
    'x': '0.0045',
    'c': '0.0060',
    'Q': '1',
    'b': '0.0059',
    'B': '100',
    'a': '0.0061',
    'A': '50',
    'o': '0.0045',
    'h': '0.0080',
    'l': '0.0039',
    'v': '500000',
    'q': '3000',
    'O': 0,
    'C': 86400000,
    'F': 0,
    'L': 18150,
    'n': 18151
}


@pytest.mark.asyncio
async def test_all_market_tickers_handler_column_mapping(client):
    future = create_future()

    class MyHandler(AllMarketTickersHandlerBase):
        def receive(self, p):
            p = super().receive(p)
            if not future.done():
                future.set_result(p)

    client.handler(MyHandler())

    await client._receive({
        'stream': '!ticker@arr',
        'data': [FULL_TICKER]
    })

    df = await future
    row = df.iloc[0]

    assert row['type'] == '24hrTicker'
    assert row['event_time'] == 123456789
    assert row['symbol'] == 'BNBBTC'
    assert row['price_change'] == '0.0015'
    assert row['percent'] == '25.00'
    assert row['weighted_average_price'] == '0.0060'
    assert row['last_price'] == '0.0060'
    assert row['best_bid_price'] == '0.0059'
    assert row['best_ask_price'] == '0.0061'
    assert row['volume'] == '500000'
    assert row['quote_volume'] == '3000'
    assert row['stat_open_time'] == 0
    assert row['stat_close_time'] == 86400000
    assert row['total_trades'] == 18151


# ---------------------------------------------------------------------------
# SubType value
# ---------------------------------------------------------------------------

def test_subtype_all_market_tickers_value():
    assert str(SubType.ALL_MARKET_TICKERS) == 'allMarketTickers'
