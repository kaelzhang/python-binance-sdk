import importlib
import traceback

from binance.common.constants import STREAM_TYPE_MAP, STREAM_OHLC_MAP

__all__ = [
    'HandlerExceptionHandlerBase',
    'TradeHandlerBase',
    'AggTradeHandlerBase',
    'OrderBookHandlerBase',
    'KlineHandlerBase',
    'MiniTickerHandlerBase',
    'TickerHandlerBase',
    'AllMarketMiniTickersHandlerBase',
    'AllMarketTickersHandlerBase',
    # 'AccountInfoHandlerBase',
    # 'AccountPositionHandlerBase',
    # 'BalanceUpdateHandlerBase',
    # 'OrderUpdateHandlerBase',
    # 'OrderListStatusHandlerBase'
]

class HandlerBase(object):
    def __init__(self):
        self._client = None

    def _receive(self, res, index=[0]):
        return pd.DataFrame(
            res, columns=self.COLUMNS, index=index
        ).rename(columns=self.COLUMNS_MAP)

try:
    pd = importlib.import_module('pandas')
    HandlerBase.receive = lambda self, res: self._receive(res)

except ModuleNotFoundError:
    # If pandas is not installed
    HandlerBase.receive = lambda self, res: res
except Exception as e:
    raise e

class HandlerExceptionHandlerBase(HandlerBase):
    def receive(self, e):
        traceback.print_exc()
        return e

BASE_TRADE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    'p': 'price',
    'q': 'quantity',
    'T': 'trade_time',
    'm': 'is_maker'
}

TRADE_COLUMNS_MAP = {
    **BASE_TRADE_COLUMNS_MAP,
    't': 'trade_id',
    'b': 'buyer_order_id',
    'a': 'seller_order_id'
}

TRADE_COLUMNS = TRADE_COLUMNS_MAP.keys()

class TradeHandlerBase(HandlerBase):
    COLUMNS_MAP = TRADE_COLUMNS_MAP
    COLUMNS = TRADE_COLUMNS

AGG_TRADE_COLUMNS_MAP = {
    **BASE_TRADE_COLUMNS_MAP,
    'a': 'agg_trade_id',
    'f': 'first_trade_id',
    'l': 'last_trade_id',
}

AGG_TRADE_COLUMNS = AGG_TRADE_COLUMNS_MAP

class AggTradeHandlerBase(HandlerBase):
    COLUMNS_MAP = AGG_TRADE_COLUMNS_MAP
    COLUMNS = AGG_TRADE_COLUMNS

ORDER_BOOK_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    'U': 'first_update_id',
    'u': 'last_update_id'
}

ORDER_BOOK_COLUMNS = ORDER_BOOK_COLUMNS_MAP.keys()

def create_depth_df(l):
    return pd.DataFrame([
        {'price': x[0], 'quantity': x[1]} for x in l
    ])

class OrderBookHandlerBase(HandlerBase):
    COLUMNS_MAP = ORDER_BOOK_COLUMNS_MAP
    COLUMNS = ORDER_BOOK_COLUMNS

    def _receive(self, res):
        info = super(OrderBookHandlerBase, self)._receive(res)

        bids = create_depth_df(res['b'])
        asks = create_depth_df(res['a'])

        return info, [bids, asks]

KLINE_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    't': 'open_time',
    'T': 'close_time',
    's': 'symbol',
    'i': 'interval',
    'f': 'first_trade_id',
    'L': 'last_trade_id',
    **STREAM_OHLC_MAP,
    'x': 'is_closed',
    'v': 'volume',
    'q': 'quote_volume',
    'V': 'taker_volume',
    'Q': 'taker_quote_volume',
    'n': 'total_trades'
}

KLINE_COLUMNS = KLINE_COLUMNS_MAP.keys()

class KlineHandlerBase(HandlerBase):
    COLUMNS_MAP = KLINE_COLUMNS_MAP
    COLUMNS = KLINE_COLUMNS

    def _receive(self, res):
        k = res['k']
        k['E'] = res['E']

        return super(KlineHandlerBase, self)._receive(k)

MINI_TICKER_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    **STREAM_OHLC_MAP,
    'v': 'volume',
    'q': 'quote_volume',
}

MINI_TICKER_COLUMNS = MINI_TICKER_COLUMNS_MAP.keys()

class MiniTickerHandlerBase(HandlerBase):
    COLUMNS_MAP = MINI_TICKER_COLUMNS_MAP
    COLUMNS = MINI_TICKER_COLUMNS

TICKER_COLUMNS_MAP = {
    **MINI_TICKER_COLUMNS_MAP,
    'p': 'price',
    'P': 'percent',
    'w': 'weighted_average_price',
    'x': 'first_trade_price',
    'Q': 'last_quantity',
    'b': 'best_bid_price',
    'B': 'best_bid_quantity',
    'O': 'stat_open_time',
    'C': 'stat_close_time',
    'F': 'first_trade_id',
    'L': 'last_trade_id',
    'n': 'total_trades'
}

TICKER_COLUMNS = TICKER_COLUMNS_MAP.keys()

class TickerHandlerBase(HandlerBase):
    COLUMNS_MAP = TICKER_COLUMNS_MAP
    COLUMNS = TICKER_COLUMNS

class AllMarketMiniTickersHandlerBase(HandlerBase):
    COLUMNS_MAP = MINI_TICKER_COLUMNS_MAP
    COLUMNS = MINI_TICKER_COLUMNS

    def _receive(self, res):
        return super(AllMarketMiniTickersHandlerBase, self)._receive(
            res, None)

class AllMarketTickersHandlerBase(HandlerBase):
    COLUMNS_MAP = TICKER_COLUMNS_MAP
    COLUMNS = TICKER_COLUMNS

    def _receive(self, res):
        return super(AllMarketTickersHandlerBase, self)._receive(
            res, None)

class AccountInfoHandlerBase(HandlerBase):
    pass

class AccountPositionHandlerBase(HandlerBase):
    pass

class BalanceUpdateHandlerBase(HandlerBase):
    pass

class OrderUpdateHandlerBase(HandlerBase):
    pass

class OrderListStatusHandlerBase(HandlerBase):
    pass
