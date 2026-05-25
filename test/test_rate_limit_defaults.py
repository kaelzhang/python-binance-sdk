from binance.core.rate_limit.defaults import parse_retry_after
from binance.spot.rate_limit import depth_weight, DEFAULT_RULES
from binance.core.rate_limit.types import RateLimitType


class _Resp:
    def __init__(self, headers):
        from multidict import CIMultiDict
        self.headers = CIMultiDict(headers)


def test_parse_retry_after():
    assert parse_retry_after(_Resp({'Retry-After': '120'})) == 120
    assert parse_retry_after(_Resp({})) is None
    assert parse_retry_after(_Resp({'Retry-After': 'not-a-number'})) is None


def test_depth_weight_tiers():
    assert depth_weight(100) == 5
    assert depth_weight(101) == 25
    assert depth_weight(1000) == 50
    assert depth_weight(5000) == 250



def test_default_rules_cover_pools():
    types = {r.type for r in DEFAULT_RULES}
    assert RateLimitType.REQUEST_WEIGHT in types
    assert RateLimitType.RAW_REQUESTS in types
    assert RateLimitType.ORDERS in types
    assert RateLimitType.WS_CONNECTIONS in types
    assert sum(1 for r in DEFAULT_RULES if r.type == RateLimitType.ORDERS) == 2
