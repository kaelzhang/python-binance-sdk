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


# ---------------------------------------------------------------------------
# Spot ORDERS pool caps: 50 / 10s and 160000 / 1d
# Docs: https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/rate-limits
# ---------------------------------------------------------------------------

def test_spot_default_orders_10s_limit_is_50():
    """ORDERS SECOND x10 -> 50 per docs."""
    from binance.spot.constants import (
        DEFAULT_ORDERS_10S_LIMIT, DEFAULT_ORDERS_10S_INTERVAL,
    )
    assert DEFAULT_ORDERS_10S_LIMIT == 50
    assert DEFAULT_ORDERS_10S_INTERVAL == 10.0


def test_spot_default_orders_1d_limit_is_160000():
    """ORDERS DAY x1 -> 160000 per docs."""
    from binance.spot.constants import (
        DEFAULT_ORDERS_1D_LIMIT, DEFAULT_ORDERS_1D_INTERVAL,
    )
    assert DEFAULT_ORDERS_1D_LIMIT == 160000
    assert DEFAULT_ORDERS_1D_INTERVAL == 86400.0


def test_spot_default_rules_orders_caps_match_docs():
    """The Spot DEFAULT_RULES tuple wires the docs-correct ORDERS caps."""
    orders = [r for r in DEFAULT_RULES if r.type == RateLimitType.ORDERS]
    by_interval = {r.interval_seconds: r.limit for r in orders}
    assert by_interval[10.0] == 50
    assert by_interval[86400.0] == 160000


# ---------------------------------------------------------------------------
# WS message-rate limit splits per market (Spot 5/s, futures 10/s).
# Docs:
#  - Spot WS streams:    https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
#  - UM market streams:  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams
#  - CM market streams:  https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams
# ---------------------------------------------------------------------------

def test_spot_ws_max_messages_per_sec_is_5():
    from binance.spot.constants import WS_MAX_MESSAGES_PER_SEC
    assert WS_MAX_MESSAGES_PER_SEC == 5


def test_um_ws_max_messages_per_sec_is_10():
    from binance.futures.um.constants import WS_MAX_MESSAGES_PER_SEC
    assert WS_MAX_MESSAGES_PER_SEC == 10


def test_cm_ws_max_messages_per_sec_is_10():
    from binance.futures.cm.constants import WS_MAX_MESSAGES_PER_SEC
    assert WS_MAX_MESSAGES_PER_SEC == 10


def test_core_common_constants_no_longer_exports_ws_max_messages_per_sec():
    """The market-specific limit must NOT live in the market-agnostic module."""
    import binance.core.common.constants as core_constants
    assert not hasattr(core_constants, 'WS_MAX_MESSAGES_PER_SEC'), (
        'WS_MAX_MESSAGES_PER_SEC is market-specific; declare it in each '
        'market constants module (spot/futures-um/futures-cm) instead.'
    )


def test_spot_client_per_connection_ws_message_bucket_caps_at_5():
    """SpotClient's RateLimiter registers a per-connection ws-messages bucket
    capped at 5/s (Spot WS streams docs)."""
    from binance import SpotClient
    client = SpotClient()
    client._rate_limiter.register_connection('c1')
    msgs = [
        w for w in client._rate_limiter.snapshot().windows
        if w.type == RateLimitType.WS_MESSAGES
    ]
    assert msgs and msgs[0].limit == 5


def test_um_client_per_connection_ws_message_bucket_caps_at_10():
    """UMFuturesClient's RateLimiter registers a per-connection ws-messages
    bucket capped at 10/s (USDⓈ-M docs)."""
    from binance import UMFuturesClient
    client = UMFuturesClient()
    client._rate_limiter.register_connection('c1')
    msgs = [
        w for w in client._rate_limiter.snapshot().windows
        if w.type == RateLimitType.WS_MESSAGES
    ]
    assert msgs and msgs[0].limit == 10


def test_cm_client_per_connection_ws_message_bucket_caps_at_10():
    """CMFuturesClient's RateLimiter registers a per-connection ws-messages
    bucket capped at 10/s (COIN-M docs)."""
    from binance import CMFuturesClient
    client = CMFuturesClient()
    client._rate_limiter.register_connection('c1')
    msgs = [
        w for w in client._rate_limiter.snapshot().windows
        if w.type == RateLimitType.WS_MESSAGES
    ]
    assert msgs and msgs[0].limit == 10
