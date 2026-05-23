from binance.rate_limit.types import (
    RateLimitScope, RateLimitType, RateLimitKind, EnforceMode,
    RateLimitRule, interval_label
)


def test_interval_label():
    assert interval_label(1) == '1s'
    assert interval_label(10) == '10s'
    assert interval_label(60) == '1m'
    assert interval_label(300) == '5m'
    assert interval_label(3600) == '1h'
    assert interval_label(86400) == '1d'
    assert interval_label(0) == ''


def test_rule_interval_property():
    rule = RateLimitRule(
        RateLimitScope.IP, RateLimitType.REQUEST_WEIGHT,
        60.0, 6000, RateLimitKind.WEIGHT, EnforceMode.SLEEP)
    assert rule.interval == '1m'
    assert rule.safety_ratio == 1.0
