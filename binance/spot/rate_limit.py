"""Spot market default rate-limit rules.

The rate-limit *engine* (RateLimiter / buckets / rule types) is market-agnostic
and lives in :mod:`binance.core.rate_limit`. This module pins the Spot market's
default rule set; a per-client :class:`~binance.core.rate_limit.RateLimiter` is
built from it. (The numeric limits are reconciled at runtime against response
headers and ``exchangeInfo``.)

Verified 2026-05-23 against the Binance Spot API docs; the canonical Spot pool
definitions are authored in :data:`binance.core.rate_limit.defaults.DEFAULT_RULES`.
"""

from binance.core.rate_limit.defaults import DEFAULT_RULES

__all__ = ['DEFAULT_RULES']
