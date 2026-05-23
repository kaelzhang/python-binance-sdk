from dataclasses import dataclass
from enum import Enum


class RateLimitScope(str, Enum):
    IP = 'ip'
    ACCOUNT = 'account'
    CONNECTION = 'connection'


class RateLimitType(str, Enum):
    REQUEST_WEIGHT = 'request_weight'
    RAW_REQUESTS = 'raw_requests'
    ORDERS = 'orders'
    WS_CONNECTIONS = 'ws_connections'
    WS_MESSAGES = 'ws_messages'
    WS_STREAMS = 'ws_streams'


class RateLimitKind(str, Enum):
    WEIGHT = 'weight'   # cost-weighted sliding window
    COUNT = 'count'     # 1-per-event sliding window
    CAP = 'cap'         # instantaneous current-count ceiling


class EnforceMode(str, Enum):
    SLEEP = 'sleep'     # block until headroom
    RAISE = 'raise'     # raise immediately when a request would exceed
    TRACK = 'track'     # never block/raise; only account


def interval_label(seconds: float) -> str:
    if seconds <= 0:
        return ''
    if seconds % 86400 == 0:
        return f'{int(seconds // 86400)}d'
    if seconds % 3600 == 0:
        return f'{int(seconds // 3600)}h'
    if seconds % 60 == 0:
        return f'{int(seconds // 60)}m'
    return f'{int(seconds)}s'


@dataclass(frozen=True)
class RateLimitRule:
    scope: RateLimitScope
    type: RateLimitType
    interval_seconds: float
    limit: int
    kind: RateLimitKind
    enforce: EnforceMode
    safety_ratio: float = 1.0

    @property
    def interval(self) -> str:
        return interval_label(self.interval_seconds)
