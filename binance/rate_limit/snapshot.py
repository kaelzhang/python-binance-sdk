from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class RateLimitWindow:
    scope: str
    type: str
    interval: str
    used: int
    limit: int
    remaining: int
    utilization: float
    pending: int
    source: str          # 'header' (authoritative) | 'client' (estimate)


@dataclass(frozen=True)
class RateLimitSnapshot:
    windows: Tuple[RateLimitWindow, ...]
    pending: int
    retry_after: Optional[int]
    throttled: bool
    at: float

    @property
    def max_utilization(self) -> float:
        return max((w.utilization for w in self.windows), default=0.0)
