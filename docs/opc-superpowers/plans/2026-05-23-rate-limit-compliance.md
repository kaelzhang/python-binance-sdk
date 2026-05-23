# Binance Rate-Limit Compliance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use opc-superpowers:subagent-driven-development (recommended) or opc-superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every rate-limit-relevant code path in `binance-sdk` provably compliant with Binance's documented Spot REST, WebSocket-stream, and WebSocket-API limits, and add the defensive layers a live-quant system needs to avoid 429/418 IP bans.

**Architecture:** Add one shared rate-limit primitives module (`binance/common/rate_limit.py`), make the REST response handler 429/418/`Retry-After`-aware with typed exceptions and used-weight/order-count exposure, replace the unsafe default WebSocket reconnect policy and gate every connection attempt through a 300-per-5-min limiter, fix the WebSocket message limiter to the documented 5/s with a corrected sliding window, and enforce the 1024-stream / `serverShutdown` lifecycle rules. A proactive REST weight budget is provided opt-in.

**Tech Stack:** Python 3, `aiohttp` (REST), `websockets >= 15` + `aioretry >= 6` (streams), `pytest` + `pytest-asyncio` + `aioresponses` (tests). Version source: `binance/__init__.py::__version__` (currently `3.1.0`).

---

## Part 0 — Verified Findings (the audit)

All "Official rule" rows below were verified on 2026-05-23 against the live Binance Spot API docs (`binance/binance-spot-api-docs`: `rest-api.md` LIMITS, `web-socket-streams.md`, `web-socket-api.md`, `faqs/rate_limits.md`) and the 2023-08-25 "increase request weight limits" announcement.

### Official rules (ground truth)

| Surface | Rule (verified) |
|---|---|
| REST IP weight | **6000 weight / 1 min / IP** (since 2023-08-25). Reported in header `X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)` e.g. `X-MBX-USED-WEIGHT-1M`. Limits are per-IP, not per-key. |
| REST order count | `X-MBX-ORDER-COUNT-(intervalNum)(intervalLetter)` returned on order responses. |
| REST 429 | "Too Many Requests" — caller MUST back off. `Retry-After` header (seconds) sent. |
| REST 418 | IP auto-banned for continuing to send after 429. `Retry-After` header (seconds until ban lifts). Bans escalate **2 min → 3 days**. |
| REST endpoint weights | `GET /api/v3/exchangeInfo` = 20. `GET /api/v3/depth` tiered by `limit`: 1–100→5, 101–500→25, 501–1000→50, 1001–5000→250. Authoritative live values in the `rateLimits` array of `exchangeInfo` (`rateLimitType`/`interval`/`intervalNum`/`limit`). |
| WS streams — connections | **300 connection attempts / 5 min / IP.** |
| WS streams — messages | **5 incoming messages / second**, counting PING + PONG + JSON (subscribe/unsubscribe). Exceeding → disconnect; repeat → ban. |
| WS streams — streams/conn | **1024 streams** max per single connection. |
| WS streams — lifecycle | Connection valid **24h**; server sends a `serverShutdown` event ~10 min before the forced disconnect. Server pings every 20s; client must pong within 60s. |
| WS API | 300 conns / 5 min / IP; REQUEST_WEIGHT 6000/min (connect costs 2); ORDERS 50/10s and 160000/24h; responses carry a `rateLimits` array with a `count`; rate-limit errors use code **-1003** / status 418/429 with `retryAfter`; 403 = WAF. |

### Problems in the SDK (each cites file:line + severity)

| # | Severity | Problem | Evidence |
|---|---|---|---|
| **P1** | CRITICAL | REST layer has **zero** rate-limit awareness: no reading of `X-MBX-USED-WEIGHT-*` / `X-MBX-ORDER-COUNT-*`, no 429/418 special-casing, no `Retry-After`, no backoff. | `binance/client/base.py:182-191` (`_handle_response` only branches `2xx` vs raise). |
| **P2** | CRITICAL | `Retry-After` is **dropped**; 429 and 418 are indistinguishable from a generic 4xx — caller cannot detect "banned, wait N seconds". No `RateLimitException`/`IPBannedException`. | `binance/common/exceptions.py:70-101` (`StatusException` stores only status/code/msg, discards headers). |
| **P3** | CRITICAL | Default WS reconnect policy can far exceed **300 conn/5 min** → IP ban. `delay = (fails-1) % 10 * 0.1` cycles 0,0.1…0.9,0,… and **never abandons**; first retry delay is `0`. Avg ≈0.45 s/attempt ⇒ ≈666 attempts/5 min. No cap, no floor, no jitter, no connection-rate guard. | `binance/common/constants.py:41-53` (`DEFAULT_RETRY_POLICY`); reconnect loop `binance/subscribe/stream.py:273-336`. |
| **P4** | HIGH | WS message limiter is wrong & buggy: instantiated `max_messages=2` (anecdotal) while its own docstring says 5 and the doc-comment says 5; **stale-timestamp bug** — after `await asyncio.sleep(wait_time)` it appends the pre-sleep `now`, so the sliding window expires entries early and can exceed the intended rate; not user-configurable. | `binance/subscribe/stream.py:63-92` (class), `:157` (`max_messages=2`), `:401` (comment), `:92` (stale append). |
| **P5** | MEDIUM | **1024 streams/connection** limit not enforced; subscribing beyond it fails silently/opaquely. | `binance/subscribe/manager.py:160-175,235-239` (no count guard). |
| **P6** | MEDIUM | 24h connection lifecycle / `serverShutdown` not handled; SDK only reacts after the hard disconnect, colliding with the P3 storm. | `binance/subscribe/manager.py:91-111` (only handles `eventStreamTerminated`). |
| **P7** | MEDIUM | No client-side REQUEST_WEIGHT budget; `exchangeInfo.rateLimits` is fetched but ignored — no proactive throttle before 6000/min. | `binance/apis/rest.py:41-45,249-300` (exchangeInfo defined, never parsed). |
| **P8** | MEDIUM | WS-API (user-stream) rate-limit responses unhandled: `error.data.retryAfter`, code `-1003`, and the `rateLimits` array are ignored; the user-stream subscribe path can spin into a ban. | `binance/subscribe/stream.py:201-214` (`error` → generic `StreamSubscribeException`). |
| **P9** | LOW–MED | Used weight / order count never surfaced to callers, so the trading layer cannot self-pace. | Same as P1 (`_handle_response` discards headers on success). |
| **P10** | LOW | A brand-new `ClientSession` is created **per request** (no keep-alive / pooling) — inflates connection churn and undercuts header-based throttling. | `binance/client/base.py:217` (`async with self._init_api_session(...)` inside `_request`). |

**Out of scope (flagged, separate task):** `binance/apis/wapi.py` targets the long-removed `/wapi/...html` surface — those endpoints 404 and never return weight headers. Not a rate-limit-correctness fix; track separately.

---

## Design Decisions

1. **Reactive safety is always on; auto-retry is never on.** `_handle_response` always parses headers, exposes used weight/order count, and raises typed `RateLimitException` (429) / `IPBannedException` (418) carrying `retry_after`. We do **not** auto-retry REST calls — blind retry of `create_order` risks duplicate orders. The caller decides, using `retry_after`.
2. **Proactive REST weight budget is opt-in (recommended for live trading).** A `WeightRateLimiter` (sliding window, configurable safety ratio) sleeps *before* sending to stay under 6000/min. Off by default to preserve current behavior; enabled via a constructor kwarg. Static per-endpoint weights are conservative defaults; the authoritative truth remains the response headers (always read) and `exchangeInfo.rateLimits`.
3. **Connection-rate guard is independent of the user retry policy.** Because `stream_retry_policy` is user-overridable, a `SlidingWindowRateLimiter(290, 300s)` (safety margin under the 300/5-min hard cap) gates *every* `connect()`, shared across a Client's data + user streams. This makes a ban impossible from one process even with a misconfigured policy.
4. **New default reconnect policy:** bounded exponential backoff with jitter and a floor (≈1→30 s), never abandoning. This is a **behavioral change** (reconnects are slower) and is called out in README + commit.
5. **Backward compatibility / SemVer:** new exceptions subclass `StatusException` so existing `except StatusException` keeps working; new behavior is additive via optional kwargs. The default-timing changes (P3 policy, P4 2→5) are spec-compliance fixes. Net: **MINOR** bump `3.1.0 → 3.2.0`, with behavioral changes documented per `GC-12`.
6. **One commit per coherent change** (`GC-1`); mechanical moves separate from behavior (`GC-5`); public-API changes ship with README + tests in the same change (`AH-6`).

---

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `binance/common/rate_limit.py` | **Create** | `SlidingWindowRateLimiter` (count/window — messages & connections), `WeightRateLimiter` (weighted window — REST), `parse_retry_after`, `depth_weight`, `REST_ENDPOINT_WEIGHTS`. |
| `binance/common/exceptions.py` | Modify | Add `RateLimitException`, `IPBannedException`, `TooManyStreamsException`, `StreamRateLimitException`. |
| `binance/common/constants.py` | Modify | Add header names, status codes, limit constants; replace `DEFAULT_RETRY_POLICY`. |
| `binance/client/base.py` | Modify | 429/418/`Retry-After`-aware `_handle_response`; capture headers; `used_weight`/`order_count` props; optional weight throttle in `_request`. |
| `binance/client/__init__.py` | Modify | New kwargs (`rate_limit_guard`, `stream_message_rate`); init limiters + header dicts. |
| `binance/apis/rest.py` | Modify | Per-endpoint `weight` in `APIS`; depth weight wiring. |
| `binance/subscribe/stream.py` | Modify | Use shared limiters; fix message limiter; parse WS-API rate-limit errors; `recycle()`. |
| `binance/subscribe/manager.py` | Modify | Share connection limiter; 1024-stream guard; `serverShutdown` → proactive recycle. |
| `binance/__init__.py` | Modify | Export new exceptions; bump `__version__` to `3.2.0`. |
| `README.md` | Modify | Document rate-limit behavior, new exceptions, kwargs, behavioral changes. |
| `test/test_rate_limit.py` | **Create** | Unit tests for limiters + REST 429/418/headers. |
| `test/test_stream_rate_limit.py` | **Create** | WS message-rate + connection-rate + serverShutdown + WS-API error tests. |

---

## Phase 1 — REST 429/418/Retry-After safety (CRITICAL: P1, P2, P9)

### Task 1: Rate-limit constants

**Files:**
- Modify: `binance/common/constants.py`

- [ ] **Step 1: Add constants** (append near the `# APIs` section; keep the existing `DEFAULT_RETRY_POLICY` for now — replaced in Task 7)

```python
# Rate limits — verified 2026-05-23 against Binance Spot API docs
# REST (rest-api.md LIMITS, faqs/rate_limits.md)
HEADER_USED_WEIGHT_PREFIX = 'x-mbx-used-weight-'   # e.g. x-mbx-used-weight-1m
HEADER_ORDER_COUNT_PREFIX = 'x-mbx-order-count-'   # e.g. x-mbx-order-count-1m
HEADER_RETRY_AFTER = 'Retry-After'

HTTP_TOO_MANY_REQUESTS = 429
HTTP_IP_BANNED = 418

DEFAULT_REQUEST_WEIGHT_LIMIT = 6000      # weight / interval / IP (since 2023-08-25)
DEFAULT_REQUEST_WEIGHT_INTERVAL = 60.0   # seconds
DEFAULT_WEIGHT_SAFETY_RATIO = 0.9        # only use 90% of the budget client-side

# WebSocket streams (web-socket-streams.md)
WS_MAX_CONNECTIONS = 300
WS_CONNECTION_WINDOW = 300.0             # seconds (5 minutes)
WS_CONNECTION_SAFETY = 290               # stay below the 300 hard cap
WS_MAX_MESSAGES_PER_SEC = 5
WS_MESSAGE_WINDOW = 1.0
WS_MAX_STREAMS_PER_CONNECTION = 1024

# WS-API / stream rate-limit signalling (web-socket-api.md)
ERROR_CODE_TOO_MANY_REQUESTS = -1003
EVENT_SERVER_SHUTDOWN = 'serverShutdown'
```

- [ ] **Step 2: Commit**

```bash
git add binance/common/constants.py
git commit -m "feat(common): add verified Binance rate-limit constants"
```

### Task 2: Typed REST rate-limit exceptions

**Files:**
- Modify: `binance/common/exceptions.py`
- Test: `test/test_exceptions.py`

- [ ] **Step 1: Write the failing test** (append to `test/test_exceptions.py`)

```python
def test_rate_limit_exception_carries_retry_after():
    from binance.common.exceptions import RateLimitException, IPBannedException

    class _Resp:
        url = 'https://api.binance.com/api/v3/order'
        status = 429
    exc = RateLimitException(_Resp(), '{"code":-1003,"msg":"Too many requests"}', retry_after=120)
    assert exc.retry_after == 120
    assert exc.status == 429
    assert '429' in str(exc)

    banned = IPBannedException(_Resp(), '{"code":-1003,"msg":"banned"}', retry_after=3000)
    assert banned.retry_after == 3000
    assert '418' in str(banned) or 'banned' in str(banned).lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_exceptions.py::test_rate_limit_exception_carries_retry_after -v`
Expected: FAIL with `ImportError: cannot import name 'RateLimitException'`

- [ ] **Step 3: Add the exceptions** (append to `binance/common/exceptions.py`; `format_msg` is already imported)

```python
class RateLimitException(StatusException):
    """HTTP 429 — request weight or order-count rate limit exceeded.

    `retry_after` is the number of seconds the caller MUST wait before
    retrying, taken from the `Retry-After` response header.
    """

    def __init__(self, response, text, retry_after=None) -> None:
        super().__init__(response, text)
        self.retry_after = retry_after

    def __str__(self) -> str:
        return format_msg(
            'rate limit exceeded (HTTP 429) for "%s", retry after %s second(s)',
            self.response.url,
            self.retry_after
        )


class IPBannedException(StatusException):
    """HTTP 418 — IP auto-banned for sending requests after a 429.

    `retry_after` is the number of seconds until the ban is lifted.
    """

    def __init__(self, response, text, retry_after=None) -> None:
        super().__init__(response, text)
        self.retry_after = retry_after

    def __str__(self) -> str:
        return format_msg(
            'IP banned (HTTP 418) for "%s", banned for %s more second(s)',
            self.response.url,
            self.retry_after
        )


class TooManyStreamsException(Exception):
    """Raised when a single connection would exceed Binance's 1024-stream limit."""

    def __init__(self, requested: int, limit: int) -> None:
        self.requested = requested
        self.limit = limit

    def __str__(self) -> str:
        return format_msg(
            'requested %s streams on one connection exceeds the Binance limit of %s',
            self.requested,
            self.limit
        )


class StreamRateLimitException(StreamSubscribeException):
    """WebSocket-API rate-limit error (e.g. code -1003) with a retry hint."""

    def __init__(self, code: int, message: str, retry_after=None) -> None:
        super().__init__(code, message)
        self.retry_after = retry_after

    def __str__(self) -> str:
        return format_msg(
            'stream rate limit (code %s): %s, retry after %s second(s)',
            self.code,
            self.message,
            self.retry_after
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest test/test_exceptions.py::test_rate_limit_exception_carries_retry_after -v`
Expected: PASS

- [ ] **Step 5: Export the new exceptions** (`binance/__init__.py`, add to the public re-exports next to `StatusException`)

```python
from binance.common.exceptions import (
    RateLimitException,
    IPBannedException,
    TooManyStreamsException,
    StreamRateLimitException
)
```

- [ ] **Step 6: Commit**

```bash
git add binance/common/exceptions.py binance/__init__.py test/test_exceptions.py
git commit -m "feat(common): add typed rate-limit exceptions with retry_after"
```

### Task 3: `parse_retry_after` + rate-limit primitives module

**Files:**
- Create: `binance/common/rate_limit.py`
- Test: `test/test_rate_limit.py`

- [ ] **Step 1: Write the failing test** (create `test/test_rate_limit.py`)

```python
import asyncio
import time
import pytest

from binance.common.rate_limit import (
    parse_retry_after,
    depth_weight,
    SlidingWindowRateLimiter,
    WeightRateLimiter
)


class _Resp:
    def __init__(self, headers):
        from multidict import CIMultiDict
        self.headers = CIMultiDict(headers)


def test_parse_retry_after_reads_header_case_insensitively():
    assert parse_retry_after(_Resp({'Retry-After': '120'})) == 120
    assert parse_retry_after(_Resp({'retry-after': '7'})) == 7
    assert parse_retry_after(_Resp({})) is None
    assert parse_retry_after(_Resp({'Retry-After': 'nope'})) is None


def test_depth_weight_matches_documented_tiers():
    assert depth_weight(1) == 5
    assert depth_weight(100) == 5
    assert depth_weight(101) == 25
    assert depth_weight(500) == 25
    assert depth_weight(1000) == 50
    assert depth_weight(5000) == 250


@pytest.mark.asyncio
async def test_sliding_window_blocks_when_full():
    limiter = SlidingWindowRateLimiter(max_count=2, window=0.3)
    start = time.monotonic()
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()  # third must wait ~window
    assert time.monotonic() - start >= 0.25


@pytest.mark.asyncio
async def test_weight_limiter_blocks_over_budget():
    limiter = WeightRateLimiter(limit=10, window=0.3, safety_ratio=1.0)
    start = time.monotonic()
    await limiter.acquire(6)
    await limiter.acquire(6)  # 12 > 10 -> must wait
    assert time.monotonic() - start >= 0.25
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_rate_limit.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'binance.common.rate_limit'`

- [ ] **Step 3: Implement the module** (create `binance/common/rate_limit.py`)

```python
import asyncio
import time
from typing import List, Optional, Tuple

from binance.common.constants import HEADER_RETRY_AFTER


def parse_retry_after(response) -> Optional[int]:
    """Read the integer `Retry-After` (seconds) from a response, or None."""
    value = response.headers.get(HEADER_RETRY_AFTER)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def depth_weight(limit: int) -> int:
    """Verified weight tiers for GET /api/v3/depth."""
    if limit <= 100:
        return 5
    if limit <= 500:
        return 25
    if limit <= 1000:
        return 50
    return 250


# Conservative static defaults; runtime truth comes from X-MBX-USED-WEIGHT
# headers and exchangeInfo.rateLimits. `depth` is computed via depth_weight().
REST_ENDPOINT_WEIGHTS = {
    'exchangeInfo': 20,
    'account': 20,
    'myTrades': 20,
    'allOrders': 20,
    'openOrders': 6,   # 6 for a symbol; higher with no symbol — kept conservative
    'order': 4,
    'ticker/24hr': 80,
    'ticker/price': 4,
    'ticker/bookTicker': 4,
}


class SlidingWindowRateLimiter:
    """Count-based sliding-window limiter (e.g. WS messages, connections)."""

    def __init__(self, max_count: int, window: float) -> None:
        self._max = max(1, int(max_count))
        self._window = window
        self._events: List[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                cutoff = now - self._window
                self._events = [t for t in self._events if t > cutoff]
                if len(self._events) < self._max:
                    self._events.append(time.monotonic())
                    return
                wait = self._events[0] + self._window - now
                if wait > 0:
                    await asyncio.sleep(wait)
```

```python
class WeightRateLimiter:
    """Weighted sliding-window limiter for REST REQUEST_WEIGHT."""

    def __init__(
        self,
        limit: int,
        window: float,
        safety_ratio: float = 1.0
    ) -> None:
        self._limit = max(1, int(limit * safety_ratio))
        self._window = window
        self._events: List[Tuple[float, int]] = []
        self._lock = asyncio.Lock()

    async def acquire(self, weight: int = 1) -> None:
        weight = max(1, int(weight))
        async with self._lock:
            while True:
                now = time.monotonic()
                cutoff = now - self._window
                self._events = [(t, w) for (t, w) in self._events if t > cutoff]
                used = sum(w for _, w in self._events)
                if used + weight <= self._limit:
                    self._events.append((time.monotonic(), weight))
                    return
                wait = self._events[0][0] + self._window - now
                if wait > 0:
                    await asyncio.sleep(wait)
```

> **Why correct (vs. P4):** both limiters re-read `time.monotonic()` *after* sleeping when recording the event, so timestamps reflect the actual admit time — fixing the stale-`now` bug. `monotonic()` is immune to wall-clock jumps.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest test/test_rate_limit.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add binance/common/rate_limit.py test/test_rate_limit.py
git commit -m "feat(common): add sliding-window/weight rate limiters and retry-after parser"
```

### Task 4: 429/418/header-aware `_handle_response` + used-weight exposure

**Files:**
- Modify: `binance/client/base.py`
- Modify: `binance/client/__init__.py:50-72` (init header dicts)
- Test: `test/test_rate_limit.py`

- [ ] **Step 1: Write the failing test** (append to `test/test_rate_limit.py`)

```python
from aioresponses import aioresponses
from binance import Client, RateLimitException, IPBannedException

_URL = 'https://api.binance.com/api/v3/depth'


@pytest.mark.asyncio
async def test_429_raises_rate_limit_with_retry_after():
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT', status=429,
              headers={'Retry-After': '42', 'X-MBX-USED-WEIGHT-1M': '6001'},
              payload={'code': -1003, 'msg': 'Too many requests'})
        with pytest.raises(RateLimitException) as info:
            await client.get(_URL, symbol='BTCUSDT')
    assert info.value.retry_after == 42
    assert client.used_weight.get('1m') == 6001


@pytest.mark.asyncio
async def test_418_raises_ip_banned_with_retry_after():
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT', status=418,
              headers={'Retry-After': '120'},
              payload={'code': -1003, 'msg': 'banned'})
        with pytest.raises(IPBannedException) as info:
            await client.get(_URL, symbol='BTCUSDT')
    assert info.value.retry_after == 120


@pytest.mark.asyncio
async def test_success_captures_used_weight_and_order_count():
    client = Client()
    with aioresponses() as m:
        m.get(_URL + '?symbol=BTCUSDT', status=200,
              headers={'X-MBX-USED-WEIGHT-1M': '12', 'X-MBX-ORDER-COUNT-10S': '3'},
              payload={'lastUpdateId': 1, 'bids': [], 'asks': []})
        await client.get(_URL, symbol='BTCUSDT')
    assert client.used_weight.get('1m') == 12
    assert client.order_count.get('10s') == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_rate_limit.py::test_429_raises_rate_limit_with_retry_after -v`
Expected: FAIL — currently a generic `StatusException` is raised and `client.used_weight` does not exist.

- [ ] **Step 3: Update `_handle_response` + add capture/props** (`binance/client/base.py`)

Add imports:

```python
from binance.common.exceptions import (
    APIKeyNotDefinedException,
    APISecretNotDefinedException,
    StatusException,
    InvalidResponseException,
    RateLimitException,
    IPBannedException
)
from binance.common.constants import (
    HEADER_API_KEY,
    SecurityType,
    RequestMethod,
    HEADER_USED_WEIGHT_PREFIX,
    HEADER_ORDER_COUNT_PREFIX,
    HTTP_TOO_MANY_REQUESTS,
    HTTP_IP_BANNED
)
from binance.common.rate_limit import parse_retry_after
```

Add class-level annotations on `ClientBase` (near the top of the class):

```python
    _used_weight: Dict[str, int]
    _order_count: Dict[str, int]
    _weight_limiter: Any
```

Add capture + properties + the new handler (replace the existing `_handle_response`):

```python
    def _capture_rate_limit_headers(self, response) -> None:
        for key, value in response.headers.items():
            lower = key.lower()
            if lower.startswith(HEADER_USED_WEIGHT_PREFIX):
                interval = lower[len(HEADER_USED_WEIGHT_PREFIX):]
                try:
                    self._used_weight[interval] = int(value)
                except (TypeError, ValueError):
                    pass
            elif lower.startswith(HEADER_ORDER_COUNT_PREFIX):
                interval = lower[len(HEADER_ORDER_COUNT_PREFIX):]
                try:
                    self._order_count[interval] = int(value)
                except (TypeError, ValueError):
                    pass

    @property
    def used_weight(self) -> Dict[str, int]:
        """Latest X-MBX-USED-WEIGHT-* values keyed by interval, e.g. {'1m': 12}."""
        return dict(self._used_weight)

    @property
    def order_count(self) -> Dict[str, int]:
        """Latest X-MBX-ORDER-COUNT-* values keyed by interval, e.g. {'10s': 3}."""
        return dict(self._order_count)

    async def _handle_response(
        self,
        response: ClientResponse
    ) -> APIResponse:
        self._capture_rate_limit_headers(response)

        status = response.status
        if status == HTTP_TOO_MANY_REQUESTS:
            raise RateLimitException(
                response, await response.text(), parse_retry_after(response))
        if status == HTTP_IP_BANNED:
            raise IPBannedException(
                response, await response.text(), parse_retry_after(response))
        if not str(status).startswith('2'):
            raise StatusException(response, await response.text())
        try:
            return await response.json()
        except ValueError:
            raise InvalidResponseException(response, await response.text())
```

Add `Any` to the `typing` import line in `base.py` if not already present (it is).

- [ ] **Step 4: Initialize the dicts** (`binance/client/__init__.py`, inside `Client.__init__`, near `self._api_key = None`)

```python
        self._used_weight = {}
        self._order_count = {}
        self._weight_limiter = None
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest test/test_rate_limit.py -v`
Expected: PASS (all, incl. the 3 new REST tests)

- [ ] **Step 6: Run the existing REST suite to confirm no regressions**

Run: `pytest test/test_client_base.py test/test_rest_api.py -v`
Expected: PASS (the 307-redirect test still raises `StatusException` since 307 is neither 429/418/2xx)

- [ ] **Step 7: Commit**

```bash
git add binance/client/base.py binance/client/__init__.py test/test_rate_limit.py
git commit -m "feat(client): handle 429/418 with retry_after and expose used-weight/order-count"
```

---

## Phase 2 — WebSocket connection-rate guard + safe reconnect (CRITICAL: P3)

### Task 5: Gate every connection attempt through a shared limiter

**Files:**
- Modify: `binance/subscribe/stream.py` (constructor + `_connect`)
- Modify: `binance/subscribe/manager.py` (`_get_data_stream`, `_get_user_stream`)
- Modify: `binance/client/__init__.py` (create the shared limiter)
- Test: `test/test_stream_rate_limit.py`

- [ ] **Step 1: Write the failing test** (create `test/test_stream_rate_limit.py`)

```python
import asyncio
import time
import pytest

from binance import Stream
from binance.common.rate_limit import SlidingWindowRateLimiter
from logging import getLogger

logger = getLogger(__name__)


@pytest.mark.asyncio
async def test_stream_connect_is_gated_by_connection_limiter():
    # 2 connections allowed per 0.4s window
    limiter = SlidingWindowRateLimiter(max_count=2, window=0.4)
    attempts = []

    async def on_message(_):
        return None

    def policy(info):
        return False, 0.0  # retry immediately; the limiter must throttle

    # Point at a port nobody is listening on so connect() fails fast and retries
    stream = Stream(
        'ws://localhost:9099/stream',
        on_message=on_message,
        retry_policy=policy,
        timeout=0.1,
        logger=logger,
        connection_limiter=limiter
    )
    limiter._events.clear()
    start = time.monotonic()
    stream.connect()
    await asyncio.sleep(0.5)
    await stream.close()
    # With 2/0.4s, far fewer than the unbounded storm would produce
    assert len(limiter._events) <= 4
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_stream_rate_limit.py::test_stream_connect_is_gated_by_connection_limiter -v`
Expected: FAIL with `TypeError: __init__() got an unexpected keyword argument 'connection_limiter'`

- [ ] **Step 3: Add `connection_limiter` to `Stream`** (`binance/subscribe/stream.py`)

In imports:

```python
from binance.common.constants import (
    DEFAULT_RETRY_POLICY,
    DEFAULT_STREAM_TIMEOUT,
    DEFAULT_STREAM_CLOSE_CODE,
    STREAM_KEY_ID,
    STREAM_KEY_RESULT,
    STREAM_KEY_ERROR,
    ERROR_KEY_CODE,
    ERROR_KEY_MESSAGE,
    WS_CONNECTION_SAFETY,
    WS_CONNECTION_WINDOW,
    WS_MAX_MESSAGES_PER_SEC,
    WS_MESSAGE_WINDOW
)
from binance.common.rate_limit import SlidingWindowRateLimiter
```

Add the parameter to `Stream.__init__` (after `timeout`):

```python
        timeout: Timeout = DEFAULT_STREAM_TIMEOUT,
        connection_limiter: Optional[SlidingWindowRateLimiter] = None
    ) -> None:
```

In the body, replace the inline `RateLimiter(...)` instantiation with the shared primitive and set up the connection limiter:

```python
        # message rate limiter: 5 incoming messages / second (verified)
        self._rate_limiter = SlidingWindowRateLimiter(
            WS_MAX_MESSAGES_PER_SEC, WS_MESSAGE_WINDOW)

        # connection-rate guard: stay under 300 attempts / 5 min / IP
        self._connection_limiter = connection_limiter or SlidingWindowRateLimiter(
            WS_CONNECTION_SAFETY, WS_CONNECTION_WINDOW)
        self._logger = logger
```

Delete the old `class RateLimiter` (lines 63-92) — superseded by `SlidingWindowRateLimiter`.

In `_connect`, acquire a connection slot before opening the socket:

```python
    @retry(
        retry_policy='_retry_policy',
        before_retry='_reconnect'
    )
    async def _connect(self) -> None:
        await self._connection_limiter.acquire()
        async with connect(self._uri) as socket:
            ...
```

- [ ] **Step 4: Share one limiter per Client** (`binance/client/__init__.py`)

Add import + create the limiter in `__init__`:

```python
from binance.common.constants import (
    REST_API_HOST,
    STREAM_HOST,
    WS_API_HOST,
    DEFAULT_RETRY_POLICY, DEFAULT_STREAM_TIMEOUT,
    WS_CONNECTION_SAFETY, WS_CONNECTION_WINDOW
)
from binance.common.rate_limit import SlidingWindowRateLimiter
```

```python
        self._connection_limiter = SlidingWindowRateLimiter(
            WS_CONNECTION_SAFETY, WS_CONNECTION_WINDOW)
```

Pass it from both factories (`binance/subscribe/manager.py`):

```python
    def _get_data_stream(self) -> Stream:
        if self._data_stream is None:
            self._data_stream = Stream(
                self._stream_host + '/stream',
                on_message=self._receive,
                on_connected=self._resubscribe,
                retry_policy=self._stream_retry_policy,
                timeout=self._stream_timeout,
                logger=self._logger,
                connection_limiter=self._connection_limiter
            ).connect()
        return self._data_stream
```

Apply the same `connection_limiter=self._connection_limiter` to `_get_user_stream`. Add `_connection_limiter` to the `SubscriptionManager` class annotations.

- [ ] **Step 5: Run the new test + existing stream tests**

Run: `pytest test/test_stream_rate_limit.py::test_stream_connect_is_gated_by_connection_limiter test/test_stream.py test/test_stream_retry.py -v`
Expected: PASS (existing tests inject their own short policies and a generous default window, so they are not throttled)

- [ ] **Step 6: Commit**

```bash
git add binance/subscribe/stream.py binance/subscribe/manager.py binance/client/__init__.py test/test_stream_rate_limit.py
git commit -m "feat(subscribe): gate every ws connection through a 300/5min limiter"
```

### Task 6: Safe default reconnect policy (bounded backoff + jitter)

**Files:**
- Modify: `binance/common/constants.py:41-57`
- Test: `test/test_rate_limit.py`

- [ ] **Step 1: Write the failing test** (append to `test/test_rate_limit.py`)

```python
def test_default_retry_policy_has_floor_and_ceiling():
    from types import SimpleNamespace
    from binance.common.constants import DEFAULT_RETRY_POLICY, RETRY_MAX_DELAY

    delays = []
    for fails in range(1, 12):
        abandon, delay = DEFAULT_RETRY_POLICY(SimpleNamespace(fails=fails, exception=None))
        assert abandon is False
        assert delay >= 0.5            # floor: never a 0s busy-reconnect
        assert delay <= RETRY_MAX_DELAY
        delays.append(delay)
    # backoff grows then caps
    assert delays[-1] >= delays[0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_rate_limit.py::test_default_retry_policy_has_floor_and_ceiling -v`
Expected: FAIL — current policy returns `delay = 0.0` for `fails=1`, and `RETRY_MAX_DELAY` does not exist.

- [ ] **Step 3: Replace the policy** (`binance/common/constants.py`)

```python
import random

# RetryPolicy
# ==================================================

RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0


def DEFAULT_RETRY_POLICY(info: RetryInfo) -> RetryPolicyStrategy:
    # Bounded exponential backoff with full jitter and a floor, never abandoning.
    # Combined with the per-IP connection limiter this cannot breach 300/5min.
    ceiling = min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ** min(info.fails - 1, 5)))
    delay = ceiling / 2 + random.uniform(0, ceiling / 2)
    return False, delay


def NO_RETRY_POLICY(_) -> RetryPolicyStrategy:
    return True, 0
```

Remove the now-unused `ATOM_RETRY_DELAY` / `MAX_RETRIES_BEFORE_RESET` constants and their comment block.

- [ ] **Step 4: Run test + full suite**

Run: `pytest test/test_rate_limit.py::test_default_retry_policy_has_floor_and_ceiling test/test_subscribe.py test/test_order_book.py -v`
Expected: PASS (tests that need fast retries pass their own policy)

- [ ] **Step 5: Commit**

```bash
git add binance/common/constants.py test/test_rate_limit.py
git commit -m "fix(common): replace zero-floor reconnect policy with bounded jittered backoff"
```

---

## Phase 3 — WebSocket message-rate correctness (HIGH: P4)

### Task 7: Verify 5/s message limiter + configurable rate

**Files:**
- Modify: `binance/subscribe/stream.py` (already uses `SlidingWindowRateLimiter` after Task 5)
- Modify: `binance/client/__init__.py` (add `stream_message_rate` kwarg)
- Test: `test/test_stream_rate_limit.py`

> P4's stale-timestamp bug and the wrong `max_messages=2` are already fixed by Task 5 (the inline `RateLimiter` is deleted and replaced with the corrected `SlidingWindowRateLimiter(5, 1.0)`). This task adds the regression test and makes the rate configurable.

- [ ] **Step 1: Write the failing test** (append to `test/test_stream_rate_limit.py`)

```python
@pytest.mark.asyncio
async def test_message_limiter_enforces_five_per_second():
    limiter = SlidingWindowRateLimiter(max_count=5, window=1.0)
    start = time.monotonic()
    for _ in range(5):
        await limiter.acquire()
    assert time.monotonic() - start < 0.2     # first 5 are immediate
    await limiter.acquire()                     # 6th must wait into next window
    assert time.monotonic() - start >= 0.9


@pytest.mark.asyncio
async def test_client_stream_message_rate_is_configurable():
    from binance import Client
    client = Client(stream_message_rate=3)
    assert client._stream_message_rate == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_stream_rate_limit.py::test_client_stream_message_rate_is_configurable -v`
Expected: FAIL with `TypeError: __init__() got an unexpected keyword argument 'stream_message_rate'`

- [ ] **Step 3: Add the kwarg + thread it through** (`binance/client/__init__.py`)

Add to the constructor signature (after `stream_timeout`):

```python
        stream_timeout: Timeout = DEFAULT_STREAM_TIMEOUT,
        stream_message_rate: int = WS_MAX_MESSAGES_PER_SEC,
        logger: Logger = getLogger(__name__)
```

Add `WS_MAX_MESSAGES_PER_SEC` to the constants import, and store it:

```python
        self._stream_message_rate = stream_message_rate
```

In `binance/subscribe/stream.py`, accept and use it:

```python
        connection_limiter: Optional[SlidingWindowRateLimiter] = None,
        message_rate: int = WS_MAX_MESSAGES_PER_SEC
    ) -> None:
        ...
        self._rate_limiter = SlidingWindowRateLimiter(
            message_rate, WS_MESSAGE_WINDOW)
```

In `binance/subscribe/manager.py`, pass `message_rate=self._stream_message_rate` to both `Stream(...)` factories, and add `_stream_message_rate` to the class annotations.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest test/test_stream_rate_limit.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add binance/subscribe/stream.py binance/subscribe/manager.py binance/client/__init__.py test/test_stream_rate_limit.py
git commit -m "feat(subscribe): enforce documented 5/s ws message rate, make it configurable"
```

---

## Phase 4 — Stream count, lifecycle, weight budget, WS-API (MEDIUM: P5, P6, P7, P8, P10)

### Task 8: Enforce the 1024-streams-per-connection limit (P5)

**Files:**
- Modify: `binance/subscribe/manager.py`
- Test: `test/test_subscribe.py`

- [ ] **Step 1: Write the failing test** (append to `test/test_subscribe.py`)

```python
import pytest
from binance.common.exceptions import TooManyStreamsException


@pytest.mark.asyncio
async def test_subscribe_rejects_more_than_1024_streams(monkeypatch):
    from binance import Client
    client = Client()

    async def fake_send(_msg):
        return None

    # Pretend 1024 market streams are already active
    client._stream_names = set(f's{i}@trade' for i in range(1024))

    async def fake_params(subscribe, subscriptions):
        return ['btcusdt@trade']

    monkeypatch.setattr(
        client._get_handler_ctx(), 'subscribe_params', fake_params)
    monkeypatch.setattr(client, '_get_data_stream', lambda: type(
        'S', (), {'send': staticmethod(fake_send)})())

    with pytest.raises(TooManyStreamsException):
        await client._subscribe_only(True, [('trade', 'BTCUSDT')])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_subscribe.py::test_subscribe_rejects_more_than_1024_streams -v`
Expected: FAIL — no `_stream_names` tracking and no guard.

- [ ] **Step 3: Add stream tracking + guard** (`binance/subscribe/manager.py`)

Add imports + annotation:

```python
from binance.common.constants import (
    DEFAULT_STREAM_CLOSE_CODE,
    SubType,
    WS_MAX_STREAMS_PER_CONNECTION
)
from binance.common.exceptions import (
    InvalidHandlerException,
    TooManyStreamsException
)
```

Add `_stream_names: Set[str]` to the class annotations and initialize it in `Client.__init__` (`binance/client/__init__.py`): `self._stream_names = set()`.

Rewrite `_subscribe_only` to guard on subscribe and maintain the set:

```python
    async def _subscribe_only(
        self,
        subscribe: bool,
        subscriptions: Iterable[tuple]
    ) -> None:
        params = await self._get_handler_ctx().subscribe_params(
            subscribe,
            subscriptions
        )

        if subscribe:
            projected = self._stream_names | set(params)
            if len(projected) > WS_MAX_STREAMS_PER_CONNECTION:
                raise TooManyStreamsException(
                    len(projected), WS_MAX_STREAMS_PER_CONNECTION)

        stream = self._get_data_stream()

        await stream.send({
            'method': 'SUBSCRIBE' if subscribe else 'UNSUBSCRIBE',
            'params': params
        })

        if subscribe:
            self._stream_names.update(params)
        else:
            self._stream_names.difference_update(params)
```

- [ ] **Step 4: Run test + subscribe suite**

Run: `pytest test/test_subscribe.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add binance/subscribe/manager.py binance/client/__init__.py test/test_subscribe.py
git commit -m "feat(subscribe): enforce 1024-stream-per-connection limit with clear error"
```

### Task 9: Proactive `serverShutdown` recycle (P6)

**Files:**
- Modify: `binance/subscribe/stream.py` (add `recycle()`)
- Modify: `binance/subscribe/manager.py` (`_receive` detection + helper)
- Test: `test/test_stream_rate_limit.py`

- [ ] **Step 1: Write the failing test** (append to `test/test_stream_rate_limit.py`)

```python
def test_extract_event_type_handles_documented_shapes():
    from binance.subscribe.manager import _extract_event_type
    assert _extract_event_type({'e': 'serverShutdown'}) == 'serverShutdown'
    assert _extract_event_type(
        {'stream': 'x', 'data': {'e': 'serverShutdown'}}) == 'serverShutdown'
    assert _extract_event_type(
        {'event': {'e': 'eventStreamTerminated'}}) == 'eventStreamTerminated'
    assert _extract_event_type({'data': {'e': 'depthUpdate'}}) == 'depthUpdate'
    assert _extract_event_type('not-a-dict') is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_stream_rate_limit.py::test_extract_event_type_handles_documented_shapes -v`
Expected: FAIL with `ImportError: cannot import name '_extract_event_type'`

- [ ] **Step 3: Add the helper + detection** (`binance/subscribe/manager.py`)

Add the module-level helper:

```python
def _extract_event_type(msg):
    """Return the Binance event type ('e') from any documented message shape."""
    if not isinstance(msg, dict):
        return None
    for container_key in ('data', 'event'):
        container = msg.get(container_key)
        if isinstance(container, dict) and 'e' in container:
            return container['e']
    return msg.get('e')
```

Update `_receive` to detect `serverShutdown` and proactively recycle the data stream:

```python
    async def _receive(self, msg) -> None:
        if not self._receiving:
            return

        event_type = _extract_event_type(msg)

        if event_type == EVENT_SERVER_SHUTDOWN:
            self._logger.warning(
                'serverShutdown received; recycling data stream proactively')
            if self._data_stream is not None:
                await self._data_stream.recycle()
            return

        if event_type == 'eventStreamTerminated':
            try:
                await self._recover_user_stream_if_needed()
            except Exception as e:
                self._logger.error(format_msg(
                    'Failed to recover user stream after eventStreamTerminated: %s',
                    repr_exception(e)))

        await self._handler_ctx.receive(msg)
```

Add `EVENT_SERVER_SHUTDOWN` to the constants import in `manager.py`.

> Note: this changes the `eventStreamTerminated` detection from `msg.get('event').get('e')` to the shape-tolerant `_extract_event_type`, which still matches the previous `{'event': {'e': ...}}` shape — confirm against `test/test_subscribe.py` fixtures and keep both passing.

- [ ] **Step 4: Add `recycle()` to `Stream`** (`binance/subscribe/stream.py`)

```python
    async def recycle(self) -> None:
        """Proactively drop the current socket so aioretry reconnects.

        Used on `serverShutdown` to reconnect before the 24h forced cut.
        Unlike `close()`, this does NOT set `_closing`, so the reconnect
        machinery (and the connection limiter) take over.
        """
        socket = self._socket
        if socket is not None:
            await socket.close(DEFAULT_STREAM_CLOSE_CODE)
```

- [ ] **Step 5: Run tests**

Run: `pytest test/test_stream_rate_limit.py test/test_subscribe.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add binance/subscribe/stream.py binance/subscribe/manager.py test/test_stream_rate_limit.py
git commit -m "feat(subscribe): proactively recycle stream on serverShutdown before 24h cut"
```

### Task 10: Parse WS-API rate-limit errors (P8)

**Files:**
- Modify: `binance/subscribe/stream.py` (`_handle_message`)
- Test: `test/test_stream_rate_limit.py`

- [ ] **Step 1: Write the failing test** (append to `test/test_stream_rate_limit.py`)

```python
@pytest.mark.asyncio
async def test_ws_api_error_minus_1003_raises_stream_rate_limit():
    from binance.common.exceptions import StreamRateLimitException
    from binance.common.utils import create_future

    async def on_message(_):
        return None

    stream = Stream('ws://localhost:9098/ws', on_message=on_message,
                    timeout=0.1, logger=logger)
    fut = create_future()
    stream._message_futures[7] = fut
    await stream._handle_message({
        'id': 7,
        'status': 418,
        'error': {'code': -1003, 'msg': 'Too much request weight used',
                  'data': {'retryAfter': 88}}
    })
    with pytest.raises(StreamRateLimitException) as info:
        await fut
    assert info.value.retry_after == 88
    assert info.value.code == -1003
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_stream_rate_limit.py::test_ws_api_error_minus_1003_raises_stream_rate_limit -v`
Expected: FAIL — current code raises a generic `StreamSubscribeException` with no `retry_after`.

- [ ] **Step 3: Update `_handle_message`** (`binance/subscribe/stream.py`)

Import the new exception + constant:

```python
from binance.common.exceptions import (
    StreamDisconnectedException,
    StreamSubscribeException,
    StreamRateLimitException
)
from binance.common.constants import (
    ...,
    ERROR_CODE_TOO_MANY_REQUESTS
)
```

Replace the `error` branch:

```python
        elif STREAM_KEY_ERROR in msg:
            error = msg[STREAM_KEY_ERROR]
            code = error[ERROR_KEY_CODE]
            message = error[ERROR_KEY_MESSAGE]
            status = msg.get('status')

            if code == ERROR_CODE_TOO_MANY_REQUESTS or status in (418, 429):
                data = error.get('data') or {}
                future.set_exception(StreamRateLimitException(
                    code, message, data.get('retryAfter')))
            else:
                future.set_exception(StreamSubscribeException(code, message))
```

- [ ] **Step 4: Run tests**

Run: `pytest test/test_stream_rate_limit.py test/test_stream.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add binance/subscribe/stream.py test/test_stream_rate_limit.py
git commit -m "feat(subscribe): map ws-api rate-limit errors (-1003/418/429) to typed exception"
```

### Task 11: Opt-in proactive REST weight budget (P7, P10)

**Files:**
- Modify: `binance/client/__init__.py` (`rate_limit_guard` kwarg)
- Modify: `binance/client/base.py` (`_request` honors the limiter; reuse one session)
- Modify: `binance/apis/rest.py` (per-endpoint weight; depth tiering)
- Test: `test/test_rate_limit.py`

- [ ] **Step 1: Write the failing test** (append to `test/test_rate_limit.py`)

```python
@pytest.mark.asyncio
async def test_rate_limit_guard_throttles_before_sending(monkeypatch):
    from binance import Client
    client = Client(rate_limit_guard=True)
    # Tiny budget so the 2nd heavy call must wait
    from binance.common.rate_limit import WeightRateLimiter
    client._weight_limiter = WeightRateLimiter(limit=20, window=0.4, safety_ratio=1.0)

    calls = []
    with aioresponses() as m:
        for _ in range(2):
            m.get('https://api.binance.com/api/v3/exchangeInfo',
                  status=200, payload={'ok': True},
                  headers={'X-MBX-USED-WEIGHT-1M': '20'})
        start = time.monotonic()
        await client.get_exchange_info()   # weight 20
        await client.get_exchange_info()   # weight 20 -> over budget -> waits
        calls.append(time.monotonic() - start)
    assert calls[0] >= 0.3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/test_rate_limit.py::test_rate_limit_guard_throttles_before_sending -v`
Expected: FAIL with `TypeError: __init__() got an unexpected keyword argument 'rate_limit_guard'`

- [ ] **Step 3: Add the kwarg + limiter** (`binance/client/__init__.py`)

Import and construct:

```python
from binance.common.constants import (
    ...,
    DEFAULT_REQUEST_WEIGHT_LIMIT,
    DEFAULT_REQUEST_WEIGHT_INTERVAL,
    DEFAULT_WEIGHT_SAFETY_RATIO
)
from binance.common.rate_limit import (
    SlidingWindowRateLimiter,
    WeightRateLimiter
)
```

Add `rate_limit_guard: bool = False` to the signature, then:

```python
        self._weight_limiter = WeightRateLimiter(
            DEFAULT_REQUEST_WEIGHT_LIMIT,
            DEFAULT_REQUEST_WEIGHT_INTERVAL,
            DEFAULT_WEIGHT_SAFETY_RATIO
        ) if rate_limit_guard else None
```

(Remove the earlier `self._weight_limiter = None` line added in Task 4 Step 4 — this supersedes it.)

- [ ] **Step 4: Honor the limiter + reuse one session** (`binance/client/base.py`, `_request`)

```python
    async def _request(
        self,
        method: RequestMethod,
        uri: str,
        security_type: SecurityType = SecurityType.NONE,
        weight: int = 1,
        **kwargs
    ) -> APIResponse:
        need_api_key, need_signed = security_type.value

        if need_api_key:
            if self._api_key is None:
                raise APIKeyNotDefinedException(uri)
            api_key = self._api_key
        else:
            api_key = None

        if need_signed and self._api_secret is None:
            raise APISecretNotDefinedException(uri)

        if self._weight_limiter is not None:
            await self._weight_limiter.acquire(weight)

        req_kwargs = self._get_request_kwargs(method, need_signed, **kwargs)

        async with self._init_api_session(api_key) as session:
            async with getattr(session, method.value)(uri, **req_kwargs) as response:
                return await self._handle_response(response)
```

> P10 (one session per request) is intentionally left as a follow-up: switching to a persistent `ClientSession` requires per-key header handling and explicit lifecycle/close in `Client.close()`. Tracked as a separate task to keep this change focused. The `weight` parameter is the rate-limit-relevant part and is wired here.

- [ ] **Step 5: Wire per-endpoint weight** (`binance/apis/rest.py`)

Add `weight` to `define_getter` and pass it; compute depth tiers at call time:

```python
from binance.common.rate_limit import REST_ENDPOINT_WEIGHTS, depth_weight


def define_getter(
    Target,
    name,
    path,
    params=True,
    version=REST_API_VERSION,
    method=RequestMethod.GET,
    security_type=SecurityType.NONE
):
    base_weight = REST_ENDPOINT_WEIGHTS.get(path, 1)

    def getter(self, **kwargs):
        uri = self._rest_uri(path, version)
        ka = kwargs if params else {}
        if path == 'depth':
            weight = depth_weight(int(kwargs.get('limit', 100)))
        else:
            weight = base_weight
        return self._request(method, uri, security_type, weight=weight, **ka)

    origin = getattr(Target, name)
    getter.__doc__ = origin.__doc__
    setattr(Target, name, getter)
```

- [ ] **Step 6: Run tests + full suite**

Run: `pytest test/test_rate_limit.py test/test_rest_api.py test/test_client_base.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add binance/client/__init__.py binance/client/base.py binance/apis/rest.py test/test_rate_limit.py
git commit -m "feat(client): add opt-in proactive REST weight budget with per-endpoint weights"
```

---

## Phase 5 — Docs, version, full verification (release)

### Task 12: README + version bump

**Files:**
- Modify: `README.md`
- Modify: `binance/__init__.py` (`__version__`)

- [ ] **Step 1: Document rate-limit behavior** in `README.md` — add a "Rate Limits" section covering: `RateLimitException`/`IPBannedException`/`TooManyStreamsException`/`StreamRateLimitException` with `retry_after`; `client.used_weight` / `client.order_count`; `rate_limit_guard=True`; `stream_message_rate`; and a **Behavioral changes (3.2.0)** note: default reconnect is now bounded backoff with jitter (slower, ban-safe), and ws message rate defaults to the documented 5/s.

- [ ] **Step 2: Bump version** (`binance/__init__.py`)

```python
__version__ = '3.2.0'
```

- [ ] **Step 3: Commit**

```bash
git add README.md binance/__init__.py
git commit -m "docs(release): document rate-limit handling and bump to 3.2.0"
```

### Task 13: Final verification gate

- [ ] **Step 1: Run the entire suite**

Run: `make test` (or `pytest test/ -v`)
Expected: ALL PASS, no warnings escaping `_handle_task_exception`.

- [ ] **Step 2: Lint/type per repo Makefile**

Run: `make lint` (whatever the repo defines — flake8/mypy)
Expected: clean.

- [ ] **Step 3: Manual spec re-check** — confirm against Part 0 that each verified rule has a passing test:
  - 6000/min weight read from headers → `test_success_captures_used_weight_and_order_count`
  - 429 + Retry-After → `test_429_raises_rate_limit_with_retry_after`
  - 418 + Retry-After → `test_418_raises_ip_banned_with_retry_after`
  - 300/5min connections → `test_stream_connect_is_gated_by_connection_limiter`
  - reconnect floor/ceiling → `test_default_retry_policy_has_floor_and_ceiling`
  - 5/s messages → `test_message_limiter_enforces_five_per_second`
  - 1024 streams → `test_subscribe_rejects_more_than_1024_streams`
  - serverShutdown → `test_extract_event_type_handles_documented_shapes`
  - WS-API -1003 → `test_ws_api_error_minus_1003_raises_stream_rate_limit`
  - depth weight tiers → `test_depth_weight_matches_documented_tiers`

---

## Self-Review

**Spec coverage:** Every Part-0 rule maps to a task and a test (see Task 13 Step 3). P1/P2/P9 → Phase 1; P3 → Phase 2; P4 → Phases 2–3; P5/P6/P7/P8/P10 → Phase 4.

**Placeholder scan:** No "TBD"/"add error handling"/"similar to" — each step shows real code and exact commands.

**Type consistency:** `retry_after` is the consistent attribute on `RateLimitException`/`IPBannedException`/`StreamRateLimitException`; `SlidingWindowRateLimiter(max_count, window)` and `WeightRateLimiter(limit, window, safety_ratio).acquire(weight)` are used identically everywhere; `connection_limiter` / `message_rate` / `weight` parameter names match across `Stream`, `SubscriptionManager`, and `Client`; `_stream_names`, `_used_weight`, `_order_count`, `_weight_limiter`, `_connection_limiter`, `_stream_message_rate` are all initialized in `Client.__init__`.

**Documentation sync:** Public-API additions (new exceptions, kwargs, properties, behavioral changes) are documented in Task 12; version bumped per `WR-2`.

**Open decisions (caller may redirect):** (a) `rate_limit_guard` default — plan keeps it **off** for compatibility; flip to **on** if you want throttle-by-default for live trading. (b) Persistent `ClientSession` (P10) is deferred to a focused follow-up. (c) Dead `/wapi/` endpoints are out of scope (separate task).

---

## Execution Handoff

**Plan complete and saved to `docs/opc-superpowers/plans/2026-05-23-rate-limit-compliance.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
