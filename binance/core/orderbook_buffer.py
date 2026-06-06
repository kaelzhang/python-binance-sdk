from dataclasses import dataclass
from typing import Any, Dict, List


DEFAULT_ORDERBOOK_BUFFER_LIMIT = 4096
DEFAULT_ORDERBOOK_REPLAY_BATCH_SIZE = 16
DEFAULT_ORDERBOOK_REPLAY_YIELD_SECONDS = 0.001


@dataclass(frozen=True)
class OrderBookBufferPolicy:
    """Backpressure policy for buffered depth diffs during snapshot sync."""

    max_pending_updates: int = DEFAULT_ORDERBOOK_BUFFER_LIMIT
    replay_batch_size: int = DEFAULT_ORDERBOOK_REPLAY_BATCH_SIZE
    replay_yield_seconds: float = DEFAULT_ORDERBOOK_REPLAY_YIELD_SECONDS

    def append(self, queue: List[Dict[str, Any]], payload: Dict[str, Any]) -> None:
        queue.append(payload)
        overflow = len(queue) - self.max_pending_updates
        if overflow > 0:
            del queue[:overflow]

    def should_yield_after(self, replayed: int) -> bool:
        return (
            self.replay_batch_size > 0
            and replayed % self.replay_batch_size == 0
        )
