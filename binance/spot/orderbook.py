"""Spot local order book.

Concrete :class:`~binance.core.orderbook.OrderBook` implementation for the
Binance Spot market.  All venue-agnostic lifecycle behaviour
(snapshot fetch via ``client.get_orderbook``, diff buffering, re-fetch on
sync loss) lives in the base class; this module only encodes the spot
sequence-id validation rule (``U`` / ``u``), which is the simpler of the
two rules supported by the SDK.

The futures variant additionally validates the ``pu`` chain field; see
``binance.futures.orderbook`` (R9c) for that implementation.
"""

from binance.core.orderbook import (
    OrderBook,
    KEY_ASKS,
    KEY_BIDS,
    KEY_FIRST_UPDATE_ID,
    KEY_LAST_UPDATE_ID,
    KEY_REST_ASKS,
    KEY_REST_BIDS,
    KEY_REST_LAST_UPDATE_ID,
)

# Re-export the shared keys so existing callers that import them from this
# module (e.g. ``from binance.spot.orderbook import KEY_FIRST_UPDATE_ID``)
# continue to work without change.
__all__ = (
    'OrderBook',
    'SpotOrderBook',
    'KEY_ASKS',
    'KEY_BIDS',
    'KEY_FIRST_UPDATE_ID',
    'KEY_LAST_UPDATE_ID',
    'KEY_REST_ASKS',
    'KEY_REST_BIDS',
    'KEY_REST_LAST_UPDATE_ID',
)


class SpotOrderBook(OrderBook):
    """Concrete ``OrderBook`` for Binance Spot.

    Snapshot is fetched over the WebSocket API (``depth`` request) via the
    inherited :meth:`~binance.core.orderbook.OrderBook._fetch_snapshot`,
    which calls ``client.get_orderbook`` -- the spot client serves that
    method via WS-API, so no override is needed here.

    The sequence-id validation rule is the standard spot ``U`` / ``u`` rule
    (no ``pu`` chain field on Spot diff streams):

    * If the diff's ``u`` is ``<=`` the local ``last_update_id`` the diff is
      already applied; treat as a successful no-op.
    * If the diff's ``U`` is ``<= last_update_id + 1`` it picks up exactly
      where the local state leaves off; merge it.
    * Otherwise the diff overshoots; signal the caller to refetch the
      snapshot.
    """

    # Returns whether the payload is updated
    def _update(self, payload) -> bool:
        first = payload[KEY_FIRST_UPDATE_ID]
        last = payload[KEY_LAST_UPDATE_ID]
        current_last = self._last_update_id

        if last <= current_last:
            # abandon the payload,
            #   but however it is ok, it does not ruin the orderbook
            return True

        if first <= self._last_update_id + 1:
            # It is ok, just merge
            self._merge(last, payload[KEY_ASKS], payload[KEY_BIDS])
            self._emit_updated()
            return True

        return False
