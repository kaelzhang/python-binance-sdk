"""Futures local order book (USDⓈ-M + COIN-M).

Concrete :class:`~binance.core.orderbook.OrderBook` implementation for the
Binance Futures markets (USDⓈ-M and COIN-M).  All venue-agnostic lifecycle
behaviour (snapshot fetch via ``client.get_orderbook``, diff buffering,
re-fetch on sync loss) lives in the base class; this module only encodes the
futures sequence-id validation rule.

The futures rule additionally validates the ``pu`` chain field, which spot
does not carry: each diff event after the snapshot must reference the ``u``
of the previously applied event, forming a strict chain.

The same class serves USDⓈ-M (``UMFuturesClient``) and COIN-M
(``CMFuturesClient``) -- the only differences (REST host, symbol format,
weight tables) are encapsulated at the client / endpoint layer.

References:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
    https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
"""

from binance.core.orderbook import (
    OrderBook,
    KEY_ASKS,
    KEY_BIDS,
    KEY_FIRST_UPDATE_ID,
    KEY_LAST_UPDATE_ID,
)

# Futures-specific diff field: ``pu`` is the ``u`` (final update id) of the
# previous diff event on the same symbol.  Spot diffs do NOT carry this field.
KEY_PREVIOUS_LAST_UPDATE_ID = 'pu'


__all__ = (
    'FuturesOrderBook',
    'KEY_PREVIOUS_LAST_UPDATE_ID',
)


class FuturesOrderBook(OrderBook):
    """Concrete ``OrderBook`` for Binance Futures (USDⓈ-M and COIN-M).

    Snapshot is fetched via REST (the inherited
    :meth:`~binance.core.orderbook.OrderBook._fetch_snapshot` calls
    ``client.get_orderbook(symbol=, limit=)``, which both futures clients
    expose as a REST endpoint -- ``GET /fapi/v1/depth`` for USDⓈ-M and
    ``GET /dapi/v1/depth`` for COIN-M).

    The sequence-id validation rule is the standard futures rule:

    * **Stale event** (``u < last_update_id``): treat as a no-op success;
      the diff has already been incorporated into the local state.
    * **First post-snapshot event** must straddle the snapshot id, i.e.
      ``U <= last_update_id <= u``.  After it is applied, the chain begins.
    * **Subsequent events** must satisfy ``pu == last_update_id`` -- i.e.
      reference the ``u`` of the previously applied event.  Any break in the
      chain is unrecoverable from streaming alone and triggers a full
      snapshot re-fetch via the base class.

    Implementation note -- "first vs subsequent" detection:
        We track a private ``_post_snapshot_first`` flag set to ``True`` in
        ``__init__`` and after each ``_fetch_snapshot`` (the base class
        clears the unsolved queue and resets ``_last_update_id`` via
        ``_merge``, but does not touch this flag, so we override
        ``_fetch_snapshot`` to reset it).  The flag is cleared once we
        successfully apply the first straddle event.  This avoids relying on
        ``pu`` being absent on the first event (some payloads carry ``pu``
        even on the first one) and keeps the chain check unambiguous on the
        subsequent events.
    """

    _post_snapshot_first: bool

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._post_snapshot_first = True

    async def _fetch_snapshot(self) -> None:  # type: ignore[override]
        # Reset the "first post-snapshot event" flag before delegating to
        # the base implementation.  After a successful fetch the next live
        # diff must again straddle the snapshot's ``lastUpdateId`` to
        # become the chain anchor.
        self._post_snapshot_first = True
        await super()._fetch_snapshot()  # type: ignore[misc]

    def _update(self, payload) -> bool:
        first = payload[KEY_FIRST_UPDATE_ID]
        last = payload[KEY_LAST_UPDATE_ID]
        current_last = self._last_update_id

        if last < current_last:
            # Stale diff -- already covered by a later event we've applied.
            # Treat as a successful no-op so the caller does NOT resync.
            return True

        if self._post_snapshot_first:
            # First event after a fresh snapshot must straddle the snapshot
            # id: ``U <= snapshot.lastUpdateId <= u``.  ``current_last`` was
            # set to ``snapshot.lastUpdateId`` by ``_merge`` in
            # ``_fetch_snapshot`` just before this call.
            if first <= current_last <= last:
                self._merge(last, payload[KEY_ASKS], payload[KEY_BIDS])
                self._post_snapshot_first = False
                self._emit_updated()
                return True

            # The first event does not straddle: out of sync, must resync.
            return False

        # Subsequent events: the ``pu`` chain field must point to the ``u``
        # of the previous event we applied (== current_last).  Missing
        # ``pu`` on a non-first event is a protocol violation; resync.
        previous = payload.get(KEY_PREVIOUS_LAST_UPDATE_ID)
        if previous != current_last:
            return False

        self._merge(last, payload[KEY_ASKS], payload[KEY_BIDS])
        self._emit_updated()
        return True
