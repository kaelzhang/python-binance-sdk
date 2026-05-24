import bisect
from typing import (
    List,
    Tuple,
    Iterable
)

Pair = Tuple[float, float]


class SequencedList(List[Pair]):
    """
    Sequenced list to maintain asks or bids.
    Each item of the list should be a tuple of `(price, quantity)`
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # For performance, just hardcode the logic to get the key
        self._key_list = [x[0] for x in self]

    # -------------------------------------------------
    # Override list methods

    def append(
        self,
        subject: Pair
    ) -> None:
        """Append ``subject`` to the end of the list.

        Also appends the price key to the internal ``_key_list`` so that
        binary-search operations stay in sync.

        Args:
            subject: A ``(price, quantity)`` pair to append.
        """
        self._key_list.append(subject[0])
        return super().append(subject)

    def pop(
        self,
        index: int
    ) -> Pair:
        """Remove and return the pair at ``index``.

        Removes the corresponding price key from ``_key_list`` to keep the
        shadow key list consistent with the underlying list.

        Args:
            index: Position of the element to remove.

        Returns:
            The ``(price, quantity)`` pair that was removed.
        """
        self._key_list.pop(index)
        return super().pop(index)

    def insert(
        self,
        index: int,
        subject: Pair
    ) -> None:
        """Insert ``subject`` before the element at ``index``.

        Inserts the price key into ``_key_list`` at the same position to keep
        the shadow key list consistent.

        Args:
            index: Position before which ``subject`` is inserted.
            subject: A ``(price, quantity)`` pair to insert.
        """
        self._key_list.insert(index, subject[0])
        return super().insert(index, subject)

    def clear(self) -> None:
        """Remove all pairs from the list and reset the internal key list."""
        self._key_list.clear()
        return super().clear()

    # ----------------------------------------------------
    # SequencedList specific methods

    # Add a new item into the list and maintain order
    def add(
        self,
        subject: Pair
    ) -> Tuple[int, bool]:
        """Insert or update a price level, maintaining ascending price order.

        Uses ``bisect_left`` on the shadow key list for O(log n) lookup.
        The Binance order-book diff protocol uses a quantity of 0 to signal
        that a price level should be removed; this method handles that case
        automatically.

        Behaviour by case:

        - Price not present, quantity > 0: the pair is inserted at the
          correct sorted position.
        - Price not present, quantity == 0: the pair is silently discarded
          (removing a level that does not exist is normal per the Binance
          spec).
        - Price already present, quantity > 0: the existing pair is replaced
          in-place.
        - Price already present, quantity == 0: the existing pair is removed.

        Args:
            subject: A ``(price, quantity)`` pair.  Quantity 0 signals
                deletion of that price level.

        Returns:
            A ``(index, replaced)`` tuple where ``index`` is the position at
            which the operation occurred (insertion point for new entries,
            position of the existing entry for updates/deletions) and
            ``replaced`` is ``True`` when an existing entry at that price was
            overwritten or removed, ``False`` when a new entry was inserted or
            a zero-quantity entry was discarded without touching the list.
        """
        # suppose the list is [[1, 1], [2, 3]]
        key = subject[0]
        quantity = subject[1]

        index = bisect.bisect_left(self._key_list, key)

        length = len(self)

        if index == length:
            if quantity != 0:
                # add [3, 1], then
                # index -> 2, insert to the right
                self.append(subject)

            # else:
            # add [3, 0], but it has 0 quantity, so abandon it
            # > Receiving an event that removes a price level that is not
            # >   in your local order book can happen and is normal.

            # insert_index, overridden
            return index, False

        origin = self[index]
        if origin[0] == key:
            if quantity == 0:
                # add [2, 0]
                # we need to remove the second item, the list will be
                # [[1, 1]]
                self.pop(index)
            else:
                # add [2, 4], then the list will be
                # [[1, 1], [2, 4]]
                self[index] = subject

            return index, True

        if quantity != 0:
            # add [0.5, 10], then
            # index -> 0, insert to the left, the list will be
            # [[0.5, 10], [1, 1], [2, 3]]
            self.insert(index, subject)

        return index, False

    # Merge a list into the current one and maintain order
    def merge(
        self,
        l: Iterable[Pair]
    ) -> None:
        """Apply a sequence of price-level updates to the list in order.

        Iterates over ``l`` and calls ``add`` for each pair, so all the
        insertion, replacement, and deletion semantics of ``add`` apply.
        Used to process a snapshot or a batch of order-book diff updates.

        Args:
            l: An iterable of ``(price, quantity)`` pairs to merge.
                Quantity 0 in any pair will remove that price level.
        """
        for subject in l:
            self.add(subject)

    def __setitem__(
        self,
        index: int,
        subject: Pair
    ) -> None:
        self._key_list[index] = subject[0]
        return super().__setitem__(index, subject)
