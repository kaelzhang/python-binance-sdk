import json
from math import nan
from typing import Any, ClassVar, Dict, Iterable, List, Optional

from volas import DataFrame

from binance.core.common.exceptions import ReuseHandlerException
from binance.core.common.types import Payload

_MISSING = object()
_INDEX_COLUMN = '__binance_index__'


def _payload_rows(payload: Payload) -> List[dict]:
    if isinstance(payload, dict):
        return [payload]

    if not isinstance(payload, list):
        raise TypeError('stream payload must be a dict or a list of dicts')

    if not all(isinstance(row, dict) for row in payload):
        raise TypeError('stream payload list must contain dict rows')

    return payload


def _payload_columns(rows: List[dict], columns: Optional[Iterable[str]]) -> List[str]:
    if columns is not None:
        return list(columns)

    ordered: List[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                ordered.append(key)
    return ordered


def _normalize_value(value: Any) -> Any:
    if value is _MISSING or value is None:
        return _MISSING
    if isinstance(value, (dict, list, tuple)):
        try:
            return json.dumps(value, separators=(',', ':'), sort_keys=True)
        except TypeError:
            return str(value)
    return value


def _column_values(rows: List[dict], column: str) -> List[Any]:
    values = [_normalize_value(row.get(column, _MISSING)) for row in rows]
    present = [value for value in values if value is not _MISSING]

    if any(isinstance(value, str) for value in present):
        return [
            '' if value is _MISSING else str(value)
            for value in values
        ]

    if any(type(value) is bool for value in present):
        return [
            False if value is _MISSING else value
            for value in values
        ]

    return [
        nan if value is _MISSING else value
        for value in values
    ]


class Handler:
    """Internal base class for stream message handlers."""

    COLUMNS: ClassVar[Optional[Iterable[str]]] = None
    COLUMNS_MAP: ClassVar[Optional[Dict[str, str]]] = None

    def _receive(
        self,
        payload: Payload,
        index: Optional[List[int]] = None
    ) -> DataFrame:
        rows = _payload_rows(payload)
        columns = _payload_columns(rows, self.COLUMNS)
        data = {
            column: _column_values(rows, column)
            for column in columns
        }

        if index is not None and rows:
            if len(index) != len(rows):
                raise ValueError('index length must match payload row count')
            data[_INDEX_COLUMN] = index

        df = DataFrame(data)

        if index is not None and rows:
            df = df.set_index(_INDEX_COLUMN)

        if self.COLUMNS_MAP:
            return df.rename(columns=self.COLUMNS_MAP)

        return df

    def receive(self, msg):
        """Receives a single message from the stream.

        This method is usually invoked by subclass::

            class MyTickerHandler(TickerHandlerBase):
                def receive(msg):
                    # df is a volas DataFrame
                    df = super().receive(msg)
                    print(df)

        Args:
            msg (list or dict): The stream message

        Returns:
            DataFrame: the dataframe converted from `msg` with columns renamed.
        """
        return self._receive(msg)

    def __init__(self) -> None:
        self._client = None

    def set_client(self, client) -> None:
        """Bind this handler to a ``Client``; raises ``ReuseHandlerException`` if already bound."""
        if self._client:
            # If a handler used in more than one client,
            #   there will be conflicts
            raise ReuseHandlerException(self)

        self._client = client

    def receiveDispatch(self, payload):
        """Internal dispatch entry-point; delegates to ``receive`` by default."""
        return self.receive(payload)
