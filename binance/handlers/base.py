from typing import ClassVar, Dict, Iterable, List, Optional

from stock_pandas import StockDataFrame

from binance.common.exceptions import ReuseHandlerException
from binance.common.types import Payload


class Handler:
    """Internal base class for stream message handlers."""

    COLUMNS: ClassVar[Optional[Iterable[str]]] = None
    COLUMNS_MAP: ClassVar[Optional[Dict[str, str]]] = None

    def _receive(
        self,
        payload: Payload,
        index: Optional[List[int]] = None
    ) -> StockDataFrame:
        if index is None:
            index = [0]
        return StockDataFrame(
            payload, columns=self.COLUMNS, index=index
        ).rename(columns=self.COLUMNS_MAP)

    def receive(self, msg):
        """Receives a single message from the stream.

        This method is usually invoked by subclass::

            class MyTickerHandler(TickerHandlerBase):
                def receive(msg):
                    # df is a StockDataFrame
                    df = super().receive(msg)
                    print(df)

        Args:
            msg (list or dict): The stream message

        Returns:
            StockDataFrame: the dataframe converted from `msg` with columns renamed.
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
