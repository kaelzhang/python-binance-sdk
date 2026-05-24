from typing import List

from stock_pandas import StockDataFrame

from binance.common.exceptions import ReuseHandlerException
from binance.common.types import Payload


class Handler:
    """The handler class to receive stream messages.

    Most usually, except `OrderBookHandlerBase`, we need to override the `receive()` method::

        class MyTickerHandler(TickerHandlerBase):
            def receive(self, msg):
                print('ticker', msg)
    """

    COLUMNS = None
    COLUMNS_MAP = None

    def _receive(
        self,
        payload: Payload,
        index: List[int] = [0]
    ) -> StockDataFrame:
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
        """Bind this handler to a ``Client`` instance.

        Called automatically by the SDK when the handler is registered with a
        client via ``client.handler()``.  A handler may only be bound to one
        client at a time; attempting to register the same handler instance with
        a second client raises ``ReuseHandlerException``.

        Args:
            client: The ``binance.Client`` instance that owns this handler.

        Raises:
            ReuseHandlerException: If the handler has already been bound to a
                different client.
        """
        if self._client:
            # If a handler used in more than one client,
            #   there will be conflicts
            raise ReuseHandlerException(self)

        self._client = client

    def receiveDispatch(self, payload):
        """Framework entry-point called by the stream processor to deliver a payload.

        This is the internal dispatch method invoked by the SDK's processor
        layer for each incoming stream message that matches this handler's
        subscription type.  The default implementation delegates directly to
        ``receive``; subclasses such as ``OrderBookHandlerBase`` override it
        to perform additional bookkeeping (e.g. updating the local order book)
        before (or instead of) calling ``receive``.

        You should not call this method directly; override ``receive`` instead.

        Args:
            payload: The raw stream message payload.

        Returns:
            The return value of ``receive(payload)``.
        """
        return self.receive(payload)
