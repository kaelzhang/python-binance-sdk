"""COIN-M Futures trading endpoint stubs.

WS-API order placement / modification / cancellation / status
(``create_order``, ``modify_order``, ``cancel_order``, ``get_order``) plus
REST trading endpoints (``create_test_order``, ``cancel_all_orders``,
``get_open_orders``, ``get_all_orders``, ``create_batch_orders``,
``cancel_batch_orders``). These are pre-declared stubs whose bodies are
replaced by ``define_getter`` at import time (see ``registry.py``).
"""

from typing import Awaitable


class CMTradingGetters:
    """Trading mixin for :class:`CMFuturesGetters`."""

    # ----- WS-API: trading --------------------------------------------------

    def create_order(self, **kwargs) -> Awaitable:
        """Places a new COIN-M Futures order over the WebSocket API.

        Weight: 1. Consumes the account ORDERS pool (``is_order=True``).

        Note:
            Pass prices and quantities as strings; the SDK rejects ``float``
            values to avoid scientific notation or precision loss.

        Args:
            symbol (str): The COIN-M futures symbol, e.g. ``'BTCUSD_PERP'``.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            type (FuturesOrderType): e.g. ``'LIMIT'``, ``'MARKET'``, ``'STOP'``.
            positionSide (:obj:`PositionSide`, optional): ``'BOTH'`` (default),
                ``'LONG'``, or ``'SHORT'`` for hedge mode.
            timeInForce (:obj:`FuturesTimeInForce`, optional): Required for
                ``LIMIT`` orders.
            quantity (:obj:`str`, optional): Order quantity (number of contracts).
            reduceOnly (:obj:`bool`, optional): Cannot be set to ``true`` in
                hedge mode; defaults to ``false``.
            price (:obj:`str`, optional): Required for ``LIMIT`` / stop-limit orders.
            newClientOrderId (:obj:`str`, optional): Unique client order id.
            stopPrice (:obj:`str`, optional): Required for ``STOP`` /
                ``TAKE_PROFIT`` orders.
            workingType (:obj:`WorkingType`, optional): ``'MARK_PRICE'`` or
                ``'CONTRACT_PRICE'``.
            priceProtect (:obj:`bool`, optional): Activates price protection
                for conditional orders.
            newOrderRespType (:obj:`str`, optional): ``'ACK'`` or ``'RESULT'``.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Order acknowledgement or result.
        """
        ...  # pragma: no cover

    def modify_order(self, **kwargs) -> Awaitable:
        """Modifies an existing COIN-M Futures order over the WebSocket API.

        Weight: 1. Consumes the account ORDERS pool (``is_order=True``).

        Args:
            symbol (str): The COIN-M futures symbol.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            orderId (:obj:`long`, optional): Order to modify.
            origClientOrderId (:obj:`str`, optional): Client order id to modify.
            quantity (str): New quantity (number of contracts).
            price (str): New price.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The modified order.
        """
        ...  # pragma: no cover

    def cancel_order(self, **kwargs) -> Awaitable:
        """Cancels an active COIN-M Futures order over the WebSocket API.

        Weight: 1.

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The cancelled order.
        """
        ...  # pragma: no cover

    def get_order(self, **kwargs) -> Awaitable:
        """Checks the status of a COIN-M Futures order over the WebSocket API.

        Weight: 1.

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The order.
        """
        ...  # pragma: no cover

    # ----- REST: trading ----------------------------------------------------

    def create_test_order(self, **kwargs) -> Awaitable:
        """Tests a new COIN-M futures order without submitting it.

        Weight: 1

        Args:
            Same parameters as ``create_order``.

        Returns:
            dict: An empty dict ``{}`` on success.
        """
        ...  # pragma: no cover

    def cancel_all_orders(self, **kwargs) -> Awaitable:
        """Cancels all open orders for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def get_open_orders(self, **kwargs) -> Awaitable:
        """Gets all open orders, or open orders for a specific COIN-M symbol.

        Weight: 1 when ``symbol`` is given; 40 otherwise.

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol. If omitted,
                returns open orders for all symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Open order records.
        """
        ...  # pragma: no cover

    def get_all_orders(self, **kwargs) -> Awaitable:
        """Gets all orders (active, cancelled, or filled) for a COIN-M symbol.

        Weight: 5

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional): Fetch orders >= this id.
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Default 500; max 1000.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Order records.
        """
        ...  # pragma: no cover

    def create_batch_orders(self, **kwargs) -> Awaitable:
        """Places multiple COIN-M orders in a single request.

        Weight: 5. Consumes the account ORDERS pool.

        Args:
            batchOrders (list): List of order parameter dicts.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Order results (one per input order).
        """
        ...  # pragma: no cover

    def cancel_batch_orders(self, **kwargs) -> Awaitable:
        """Cancels multiple COIN-M orders in a single request.

        Weight: 5

        Args:
            symbol (str): The COIN-M futures symbol.
            orderIdList (:obj:`list`, optional): List of order ids to cancel.
            origClientOrderIdList (:obj:`list`, optional): List of client
                order ids to cancel.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Cancellation results.
        """
        ...  # pragma: no cover
