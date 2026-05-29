"""USDⓈ-M Futures trading endpoint stubs.

WS-API order placement / modification / cancellation / status
(``create_order``, ``modify_order``, ``cancel_order``, ``get_order``,
``create_algo_order``, ``cancel_algo_order``) plus REST trading endpoints
(``create_test_order``, ``cancel_all_orders``, ``get_open_orders``,
``get_all_orders``, ``create_batch_orders``, ``cancel_batch_orders``).
These are pre-declared stubs whose bodies are replaced by ``define_getter``
at import time (see ``registry.py``).
"""

from typing import Awaitable


class UMTradingGetters:
    """Trading mixin for :class:`UMFuturesGetters`."""

    # ----- WS-API: trading --------------------------------------------------

    def create_order(self, **kwargs) -> Awaitable:
        """Places a new USDⓈ-M Futures order over the WebSocket API.

        Weight: 1. Consumes the account ORDERS pool (``is_order=True``).

        Note:
            Pass prices and quantities as strings; the SDK rejects ``float``
            values to avoid scientific notation or precision loss.

        Args:
            symbol (str): The futures symbol, e.g. ``'BTCUSDT'``.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            type (FuturesOrderType): e.g. ``'LIMIT'``, ``'MARKET'``, ``'STOP'``.
            positionSide (:obj:`PositionSide`, optional): ``'BOTH'`` (default),
                ``'LONG'``, or ``'SHORT'`` for hedge mode.
            timeInForce (:obj:`FuturesTimeInForce`, optional): Required for
                ``LIMIT`` orders.
            quantity (:obj:`str`, optional): Order quantity (base asset).
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
        """Modifies an existing USDⓈ-M Futures order over the WebSocket API.

        Weight: 1. Consumes the account ORDERS pool (``is_order=True``).

        Args:
            symbol (str): The futures symbol.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            orderId (:obj:`long`, optional): Order to modify.
            origClientOrderId (:obj:`str`, optional): Client order id to modify.
            quantity (str): New quantity.
            price (str): New price.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The modified order.
        """
        ...  # pragma: no cover

    def cancel_order(self, **kwargs) -> Awaitable:
        """Cancels an active USDⓈ-M Futures order over the WebSocket API.

        Weight: 1.

        Args:
            symbol (str): The futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The cancelled order.
        """
        ...  # pragma: no cover

    def get_order(self, **kwargs) -> Awaitable:
        """Checks the status of a USDⓈ-M Futures order over the WebSocket API.

        Weight: 1.

        Args:
            symbol (str): The futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The order.
        """
        ...  # pragma: no cover

    def create_algo_order(self, **kwargs) -> Awaitable:
        """Places an algo (TWAP/VP) order over the WebSocket API.

        Uses WS-API ``algoOrder.place`` (TRADE). Weight: 0.

        Note:
            Algo orders draw from a separate algo-orders quota, not the
            standard ORDERS pool (``is_order=False`` — no ORDERS-pool charge).

        Args:
            symbol (str): The futures symbol.
            side (OrderSide): ``'BUY'`` or ``'SELL'``.
            positionSide (:obj:`PositionSide`, optional): Hedge-mode direction.
            quantity (str): Total quantity to execute.
            duration (int): Execution duration in seconds.
            clientAlgoId (:obj:`str`, optional): Client algo order identifier.
            limitPrice (:obj:`str`, optional): Price limit for the algo.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Algo order placement acknowledgement.
        """
        ...  # pragma: no cover

    def cancel_algo_order(self, **kwargs) -> Awaitable:
        """Cancels an active algo order over the WebSocket API.

        Uses WS-API ``algoOrder.cancel`` (TRADE). Weight: 0.

        Args:
            algoId (long): The algo order id to cancel.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Cancellation confirmation.
        """
        ...  # pragma: no cover

    # ----- REST: trading ----------------------------------------------------

    def create_test_order(self, **kwargs) -> Awaitable:
        """Tests a new futures order without submitting it.

        Weight: 1

        Args:
            Same parameters as ``create_order``.

        Returns:
            dict: An empty dict ``{}`` on success.
        """
        ...  # pragma: no cover

    def cancel_all_orders(self, **kwargs) -> Awaitable:
        """Cancels all open orders for a symbol.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def countdown_cancel_all_orders(self, **kwargs) -> Awaitable:
        """Arms a dead-man's switch that cancels all open orders for
        ``symbol`` once ``countdownTime`` ms elapse without another call.

        Weight: 10.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Auto-Cancel-All-Open-Orders

        Call again periodically to refresh the timer (heartbeat). Send
        ``countdownTime=0`` to disarm. The server cancels open orders for
        the given ``symbol`` only — not across all symbols. Use this as a
        client-disconnect safety mechanism for live trading.

        Args:
            symbol (str): The futures symbol, e.g. ``'BTCUSDT'``.
            countdownTime (long): Countdown in milliseconds; ``1000`` =
                1 second. ``0`` cancels (disarms) the timer.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'symbol': <symbol>, 'countdownTime': <countdownTime>}``.
        """
        ...  # pragma: no cover

    def get_open_order(self, **kwargs) -> Awaitable:
        """Queries a single open order (singular ``/openOrder``).

        Weight: 1.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Query-Current-Open-Order

        Distinct from :py:meth:`get_open_orders` (plural). Either
        ``orderId`` or ``origClientOrderId`` MUST be supplied. Returns
        an "Order does not exist" error if the order has been filled or
        cancelled — only currently-live orders are returned.

        Args:
            symbol (str): The futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The single open order.
        """
        ...  # pragma: no cover

    def get_open_orders(self, **kwargs) -> Awaitable:
        """Gets all open orders, or open orders for a specific symbol.

        Weight: 1 when ``symbol`` is given; 40 otherwise.

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns open orders for all symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Open order records.
        """
        ...  # pragma: no cover

    def get_all_orders(self, **kwargs) -> Awaitable:
        """Gets all orders (active, cancelled, or filled) for a symbol.

        Weight: 5

        Args:
            symbol (str): The futures symbol.
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
        """Places multiple orders in a single request.

        Weight: 5. Consumes the account ORDERS pool.

        Args:
            batchOrders (list): List of order parameter dicts.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Order results (one per input order).
        """
        ...  # pragma: no cover

    def cancel_batch_orders(self, **kwargs) -> Awaitable:
        """Cancels multiple orders in a single request.

        Weight: 5

        Args:
            symbol (str): The futures symbol.
            orderIdList (:obj:`list`, optional): List of order ids to cancel.
            origClientOrderIdList (:obj:`list`, optional): List of client
                order ids to cancel.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Cancellation results.
        """
        ...  # pragma: no cover
