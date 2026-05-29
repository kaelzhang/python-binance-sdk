"""COIN-M Futures trading endpoint stubs.

WS-API order placement / modification / cancellation / status
(``create_order``, ``modify_order``, ``cancel_order``, ``get_order``) plus
REST trading endpoints (``cancel_all_orders``, ``get_open_orders``,
``get_all_orders``, ``create_batch_orders``, ``cancel_batch_orders``).
These are pre-declared stubs whose bodies are replaced by ``define_getter``
at import time (see ``registry.py``).

Note: COIN-M does NOT expose a "Test New Order" endpoint (POST
/dapi/v1/order/test); that endpoint is documented only on UM Futures and
Spot.
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

    def countdown_cancel_all_orders(self, **kwargs) -> Awaitable:
        """Arms a dead-man's switch that cancels all open orders for
        ``symbol`` once ``countdownTime`` ms elapse without another call.

        Weight: 10.
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Auto-Cancel-All-Open-Orders

        Call again periodically to refresh the timer (heartbeat). Send
        ``countdownTime=0`` to disarm. Same semantics as the UM
        equivalent — the server cancels open orders for the given
        ``symbol`` only.

        Args:
            symbol (str): The COIN-M futures symbol, e.g. ``'BTCUSD_PERP'``.
            countdownTime (long): Countdown in milliseconds; ``1000`` =
                1 second. ``0`` cancels (disarms) the timer.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'symbol': <symbol>, 'countdownTime': <countdownTime>}``.
        """
        ...  # pragma: no cover

    def get_open_order(self, **kwargs) -> Awaitable:
        """Queries a single open COIN-M order (singular ``/openOrder``).

        Weight: 1.
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Query-Current-Open-Order

        Distinct from :py:meth:`get_open_orders` (plural). Either
        ``orderId`` or ``origClientOrderId`` MUST be supplied. Returns
        an "Order does not exist" error if the order has been filled or
        cancelled.

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: The single open order.
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
        """Gets all orders (active, cancelled, or filled) for a COIN-M contract.

        Weight: 20 with `symbol`; 40 with `pair`.
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/All-Orders

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol. One of
                ``symbol`` or ``pair`` is required.
            pair (:obj:`str`, optional): The underlying pair (e.g. ``'BTCUSD'``).
                One of ``symbol`` or ``pair`` is required.
            orderId (:obj:`long`, optional): Fetch orders >= this id.
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Default 50; max 100.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Order records.
        """
        ...  # pragma: no cover

    def get_order_modify_history(self, **kwargs) -> Awaitable:
        """Gets the price/quantity amendment chain for one COIN-M order
        (``GET /dapi/v1/orderAmendment``).

        Weight: 1.
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Get-Order-Modify-History

        Either ``orderId`` or ``origClientOrderId`` MUST be supplied;
        ``orderId`` wins if both are sent. Server retains amendments
        for 3 months only.

        Args:
            symbol (str): The COIN-M futures symbol.
            orderId (:obj:`long`, optional): Either this or
                ``origClientOrderId`` must be sent.
            origClientOrderId (:obj:`str`, optional):
            startTime (:obj:`long`, optional): Inclusive lower bound.
            endTime (:obj:`long`, optional): Inclusive upper bound.
            limit (:obj:`int`, optional): Default 50; max 100.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Amendment records — each with ``amendmentId``,
            ``symbol``, ``pair``, ``orderId``, ``clientOrderId``,
            ``time``, and an ``amendment`` object.
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

    def modify_batch_orders(self, **kwargs) -> Awaitable:
        """Modifies multiple existing COIN-M Futures orders in one request
        (``PUT /dapi/v1/batchOrders``).

        Weight: 5. Consumes the account ORDERS pool (``is_order=True``);
        modifications count against the same CM 1-min ORDERS pool as new
        orders.
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Modify-Multiple-Orders

        Companion to :py:meth:`create_batch_orders` /
        :py:meth:`cancel_batch_orders`. Each entry in ``batchOrders`` MUST
        identify the order via ``orderId`` OR ``origClientOrderId`` and
        supply the new ``symbol`` / ``side`` / ``quantity`` / ``price``
        (per docs).

        Args:
            batchOrders (list): List of order modification dicts (max 5).
                Each dict requires ``symbol``, ``side``, ``quantity``,
                ``price``, and ``orderId`` OR ``origClientOrderId``.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: One result per input modification.
        """
        ...  # pragma: no cover

    def cancel_batch_orders(self, **kwargs) -> Awaitable:
        """Cancels multiple COIN-M orders in a single request.

        Weight: 1
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Cancel-Multiple-Orders

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

    def get_force_orders(self, **kwargs) -> Awaitable:
        """Queries the caller's COIN-M force-order (liquidation / ADL)
        history (``GET /dapi/v1/forceOrders``).

        Weight: 20 when ``symbol`` is given; 50 otherwise.
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Users-Force-Orders

        Returns force-order events (auto-liquidation or ADL) for the user.
        Query window is the last 90 days only; ``startTime`` / ``endTime``
        further beyond 90 days will be rejected by the server.

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol. If
                omitted, returns force orders across all symbols (weight
                50 — use with care).
            autoCloseType (:obj:`str`, optional): ``'LIQUIDATION'`` or
                ``'ADL'``; returns both kinds if omitted.
            startTime (:obj:`long`, optional): Inclusive lower bound (ms).
            endTime (:obj:`long`, optional): Inclusive upper bound (ms).
            limit (:obj:`int`, optional): Default 50; max 100.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Force-order records (price, quantity, status, etc.).
        """
        ...  # pragma: no cover
