"""COIN-M Futures account / position endpoint stubs.

Signed user-data and TRADE endpoints covering account status, balances,
positions, commission, income, leverage brackets, margin / position-mode
toggles and user-trade history. Mix of WS-API (``get_account``,
``get_balance``, ``get_position``) and REST (``get_position_risk``,
``get_commission``, ``get_income``, ``get_leverage_bracket``, leverage /
margin / position-mode getters and setters, ``get_user_trades``). These are
pre-declared stubs whose bodies are replaced by ``define_getter`` at import
time (see ``registry.py``).
"""

from typing import Awaitable


class CMAccountGetters:
    """Account / position mixin for :class:`CMFuturesGetters`."""

    # ----- WS-API: account --------------------------------------------------

    def get_account(self, **kwargs) -> Awaitable:
        """Gets COIN-M Futures account status over the WebSocket API.

        Weight: 5.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Account status including assets and positions.
        """
        ...  # pragma: no cover

    def get_balance(self, **kwargs) -> Awaitable:
        """Gets COIN-M Futures account balance over the WebSocket API.

        Weight: 5.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Per-asset balance records.
        """
        ...  # pragma: no cover

    def get_position(self, **kwargs) -> Awaitable:
        """Gets COIN-M Futures position information over the WebSocket API.

        Distinct from REST ``get_position_risk`` (``/dapi/v1/positionRisk``);
        this uses WS-API ``account.position`` for a no-REST-round-trip query.
        Weight: 5.

        Args:
            marginAsset (:obj:`str`, optional): The margin asset (e.g. ``'BTC'``).
            pair (:obj:`str`, optional): The underlying pair (e.g. ``'BTCUSD'``).
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Position information records.
        """
        ...  # pragma: no cover

    # ----- REST: account / position -----------------------------------------

    def get_position_risk(self, **kwargs) -> Awaitable:
        """Gets position risk information for COIN-M contracts.

        Weight: 1
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Position-Information

        Args:
            marginAsset (:obj:`str`, optional): The margin asset (e.g. ``'BTC'``).
            pair (:obj:`str`, optional): The underlying pair (e.g. ``'BTCUSD'``).
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Position risk records.
        """
        ...  # pragma: no cover

    def get_adl_quantile(self, **kwargs) -> Awaitable:
        """Gets position ADL (Auto-Deleveraging) quantile estimation for
        COIN-M contracts.

        Weight: 5.
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Position-ADL-Quantile-Estimation

        Each entry's ``adlQuantile`` object maps a position side
        (``LONG`` / ``SHORT`` / ``BOTH`` / ``HEDGE``) to an integer in
        ``0..4`` — the ADL queue position from low to high likelihood.
        Server cache refreshes every 30 seconds. Used for risk
        monitoring in live trading.

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol. If
                omitted, returns ADL quantiles for all symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: ``[{'symbol': ..., 'adlQuantile': {'LONG': int, ...}}, ...]``
        """
        ...  # pragma: no cover

    def get_user_trades(self, **kwargs) -> Awaitable:
        """Gets trades for a specific COIN-M account and symbol/pair.

        Weight: 20 with `symbol`; 40 with `pair`.
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/trade/rest-api/Account-Trade-List

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol. One of
                ``symbol`` or ``pair`` is required.
            pair (:obj:`str`, optional): The underlying pair (e.g. ``'BTCUSD'``).
                One of ``symbol`` or ``pair`` is required.
            orderId (:obj:`long`, optional):
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            fromId (:obj:`long`, optional): Trade id to fetch from.
            limit (:obj:`int`, optional): Default 50; max 1000.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Trade records.
        """
        ...  # pragma: no cover

    def get_commission(self, **kwargs) -> Awaitable:
        """Gets commission rates for a COIN-M symbol.

        Weight: 20

        Args:
            symbol (str): The COIN-M futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Commission rate information.
        """
        ...  # pragma: no cover

    def get_income(self, **kwargs) -> Awaitable:
        """Gets income history for COIN-M.

        Weight: 20
        Docs: https://developers.binance.com/docs/derivatives/coin-margined-futures/account/rest-api/Get-Income-History

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol.
            incomeType (:obj:`str`, optional): Income type filter.
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Default 100; max 1000.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Income history records.
        """
        ...  # pragma: no cover

    def get_leverage_bracket(self, **kwargs) -> Awaitable:
        """Gets leverage bracket information for COIN-M (``GET /dapi/v2/leverageBracket``).

        Weight: 1.

        v2 supersedes the deprecated v1 endpoint; the parameter is ``symbol``
        (not v1's ``pair``).

        Args:
            symbol (:obj:`str`, optional): The COIN-M futures symbol
                (e.g. ``'BTCUSD_PERP'``). If omitted, returns brackets for all
                symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Leverage bracket records.
        """
        ...  # pragma: no cover

    def set_leverage(self, **kwargs) -> Awaitable:
        """Changes the initial leverage for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol.
            leverage (int): Target leverage (1–125).
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation including ``leverage`` and ``maxQty``.
        """
        ...  # pragma: no cover

    def set_margin_type(self, **kwargs) -> Awaitable:
        """Changes the margin type (ISOLATED or CROSSED) for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol.
            marginType (MarginType): ``'ISOLATED'`` or ``'CROSSED'``.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def set_position_margin(self, **kwargs) -> Awaitable:
        """Adjusts isolated position margin for a COIN-M symbol.

        Weight: 1

        Args:
            symbol (str): The COIN-M futures symbol.
            positionSide (:obj:`PositionSide`, optional): ``'BOTH'``,
                ``'LONG'``, or ``'SHORT'``.
            amount (str): Margin amount.
            type (int): 1 = add; 2 = reduce.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def get_position_mode(self, **kwargs) -> Awaitable:
        """Gets the current position mode (one-way vs hedge) for COIN-M.

        Weight: 30

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'dualSidePosition': True/False}``
        """
        ...  # pragma: no cover

    def set_position_mode(self, **kwargs) -> Awaitable:
        """Changes the position mode (one-way vs hedge) for COIN-M.

        Weight: 1

        Args:
            dualSidePosition (bool): ``true`` for hedge mode;
                ``false`` for one-way.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover
