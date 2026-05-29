"""USDⓈ-M Futures account / position endpoint stubs.

Signed user-data and TRADE endpoints covering account status, balances,
positions, commission, income, leverage brackets, margin / position-mode
toggles and user-trade history. Mix of WS-API (``get_account``,
``get_balance``, ``get_position``, ``get_position_mode``) and REST
(``get_position_risk``, ``get_commission``, ``get_income``,
``get_leverage_bracket``, leverage / margin / position-mode setters,
multi-assets mode getter / setter, ``get_user_trades``). These are
pre-declared stubs whose bodies are replaced by ``define_getter`` at import
time (see ``registry.py``).
"""

from typing import Awaitable


class UMAccountGetters:
    """Account / position mixin for :class:`UMFuturesGetters`."""

    # ----- WS-API: account --------------------------------------------------

    def get_account(self, **kwargs) -> Awaitable:
        """Gets USDⓈ-M Futures account status over the WebSocket API (v2).

        Uses ``v2/account.status`` (richer field set than the deprecated v1
        ``account.status``). Weight: 5.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Account status including balances and positions.
        """
        ...  # pragma: no cover

    def get_balance(self, **kwargs) -> Awaitable:
        """Gets USDⓈ-M Futures account balance over the WebSocket API (v2).

        Uses ``v2/account.balance`` (richer field set than the deprecated v1
        ``account.balance``). Weight: 5.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Per-asset balance records.
        """
        ...  # pragma: no cover

    def get_position(self, **kwargs) -> Awaitable:
        """Gets USDⓈ-M Futures position information over the WebSocket API.

        Distinct from REST ``get_position_risk`` (``/fapi/v3/positionRisk``);
        this uses WS-API ``account.position`` for a no-REST-round-trip query.
        Weight: 5.

        Args:
            symbol (:obj:`str`, optional): The futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Position information records.
        """
        ...  # pragma: no cover

    def get_position_mode(self, **kwargs) -> Awaitable:
        """Gets the current position mode (one-way vs hedge) via WebSocket API.

        Uses WS-API ``positionSide.dual.get``. Weight: 30.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'dualSidePosition': True/False}``
        """
        ...  # pragma: no cover

    # ----- REST: account / position -----------------------------------------

    def get_position_risk(self, **kwargs) -> Awaitable:
        """Gets position risk information.

        Weight: 5

        Args:
            symbol (:obj:`str`, optional): The futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Position risk records.
        """
        ...  # pragma: no cover

    def get_adl_quantile(self, **kwargs) -> Awaitable:
        """Gets position ADL (Auto-Deleveraging) quantile estimation.

        Weight: 5.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-ADL-Quantile-Estimation

        Each entry's ``adlQuantile`` object maps a position side
        (``LONG`` / ``SHORT`` / ``BOTH`` / ``HEDGE``) to an integer in
        ``0..4`` — the ADL queue position from low to high likelihood.
        Server cache refreshes every 30 seconds, so the value lags real
        time slightly. Used for risk monitoring in live trading.

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns ADL quantiles for all symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: ``[{'symbol': ..., 'adlQuantile': {'LONG': int, ...}}, ...]``
        """
        ...  # pragma: no cover

    def get_user_trades(self, **kwargs) -> Awaitable:
        """Gets trades for a specific account and symbol.

        Weight: 5

        Args:
            symbol (str): The futures symbol.
            orderId (:obj:`long`, optional):
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            fromId (:obj:`long`, optional): Trade id to fetch from.
            limit (:obj:`int`, optional): Default 500; max 1000.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Trade records.
        """
        ...  # pragma: no cover

    def get_commission(self, **kwargs) -> Awaitable:
        """Gets commission rates for a symbol.

        Weight: 20

        Args:
            symbol (str): The futures symbol.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Commission rate information.
        """
        ...  # pragma: no cover

    def get_income(self, **kwargs) -> Awaitable:
        """Gets income history.

        Weight: 30

        Args:
            symbol (:obj:`str`, optional): The futures symbol.
            incomeType (:obj:`str`, optional): Income type filter.
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional):
            limit (:obj:`int`, optional): Default 1000; max 1000.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Income history records.
        """
        ...  # pragma: no cover

    def get_leverage_bracket(self, **kwargs) -> Awaitable:
        """Gets leverage bracket information.

        Weight: 1

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns brackets for all symbols.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Leverage bracket records.
        """
        ...  # pragma: no cover

    def set_leverage(self, **kwargs) -> Awaitable:
        """Changes the initial leverage for a symbol.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            leverage (int): Target leverage (1–125).
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation including ``leverage`` and ``maxNotionalValue``.
        """
        ...  # pragma: no cover

    def set_margin_type(self, **kwargs) -> Awaitable:
        """Changes the margin type (ISOLATED or CROSSED) for a symbol.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            marginType (MarginType): ``'ISOLATED'`` or ``'CROSSED'``.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def set_position_margin(self, **kwargs) -> Awaitable:
        """Adjusts isolated position margin.

        Weight: 1

        Args:
            symbol (str): The futures symbol.
            positionSide (:obj:`PositionSide`, optional): ``'BOTH'``,
                ``'LONG'``, or ``'SHORT'``.
            amount (str): Margin amount.
            type (int): 1 = add; 2 = reduce.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def set_position_mode(self, **kwargs) -> Awaitable:
        """Changes the position mode (one-way vs hedge).

        Weight: 1

        Args:
            dualSidePosition (bool): ``true`` for hedge mode;
                ``false`` for one-way.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover

    def get_multi_assets_mode(self, **kwargs) -> Awaitable:
        """Gets the current multi-assets margin mode.

        Weight: 30

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'multiAssetsMargin': True/False}``
        """
        ...  # pragma: no cover

    def set_multi_assets_mode(self, **kwargs) -> Awaitable:
        """Changes the multi-assets margin mode.

        Weight: 1

        Args:
            multiAssetsMargin (bool): ``true`` to enable; ``false`` to disable.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Confirmation.
        """
        ...  # pragma: no cover
