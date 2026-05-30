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

    def get_account_rest_v3(self, **kwargs) -> Awaitable:
        """Gets USDⓈ-M Futures account information via REST V3
        (``GET /fapi/v3/account``).

        Weight: 5.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Information-V3

        REST V3 carries a richer aggregated-account field set than the
        WS-API V2 (:py:meth:`get_account`); both surfaces stay available
        so callers pick the latency / richness tradeoff. CM has no V3
        equivalent.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Account snapshot with ``totalInitialMargin``,
            ``totalMaintMargin``, ``totalWalletBalance``,
            ``totalUnrealizedProfit``, ``totalMarginBalance``,
            ``assets`` (per-asset detail), and ``positions``.
        """
        ...  # pragma: no cover

    def get_balance_rest_v3(self, **kwargs) -> Awaitable:
        """Gets USDⓈ-M Futures balance via REST V3
        (``GET /fapi/v3/balance``).

        Weight: 5.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Futures-Account-Balance-V3

        See :py:meth:`get_account_rest_v3` for the V3-vs-V2 rationale.

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Per-asset balance records with ``accountAlias``,
            ``asset``, ``balance``, ``crossWalletBalance``, ``crossUnPnl``,
            ``availableBalance``, ``maxWithdrawAmount``,
            ``marginAvailable``, ``updateTime``.
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

    def get_position_margin_history(self, **kwargs) -> Awaitable:
        """Gets isolated-margin add/reduce history for a symbol.

        Weight: 1.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Get-Position-Margin-Change-History

        Server retains 30 days of history; the span between ``startTime``
        and ``endTime`` cannot exceed 30 days. Companion read endpoint
        to ``set_position_margin``.

        Args:
            symbol (str): The futures symbol.
            type (:obj:`int`, optional): ``1`` = add margin events only;
                ``2`` = reduce margin events only.
            startTime (:obj:`long`, optional):
            endTime (:obj:`long`, optional): Defaults to the server's
                current time.
            limit (:obj:`int`, optional): Default 500.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Margin-change events
            ``[{'symbol': ..., 'type': 1/2, 'deltaType': ..., 'amount': ..., 'asset': ..., 'time': ..., 'positionSide': ...}, ...]``.
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

    # ----- REST: account configuration / status / fee -----------------------

    def get_account_config(self, **kwargs) -> Awaitable:
        """Queries account-level configuration (fee tier, multi-assets margin, etc.).

        Weight: 5. Security: USER_DATA.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Config

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: Account configuration including ``feeTier``,
            ``canTrade``, ``canDeposit``, ``canWithdraw``,
            ``dualSidePosition``, ``multiAssetsMargin``, etc.
        """
        ...  # pragma: no cover

    def get_symbol_config(self, **kwargs) -> Awaitable:
        """Queries symbol-level configuration (margin type, leverage, etc.).

        Weight: 5. Security: USER_DATA.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Symbol-Config

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns config for every symbol with a non-default setting.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            list: Per-symbol configuration records.
        """
        ...  # pragma: no cover

    def get_api_trading_status(self, **kwargs) -> Awaitable:
        """Queries the API trading quantitative-rules indicators.

        Weight: 1 with ``symbol``, 10 without. Security: USER_DATA.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Futures-Trading-Quantitative-Rules-Indicators

        Used to detect server-side trading restrictions / "locked" flags
        on the API key (e.g. excess GTX cancellations triggering a
        cooldown). Critical for live operations — surface the result on a
        regular cadence so the trader knows before an order is rejected.

        Args:
            symbol (:obj:`str`, optional): The futures symbol. If omitted,
                returns the indicators for every symbol the key has used.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'indicators': {...}, 'updateTime': ...}`` describing
            current rule-violation counters and any active lock window.
        """
        ...  # pragma: no cover

    def get_fee_burn_status(self, **kwargs) -> Awaitable:
        """Queries whether BNB-burn fee discount is active on USDⓈ-M Futures.

        Weight: 30. Security: USER_DATA.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Get-BNB-Burn-Status

        Args:
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'feeBurn': True/False}``.
        """
        ...  # pragma: no cover

    def set_fee_burn(self, **kwargs) -> Awaitable:
        """Toggles BNB-burn fee discount on USDⓈ-M Futures.

        Weight: 1. Security: TRADE.
        Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Toggle-BNB-Burn-On-Futures-Trade

        Args:
            feeBurn (str): ``'true'`` to enable BNB-burn discount;
                ``'false'`` to disable.
            recvWindow (:obj:`long`, optional): Max 60000.

        Returns:
            dict: ``{'code': 200, 'msg': 'success'}`` on success.
        """
        ...  # pragma: no cover
