"""Shared USDⓈ-M and COIN-M Futures user-data-stream handler bases.

All handlers here are pass-through (raw dict payloads passed unchanged to
``receive``), mirroring the Spot user-handler pattern in
``binance.spot.user_handlers``.

Event types confirmed from official Binance USDⓈ-M Futures docs (2026-05-25):
- ``ACCOUNT_UPDATE``:    balance + position snapshot on account change
  Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Balance-and-Position-Update
- ``ORDER_TRADE_UPDATE``: order lifecycle event (new / partial / filled / cancelled)
  Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Order-Update
- ``MARGIN_CALL``:       pushed when position risk ratio exceeds maintenance margin
  Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Margin-Call
- ``ACCOUNT_CONFIG_UPDATE``: leverage or multi-assets-mode change
  (task spec; payload: ac{s,l} for leverage, ai{j} for multi-assets mode)
- ``listenKeyExpired``:  connection key expired; payload confirmed from docs
  Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-User-Data-Stream-Expired
"""

from binance.core.common.types import DictPayload
from binance.core.handlers.base import Handler


class FuturesSimpleHandler(Handler):
    """Pass-through base for futures user-data-stream handlers.

    Delivers the raw Binance API dict unchanged to ``receive``.
    Subclass and override ``receive(payload)`` to process the event.
    """

    def receive(self, msg: DictPayload):
        """Return the raw futures user-stream payload without transformation.

        Args:
            msg (dict): The raw user-data-stream event payload received from
                Binance.

        Returns:
            dict: The same ``msg`` dict, unmodified.
        """
        return msg


class FuturesAccountUpdateHandlerBase(FuturesSimpleHandler):
    """Base handler for the ``ACCOUNT_UPDATE`` futures user-data-stream event.

    Receives a balance and position snapshot whenever an order is placed,
    filled, cancelled, or when a fund transfer occurs.

    Payload structure (confirmed from Binance USDⓈ-M docs, 2026-05-25)::

        {
            "e": "ACCOUNT_UPDATE",
            "E": <event_time>,
            "T": <transaction_time>,
            "a": {
                "m": "<event_reason>",   # e.g. "ORDER", "FUNDING_FEE"
                "B": [
                    {
                        "a": "<asset>",
                        "wb": "<wallet_balance>",
                        "cw": "<cross_wallet_balance>",
                        "bc": "<balance_change>"
                    },
                    ...
                ],
                "P": [
                    {
                        "s":  "<symbol>",
                        "pa": "<position_amount>",
                        "ep": "<entry_price>",
                        "bep": "<breakeven_price>",
                        "cr": "<realized_pnl>",
                        "up": "<unrealized_pnl>",
                        "mt": "<margin_type>",
                        "iw": "<isolated_wallet>",
                        "ps": "<position_side>"
                    },
                    ...
                ]
            }
        }

    Subclass and override ``receive(payload)`` to handle the event.
    The raw Binance payload dict is passed unchanged.
    """

    pass


class FuturesOrderUpdateHandlerBase(FuturesSimpleHandler):
    """Base handler for the ``ORDER_TRADE_UPDATE`` futures user-data-stream event.

    Receives an order-lifecycle update whenever a futures order is created,
    partially filled, fully filled, cancelled, or expired.

    Payload structure (confirmed from Binance USDⓈ-M docs, 2026-05-25)::

        {
            "e": "ORDER_TRADE_UPDATE",
            "E": <event_time>,
            "T": <transaction_time>,
            "o": {
                "s":   "<symbol>",
                "c":   "<client_order_id>",
                "S":   "BUY" | "SELL",
                "o":   "<order_type>",
                "f":   "<time_in_force>",
                "q":   "<original_qty>",
                "p":   "<original_price>",
                "ap":  "<avg_price>",
                "sp":  "<stop_price>",
                "x":   "<execution_type>",
                "X":   "<order_status>",
                "i":   <order_id>,
                "l":   "<last_filled_qty>",
                "z":   "<acc_filled_qty>",
                "L":   "<last_filled_price>",
                "N":   "<commission_asset>",
                "n":   "<commission>",
                "T":   <trade_time>,
                "t":   <trade_id>,
                "b":   "<bids_notional>",
                "a":   "<ask_notional>",
                "m":   <is_maker>,
                "R":   <reduce_only>,
                "wt":  "<stop_price_working_type>",
                "ot":  "<original_order_type>",
                "ps":  "<position_side>",
                "cp":  <close_all>,
                "AP":  "<activation_price>",
                "cr":  "<callback_rate>",
                "pP":  <price_protection>,
                "rp":  "<realized_profit>",
                "V":   "<stp_mode>",
                "pm":  "<price_match_mode>",
                "gtd": <gtd_cancel_time>,
                "er":  "<expiry_reason>"
            }
        }

    Subclass and override ``receive(payload)`` to handle the event.
    The raw Binance payload dict is passed unchanged.
    """

    pass


class FuturesMarginCallHandlerBase(FuturesSimpleHandler):
    """Base handler for the ``MARGIN_CALL`` futures user-data-stream event.

    Receives a notification when the user's position risk ratio exceeds the
    maintenance margin threshold.  This is risk guidance only; Binance does
    not guarantee it precedes every liquidation.

    Payload structure (confirmed from Binance USDⓈ-M docs, 2026-05-25)::

        {
            "e": "MARGIN_CALL",
            "E": <event_time>,
            "cw": "<cross_wallet_balance>",
            "p": [
                {
                    "s":  "<symbol>",
                    "ps": "<position_side>",
                    "pa": "<position_amount>",
                    "mt": "CROSSED" | "ISOLATED",
                    "iw": "<isolated_wallet_balance>",
                    "mp": "<mark_price>",
                    "up": "<unrealized_pnl>",
                    "mm": "<maintenance_margin_required>"
                },
                ...
            ]
        }

    Subclass and override ``receive(payload)`` to handle the event.
    The raw Binance payload dict is passed unchanged.
    """

    pass


class FuturesAccountConfigUpdateHandlerBase(FuturesSimpleHandler):
    """Base handler for the ``ACCOUNT_CONFIG_UPDATE`` futures user-data-stream event.

    Receives a notification when the account leverage or multi-assets margin
    mode changes.  Two payload shapes are used:

    **Leverage change** (``ac`` present)::

        {
            "e": "ACCOUNT_CONFIG_UPDATE",
            "E": <event_time>,
            "T": <transaction_time>,
            "ac": {
                "s": "<symbol>",
                "l": <leverage>
            }
        }

    **Multi-assets mode change** (``ai`` present)::

        {
            "e": "ACCOUNT_CONFIG_UPDATE",
            "E": <event_time>,
            "T": <transaction_time>,
            "ai": {
                "j": <is_multi_assets_margin>
            }
        }

    Subclass and override ``receive(payload)`` to handle the event.
    The raw Binance payload dict is passed unchanged.
    """

    pass


class FuturesListenKeyExpiredHandlerBase(FuturesSimpleHandler):
    """Base handler for the ``listenKeyExpired`` futures user-data-stream event.

    Receives a notification when the user-data-stream listen key expires.
    After this event the stream is disconnected and a new listen key must
    be obtained to re-establish the stream.

    Payload structure (confirmed from Binance USDⓈ-M docs, 2026-05-25)::

        {
            "e": "listenKeyExpired",
            "E": <event_time>,
            "listenKey": "<expired_listen_key>"
        }

    Subclass and override ``receive(payload)`` to handle the event.
    The raw Binance payload dict is passed unchanged.
    """

    pass


class FuturesEventStreamTerminatedHandlerBase(FuturesSimpleHandler):
    """Base handler for the ``eventStreamTerminated`` SDK-synthesized event.

    Receives a notification synthesized by the SDK (not by Binance itself)
    when the futures user-data WebSocket stream is terminated unexpectedly.
    After receiving this event the SDK automatically attempts to re-establish
    the stream.  Override ``receive(payload)`` to log or react to stream
    interruptions.

    The raw payload dict is passed unchanged.
    """

    pass


__all__ = [
    'FuturesAccountUpdateHandlerBase',
    'FuturesOrderUpdateHandlerBase',
    'FuturesMarginCallHandlerBase',
    'FuturesAccountConfigUpdateHandlerBase',
    'FuturesListenKeyExpiredHandlerBase',
    'FuturesEventStreamTerminatedHandlerBase',
]
