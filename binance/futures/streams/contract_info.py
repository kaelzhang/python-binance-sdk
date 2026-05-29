"""Contract info stream handler and processor.

Hosts ``ContractInfoHandlerBase`` (``!contractInfo`` stream) and its processor.
See :mod:`binance.futures.streams._common` for the per-stream verified findings.
"""

from typing import ClassVar, List, Optional

from binance.core.common.constants import (
    SubType,
    STREAM_TYPE_MAP,
    KEY_STREAM_TYPE,
    KEY_PAYLOAD,
)
from binance.core.common.exceptions import InvalidSubTypeParamException
from binance.core.common.types import DictPayload
from binance.core.handlers.base import Handler
from binance.core.processors.base import Processor


# ---------------------------------------------------------------------------
# Futures ContractInfo (shared UM + CM)
# Wire stream: !contractInfo
# Event type: 'contractInfo'
# Confirmed fields per developers.binance.com (UM + CM, 2026-05):
#   e  'contractInfo'
#   E  event time
#   s  symbol
#   ps pair
#   ct contract type
#   dt delivery time (ms; 0 for perpetual)
#   ot onboard time
#   cs contract status
#   bks list of brackets (leverage/notional brackets); each element has
#       {bs, bnf, bnc, mmr, cf, mi, ma}.  Exposed as ``brackets`` pass-through
#       so downstream consumers can introspect.
# Docs:
# - UM https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream
# - CM https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Contract-Info-Stream
# ---------------------------------------------------------------------------

CONTRACT_INFO_COLUMNS_MAP = {
    **STREAM_TYPE_MAP,
    'E': 'event_time',
    's': 'symbol',
    'ps': 'pair',
    'ct': 'contract_type',
    'dt': 'delivery_time',
    'ot': 'onboard_time',
    'cs': 'contract_status',
    'bks': 'brackets',
}

CONTRACT_INFO_COLUMNS = CONTRACT_INFO_COLUMNS_MAP.keys()


class ContractInfoHandlerBase(Handler):
    """Base handler for the futures ``SubType.CONTRACT_INFO`` stream (``!contractInfo``).

    Shared across USDⓈ-M and COIN-M markets.  Receives contract specification
    change events such as listing, settlement, or bracket updates.  Each payload
    carries the symbol, pair, contract type, delivery time, onboard time,
    contract status, and a ``brackets`` list (raw doc field ``bks``; each
    element has ``bs, bnf, bnc, mmr, cf, mi, ma``).  The brackets list is
    surfaced as a pass-through cell so downstream consumers can introspect.

    Subclass this and override ``receive(payload)`` to handle events.

    Docs:
    - UM: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream
    - CM: https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/Contract-Info-Stream
    """

    COLUMNS_MAP = CONTRACT_INFO_COLUMNS_MAP
    COLUMNS = CONTRACT_INFO_COLUMNS

    def _receive(  # type: ignore[override]
        self, payload: DictPayload, index: Optional[List[int]] = None
    ):
        # ``bks`` is a list-valued field; pandas treats lists as a column
        # value so a raw assignment expands across rows.  Wrap it in a
        # single-element outer list so it lands as a single cell while the
        # rest of the scalar fields flow through unchanged.
        if 'bks' in payload:
            payload = {**payload, 'bks': [payload['bks']]}
        return super()._receive(payload, index)


class ContractInfoProcessor(Processor):
    """Processor for the futures contract info stream (``!contractInfo``).

    Shared by both USDⓈ-M and COIN-M markets.  No symbol parameter required.
    """

    HANDLER = ContractInfoHandlerBase
    SUB_TYPE = SubType.CONTRACT_INFO
    PAYLOAD_TYPE = 'contractInfo'
    STREAM_TYPE_NAME: ClassVar[str] = '!contractInfo'

    def is_message_type(self, msg):
        stream_type = msg.get(KEY_STREAM_TYPE)

        if stream_type == self.STREAM_TYPE_NAME:
            return True, msg.get(KEY_PAYLOAD)

        return False, None

    def subscribe_param(self, _, t, *args) -> str:
        if len(args) != 0:
            raise InvalidSubTypeParamException(
                t, 'symbol',
                '`SubType.CONTRACT_INFO` expects no parameters'
            )
        return self.STREAM_TYPE_NAME
