"""Tests asserting the *removal* of the undocumented Spot
``!ticker@arr`` (``SubType.ALL_MARKET_TICKERS``) surface.

Source of truth: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams

The Spot WebSocket Streams docs document only two all-market ticker
variants:

* ``!miniTicker@arr`` -- the all-market mini-ticker array
  (``SubType.ALL_MARKET_MINI_TICKERS``);
* ``!ticker_<window-size>@arr`` -- the all-market rolling-window
  ticker array (``SubType.ALL_MARKET_WINDOW_TICKERS``).

The standalone ``!ticker@arr`` (full 24hr ticker for every symbol with
no rolling-window suffix) is NOT documented on the Spot streams page.
Per the project's "only trust developers.binance.com" rule the SDK
must not ship a Spot binding for an undocumented stream, so the spot
``AllMarketTickersHandlerBase`` / ``AllMarketTickersProcessor`` were
removed.

Futures, however, DOES document ``!ticker@arr`` (for both USDⓈ-M and
COIN-M); the futures handler/processor and the
``SubType.ALL_MARKET_TICKERS`` enum value remain.
"""

import pytest

from binance import SubType


# ---------------------------------------------------------------------------
# The SubType enum value remains -- it is shared with Futures, which DOES
# document !ticker@arr.  Only the Spot binding was removed.
# ---------------------------------------------------------------------------


def test_subtype_all_market_tickers_value_kept_for_futures():
    """``SubType.ALL_MARKET_TICKERS`` stays because Futures (UM + CM) bind to it.

    Futures docs:
    - https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
    - https://developers.binance.com/docs/derivatives/coin-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
    """
    assert str(SubType.ALL_MARKET_TICKERS) == 'allMarketTickers'


def test_subtype_documented_spot_variants_remain():
    """``ALL_MARKET_MINI_TICKERS`` and ``ALL_MARKET_WINDOW_TICKERS`` are
    explicitly documented for Spot and MUST remain."""
    assert str(SubType.ALL_MARKET_MINI_TICKERS) == 'allMarketMiniTickers'
    assert str(SubType.ALL_MARKET_WINDOW_TICKERS) == 'allMarketWindowTickers'


# ---------------------------------------------------------------------------
# The Spot binding surfaces MUST be gone.  Each import is expected to raise
# ImportError to assert callers cannot accidentally reach the
# undocumented stream.
# ---------------------------------------------------------------------------


def test_spot_handlers_all_market_tickers_handler_base_removed():
    """``binance.spot.handlers.AllMarketTickersHandlerBase`` MUST be gone.

    The standalone ``!ticker@arr`` stream is not documented on the Spot
    WebSocket Streams page.
    """
    with pytest.raises(ImportError):
        from binance.spot.handlers import (  # noqa: F401
            AllMarketTickersHandlerBase,
        )


def test_spot_processors_all_market_tickers_processor_removed():
    """``binance.spot.processors.AllMarketTickersProcessor`` MUST be gone."""
    with pytest.raises(ImportError):
        from binance.spot.processors import (  # noqa: F401
            AllMarketTickersProcessor,
        )


def test_top_level_all_market_tickers_handler_base_removed():
    """The top-level re-export ``binance.AllMarketTickersHandlerBase``
    (the Spot one) MUST be gone."""
    with pytest.raises(ImportError):
        from binance import AllMarketTickersHandlerBase  # noqa: F401


# ---------------------------------------------------------------------------
# The documented Spot all-market variants still wire up.
# ---------------------------------------------------------------------------


def test_documented_spot_all_market_handlers_still_present():
    """``AllMarketMiniTickersHandlerBase`` and
    ``AllMarketWindowTickersHandlerBase`` are documented and stay."""
    from binance.spot.handlers import (
        AllMarketMiniTickersHandlerBase,
        AllMarketWindowTickersHandlerBase,
    )
    # Sanity: both are class objects.
    assert isinstance(AllMarketMiniTickersHandlerBase, type)
    assert isinstance(AllMarketWindowTickersHandlerBase, type)


def test_documented_spot_all_market_processors_still_present():
    """The processors for the documented variants still register."""
    from binance.spot.processors import (
        AllMarketMiniTickersProcessor,
        AllMarketWindowTickersProcessor,
    )
    assert isinstance(AllMarketMiniTickersProcessor, type)
    assert isinstance(AllMarketWindowTickersProcessor, type)


def test_spot_streams_registry_does_not_register_all_market_tickers():
    """The PROCESSORS list MUST no longer include AllMarketTickersProcessor."""
    from binance.spot.streams import PROCESSORS

    for proc_cls in PROCESSORS:
        # Class name guard: ensure no lingering Spot AllMarketTickersProcessor.
        assert proc_cls.__name__ != 'AllMarketTickersProcessor'

    # No PROCESSOR.SUB_TYPE may equal ALL_MARKET_TICKERS on the Spot side --
    # that SubType is reserved for futures-side processors.
    for proc_cls in PROCESSORS:
        sub_type = getattr(proc_cls, 'SUB_TYPE', None)
        assert sub_type != SubType.ALL_MARKET_TICKERS, (
            f'Spot PROCESSORS still wires {proc_cls.__name__} to '
            f'SubType.ALL_MARKET_TICKERS -- the !ticker@arr stream is not '
            f'documented on the Spot WebSocket Streams page.'
        )


# ---------------------------------------------------------------------------
# Futures !ticker@arr handlers remain (documented).
# ---------------------------------------------------------------------------


def test_futures_all_market_tickers_handler_base_still_present():
    """Futures (UM + CM) AllMarketTickersHandlerBase IS documented and MUST stay.

    Imported under the ``Futures...`` prefix at the package top level.
    """
    from binance import FuturesAllMarketTickersHandlerBase
    assert isinstance(FuturesAllMarketTickersHandlerBase, type)
