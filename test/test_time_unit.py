"""Tests for the F-13 WS-API ``time_unit`` (microsecond) Client option.

Opting into microseconds appends ``?timeUnit=MICROSECOND`` to the WS-API
connection URL; the default (and an explicit millisecond) leaves it untouched.
"""

import pytest

from binance import Client
from binance.client import _apply_time_unit
from binance.core.common.constants import WS_API_HOST


# ---------------------------------------------------------------------------
# _apply_time_unit helper (pure unit)
# ---------------------------------------------------------------------------

def test_apply_time_unit_none_is_unchanged():
    assert _apply_time_unit(WS_API_HOST, None) == WS_API_HOST


def test_apply_time_unit_millisecond_is_unchanged():
    # Millisecond is the server default -> no query appended.
    assert _apply_time_unit(WS_API_HOST, 'millisecond') == WS_API_HOST
    assert _apply_time_unit(WS_API_HOST, 'MILLISECOND') == WS_API_HOST


def test_apply_time_unit_microsecond_appends_query():
    assert _apply_time_unit(WS_API_HOST, 'microsecond') == (
        WS_API_HOST + '?timeUnit=MICROSECOND')
    # Case-insensitive.
    assert _apply_time_unit(WS_API_HOST, 'MICROSECOND') == (
        WS_API_HOST + '?timeUnit=MICROSECOND')


def test_apply_time_unit_microsecond_uses_ampersand_when_query_present():
    host = WS_API_HOST + '?foo=bar'
    assert _apply_time_unit(host, 'microsecond') == (
        host + '&timeUnit=MICROSECOND')


def test_apply_time_unit_invalid_raises():
    with pytest.raises(ValueError, match='time_unit'):
        _apply_time_unit(WS_API_HOST, 'nanosecond')


# ---------------------------------------------------------------------------
# Client wiring
# ---------------------------------------------------------------------------

def test_client_default_time_unit_is_millisecond():
    client = Client()
    assert client._ws_api_host == WS_API_HOST


def test_client_microsecond_time_unit_appends_query():
    client = Client(time_unit='microsecond')
    assert client._ws_api_host == WS_API_HOST + '?timeUnit=MICROSECOND'


def test_client_microsecond_time_unit_with_custom_host():
    client = Client(
        ws_api_host='ws://localhost:1234/ws-api/v3', time_unit='MICROSECOND')
    assert client._ws_api_host == (
        'ws://localhost:1234/ws-api/v3?timeUnit=MICROSECOND')


def test_client_invalid_time_unit_raises():
    with pytest.raises(ValueError, match='time_unit'):
        Client(time_unit='seconds')
