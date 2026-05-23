"""Pytest session setup.

The default test suite is fully hermetic -- it only talks to local mock
servers (``ws://localhost``) and mocked HTTP responses, never the real
Binance endpoints. If an outbound proxy is configured in the environment
(common when developing behind a GFW-bypass proxy), ``websockets >= 15``
routes even localhost connections through it and, lacking ``python-socks``,
fails every connect with ``ImportError: python-socks is required to use a
SOCKS proxy`` -- producing an endless reconnect storm that hangs the suite.

Since hermetic tests never need a proxy, strip the proxy variables for the
session. The opt-in live tests (``BINANCE_LIVE_TEST=1``) DO need the proxy to
reach Binance, so leave the environment untouched in that case.
"""

import os

_PROXY_VARS = (
    'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'WS_PROXY', 'WSS_PROXY',
    'http_proxy', 'https_proxy', 'all_proxy', 'ws_proxy', 'wss_proxy',
)

if os.environ.get('BINANCE_LIVE_TEST') != '1':
    for _var in _PROXY_VARS:
        os.environ.pop(_var, None)
    os.environ['NO_PROXY'] = '*'
    os.environ['no_proxy'] = '*'
