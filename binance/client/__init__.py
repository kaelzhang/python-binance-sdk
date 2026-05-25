"""Backward-compatible ``Client`` shim.

The Spot client is now :class:`binance.spot.client.SpotClient`, constructed with
a :class:`~binance.core.auth.Credentials` object. ``Client`` remains a thin
subclass that accepts the legacy credential keyword arguments and wraps them in
a ``Credentials`` during the restructure; it is removed once all call sites move
to ``SpotClient`` + ``Credentials``.
"""

from binance.core.auth import Credentials
from binance.core.transport.ws_api import _apply_time_unit
from binance.spot.client import SpotClient

__all__ = ['Client', '_apply_time_unit']


class Client(SpotClient):
    """Legacy Spot client wrapper (use :class:`~binance.spot.client.SpotClient`).

    Accepts the legacy credential keyword arguments (``api_key`` /
    ``api_secret`` / ``private_key`` / ``private_key_pass``) and wraps them in a
    :class:`Credentials` instance forwarded to ``SpotClient``. All other keyword
    arguments are forwarded unchanged.
    """

    def __init__(
        self,
        api_key=None,
        api_secret=None,
        private_key=None,
        private_key_pass=None,
        **kwargs
    ):
        credentials = Credentials(
            api_key=api_key,
            api_secret=api_secret,
            private_key=private_key,
            private_key_pass=private_key_pass,
        )

        super().__init__(credentials, **kwargs)
