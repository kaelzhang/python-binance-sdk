"""Market-agnostic API credentials and request signing.

:class:`Credentials` is a first-class, independently-constructible object that
holds an API key plus optional signing material (an HMAC ``api_secret`` or an
Ed25519/RSA ``private_key``) and knows how to sign a query string. The same
instance may be shared across multiple market clients (Spot / Futures); a client
only holds a reference and never copies the key material.
"""

import base64
import hashlib
import hmac
import os
from typing import Optional, Union

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key


class Credentials:
    """Holds API credentials and signs request payloads.

    Args:
        api_key (:obj:`str`, optional): API key. Required for any endpoint that
            needs an API key (signed or API-key-only).
        api_secret (:obj:`str`, optional): API secret used for HMAC-SHA256
            signing (deprecated by Binance in favour of asymmetric keys).
        private_key (str or bytes, optional): Ed25519 or RSA private key for
            asymmetric request signing. Can be the PEM content (``str`` or
            ``bytes``) or a file path to a PEM file. When supplied, the private
            key is used for signing instead of ``api_secret``.
        private_key_pass (str or bytes, optional): Password to decrypt an
            encrypted PEM private key. Pass ``None`` (default) for unencrypted
            keys.
    """

    _api_key: Optional[str]
    _api_secret: Optional[str]
    _private_key: Optional[Union[Ed25519PrivateKey, RSAPrivateKey]]

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        private_key=None,
        private_key_pass=None,
    ) -> None:
        self._api_key = api_key or None
        self._api_secret = api_secret or None
        self._private_key = self._load_private_key(private_key, private_key_pass)

    @staticmethod
    def _load_private_key(
        private_key,
        private_key_pass,
    ) -> Optional[Union[Ed25519PrivateKey, RSAPrivateKey]]:
        """Load an Ed25519/RSA PEM private key (path or PEM content) for signing."""
        if private_key is None:
            return None
        if isinstance(private_key, (bytes, bytearray)):
            pem = bytes(private_key)
        elif os.path.isfile(private_key):
            with open(private_key, 'rb') as f:
                pem = f.read()
        else:
            pem = private_key.encode('utf-8')
        password = (
            private_key_pass.encode('utf-8')
            if isinstance(private_key_pass, str) else private_key_pass
        )
        key = load_pem_private_key(pem, password)
        if not isinstance(key, (Ed25519PrivateKey, RSAPrivateKey)):
            raise ValueError(
                'private_key must be an Ed25519 or RSA private key')
        return key

    @property
    def api_key(self) -> Optional[str]:
        """The configured API key, or ``None``."""
        return self._api_key

    def has_signing(self) -> bool:
        """Whether signing material (an HMAC secret or a private key) is present."""
        return self._api_secret is not None or self._private_key is not None

    def is_ed25519(self) -> bool:
        """Whether the loaded private key is Ed25519 (the only type that supports WS-API ``session.logon``)."""
        return isinstance(self._private_key, Ed25519PrivateKey)

    def sign(self, query_string: str) -> str:
        """Sign an already-assembled query string with the active credential.

        The single crypto primitive shared by both signing paths: it does NOT
        build or encode the payload, it only signs the exact UTF-8 string it is
        given. Uses the asymmetric private key when one is loaded
        (Ed25519/RSA -> base64), otherwise HMAC-SHA256 with ``api_secret`` ->
        lowercase hex. Callers are responsible for assembling the payload in
        the form Binance expects for the transport (percent-encoded for REST,
        raw values for the WS-API).
        """
        key = self._private_key
        if key is not None:
            return self._sign_asymmetric(key, query_string)
        # callers validate credentials before calling sign
        assert self._api_secret is not None
        m = hmac.new(
            self._api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256)
        return m.hexdigest()

    @staticmethod
    def _sign_asymmetric(
        key: Union[Ed25519PrivateKey, RSAPrivateKey],
        query_string: str
    ) -> str:
        message = query_string.encode('utf-8')
        if isinstance(key, Ed25519PrivateKey):
            signature = key.sign(message)
        else:  # RSA
            signature = key.sign(message, padding.PKCS1v15(), hashes.SHA256())
        return base64.b64encode(signature).decode('utf-8')
