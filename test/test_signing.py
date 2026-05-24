"""Cryptographic tests for Ed25519 and RSA request signing.

Each test generates a real keypair, signs through the SDK, then verifies
the signature with the corresponding public key — proving the SDK produces
cryptographically valid output, not just plausible-looking bytes.
"""

import base64
import hmac
import hashlib
import re

import pytest
from aioresponses import aioresponses

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PrivateFormat,
    NoEncryption,
    BestAvailableEncryption,
)

from binance import Client
from binance.client.base import encode_params
from binance.common.exceptions import APISecretNotDefinedException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ed25519_pem_str() -> tuple:
    """Return (private_key_object, pem_str, public_key_object)."""
    key = Ed25519PrivateKey.generate()
    pem_bytes = key.private_bytes(
        Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
    )
    return key, pem_bytes.decode('utf-8'), key.public_key()


def _rsa_pem_str() -> tuple:
    """Return (private_key_object, pem_str, public_key_object)."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_bytes = key.private_bytes(
        Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
    )
    return key, pem_bytes.decode('utf-8'), key.public_key()


# ---------------------------------------------------------------------------
# Ed25519 signing
# ---------------------------------------------------------------------------

def test_ed25519_signature_verifies():
    _priv, pem_str, pub = _ed25519_pem_str()
    client = Client(api_key='k', private_key=pem_str)

    data = {'symbol': 'BTCUSDT', 'timestamp': 1}
    sig_b64 = client._generate_signature(data)

    # Must be valid base64
    sig_bytes = base64.b64decode(sig_b64)

    # Must NOT look like a hex HMAC (64 hex chars)
    assert len(sig_b64) != 64 or not all(c in '0123456789abcdef' for c in sig_b64)

    # Cryptographic verification — raises InvalidSignature on failure
    message = encode_params(data).encode('utf-8')
    pub.verify(sig_bytes, message)  # Ed25519: no exception == valid


# ---------------------------------------------------------------------------
# RSA signing
# ---------------------------------------------------------------------------

def test_rsa_signature_verifies():
    _priv, pem_str, pub = _rsa_pem_str()
    client = Client(api_key='k', private_key=pem_str)

    data = {'symbol': 'BTCUSDT', 'timestamp': 1}
    sig_b64 = client._generate_signature(data)

    sig_bytes = base64.b64decode(sig_b64)
    message = encode_params(data).encode('utf-8')

    # Cryptographic verification — raises InvalidSignature on failure
    pub.verify(sig_bytes, message, padding.PKCS1v15(), hashes.SHA256())


# ---------------------------------------------------------------------------
# HMAC fallback
# ---------------------------------------------------------------------------

def test_hmac_fallback_unchanged():
    client = Client(api_key='k', api_secret='s')
    data = {'a': 1}
    result = client._generate_signature(data)

    expected = hmac.new(
        b's',
        encode_params(data).encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    assert result == expected
    # Must be a 64-char hex string
    assert len(result) == 64
    assert all(c in '0123456789abcdef' for c in result)


# ---------------------------------------------------------------------------
# PEM as file path
# ---------------------------------------------------------------------------

def test_ed25519_from_file_path(tmp_path):
    key = Ed25519PrivateKey.generate()
    pem_bytes = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    pem_file = tmp_path / 'ed25519.pem'
    pem_file.write_bytes(pem_bytes)

    client = Client(api_key='k', private_key=str(pem_file))

    data = {'symbol': 'BTCUSDT', 'timestamp': 1}
    sig_b64 = client._generate_signature(data)
    sig_bytes = base64.b64decode(sig_b64)

    message = encode_params(data).encode('utf-8')
    key.public_key().verify(sig_bytes, message)


# ---------------------------------------------------------------------------
# Encrypted PEM
# ---------------------------------------------------------------------------

def test_ed25519_encrypted_pem():
    key = Ed25519PrivateKey.generate()
    pem_bytes = key.private_bytes(
        Encoding.PEM, PrivateFormat.PKCS8, BestAvailableEncryption(b'pw')
    )
    client = Client(
        api_key='k',
        private_key=pem_bytes.decode('utf-8'),
        private_key_pass='pw'
    )

    data = {'symbol': 'BTCUSDT', 'timestamp': 1}
    sig_b64 = client._generate_signature(data)
    sig_bytes = base64.b64decode(sig_b64)

    key.public_key().verify(sig_bytes, encode_params(data).encode('utf-8'))


# ---------------------------------------------------------------------------
# Signed REST request with private_key — no api_secret needed
# ---------------------------------------------------------------------------

_ACCOUNT_URL_RE = re.compile(r'https://api\.binance\.com/api/v3/account(\?.*)?$')


@pytest.mark.asyncio
async def test_signed_rest_request_with_private_key_no_api_secret():
    _priv, pem_str, _pub = _ed25519_pem_str()
    client = Client(api_key='k', private_key=pem_str)

    with aioresponses() as m:
        # Mock the lazy time-sync call
        m.get(
            'https://api.binance.com/api/v3/time',
            payload={'serverTime': 1_700_000_000_000},
        )
        # Mock the signed account endpoint — use regex so the dynamic
        # signature/timestamp query params don't need exact matching
        m.get(
            _ACCOUNT_URL_RE,
            payload={
                'makerCommission': 10,
                'takerCommission': 10,
                'buyerCommission': 0,
                'sellerCommission': 0,
                'canTrade': True,
                'canWithdraw': True,
                'canDeposit': True,
                'balances': [],
            },
        )
        # Must not raise APISecretNotDefinedException
        result = await client.get_account()

    assert result['canTrade'] is True


# ---------------------------------------------------------------------------
# _ws_api_signature_params with private_key
# ---------------------------------------------------------------------------

def test_ws_api_signature_params_with_private_key():
    _priv, pem_str, pub = _ed25519_pem_str()
    client = Client(api_key='k', private_key=pem_str)

    signed = client._ws_api_signature_params(symbol='BTCUSDT')

    assert signed['apiKey'] == 'k'
    assert signed['symbol'] == 'BTCUSDT'
    assert isinstance(signed['timestamp'], int)

    sig_b64 = signed['signature']
    sig_bytes = base64.b64decode(sig_b64)

    # Reconstruct the signed payload (without the signature itself) and verify
    payload = {k: v for k, v in signed.items() if k != 'signature'}
    message = encode_params(payload).encode('utf-8')
    pub.verify(sig_bytes, message)  # Ed25519: no exception == valid


# ---------------------------------------------------------------------------
# _load_private_key: bytes input path
# ---------------------------------------------------------------------------

def test_load_private_key_bytes_input():
    key = Ed25519PrivateKey.generate()
    pem_bytes = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())

    # Pass raw bytes
    client = Client(api_key='k', private_key=pem_bytes)

    data = {'x': 1}
    sig_b64 = client._generate_signature(data)
    sig_bytes = base64.b64decode(sig_b64)
    key.public_key().verify(sig_bytes, encode_params(data).encode('utf-8'))


# ---------------------------------------------------------------------------
# Credential guard: neither api_secret nor private_key → raise
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_signed_request_raises_without_any_credentials():
    client = Client(api_key='k')  # no api_secret, no private_key
    with pytest.raises(APISecretNotDefinedException):
        await client.get_account()


def test_unsupported_private_key_type_rejected():
    from cryptography.hazmat.primitives.asymmetric import ec
    ec_pem = ec.generate_private_key(ec.SECP256R1()).private_bytes(
        Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
    ).decode('utf-8')
    # Only Ed25519 and RSA are supported for Binance signing.
    with pytest.raises(ValueError, match='Ed25519 or RSA'):
        Client(api_key='k', private_key=ec_pem)
