"""Cryptographic tests for Ed25519 and RSA request signing.

Each test generates a real keypair, signs through the SDK, then verifies
the signature with the corresponding public key — proving the SDK produces
cryptographically valid output, not just plausible-looking bytes.
"""

import base64
import hmac
import hashlib

import pytest

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

from test.test_ws_api import WSAPIServer


# ---------------------------------------------------------------------------
# _ws_api_query — exact RAW sorted `key=value&...` contract (F-47)
# ---------------------------------------------------------------------------

def test_ws_api_query_is_sorted_raw_key_value_no_encoding():
    client = Client(api_key='k', api_secret='s')

    # A value with a space and a '+' — chars that percent-encoding WOULD
    # change. The WS-API payload MUST keep them raw ("no percent encoding").
    params = {
        'symbol': 'BTCUSDT',
        'newClientOrderId': 'a b+c',   # space -> %20, '+' -> %2B if encoded
        'apiKey': 'k',
        'timestamp': 1700000000000,
        # `signature` must be excluded from the payload entirely
        'signature': 'SHOULD_BE_DROPPED',
    }
    query = client._ws_api_query(params)

    # Exact sorted, raw, &-joined `key=value` (alphabetical by key).
    assert query == (
        'apiKey=k'
        '&newClientOrderId=a b+c'
        '&symbol=BTCUSDT'
        '&timestamp=1700000000000'
    )
    # Proof it is RAW, not percent-encoded:
    assert 'a b+c' in query
    assert '%20' not in query and '%2B' not in query
    # Proof `signature` is excluded:
    assert 'signature' not in query
    # And it diverges from the REST percent-encoded encoding for this payload.
    assert query != encode_params({k: v for k, v in params.items()
                                   if k != 'signature'})


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
# Signed WS-API request with private_key — no api_secret needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_signed_ws_api_request_with_private_key_no_api_secret():
    _priv, pem_str, _pub = _ed25519_pem_str()

    server = WSAPIServer(port=9089)
    # Ed25519 keys log on after connect; canned (empty) reply is enough.
    server.on('session.logon', result={'apiKey': 'k', 'authorizedSince': 1})
    server.on('account.status', result={
        'makerCommission': 10,
        'takerCommission': 10,
        'buyerCommission': 0,
        'sellerCommission': 0,
        'canTrade': True,
        'canWithdraw': True,
        'canDeposit': True,
        'balances': [],
    })
    await server.run()
    try:
        client = Client(
            ws_api_host=server.uri, api_key='k', private_key=pem_str)
        # Must not raise APISecretNotDefinedException (signs with the key).
        result = await client.get_account()
        assert result['canTrade'] is True
    finally:
        await client.close()
        await server.shutdown()


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
    # against the RAW WS-API query (the spec's signing input).
    payload = {k: v for k, v in signed.items() if k != 'signature'}
    message = client._ws_api_query(payload).encode('utf-8')
    pub.verify(sig_bytes, message)  # Ed25519: no exception == valid


def test_ws_api_signature_params_signs_raw_values_not_encoded():
    # F-35: the WS-API signature MUST be over the RAW value payload, so a value
    # with a percent-encoding-sensitive char verifies against `_ws_api_query`
    # and FAILS against the percent-encoded `encode_params`.
    _priv, pem_str, pub = _ed25519_pem_str()
    client = Client(api_key='k', private_key=pem_str)

    signed = client._ws_api_signature_params(newClientOrderId='a b+c')
    sig_bytes = base64.b64decode(signed['signature'])
    payload = {k: v for k, v in signed.items() if k != 'signature'}

    # Verifies against the RAW query.
    pub.verify(sig_bytes, client._ws_api_query(payload).encode('utf-8'))

    # And does NOT verify against the percent-encoded REST encoding.
    from cryptography.exceptions import InvalidSignature
    with pytest.raises(InvalidSignature):
        pub.verify(sig_bytes, encode_params(payload).encode('utf-8'))


def test_ws_api_signature_params_hmac_signs_raw_query():
    client = Client(api_key='k', api_secret='s')
    signed = client._ws_api_signature_params(newClientOrderId='a b+c')

    payload = {k: v for k, v in signed.items() if k != 'signature'}
    expected = hmac.new(
        b's',
        client._ws_api_query(payload).encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    assert signed['signature'] == expected


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
