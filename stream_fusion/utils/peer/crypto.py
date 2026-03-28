"""Cryptographic utilities for peer-to-peer cache authentication.

Security model:
  - Each peer relationship uses a shared 256-bit secret (stored in peer_keys table).
  - HMAC-SHA256 with a timestamp is used to authenticate every request.
    A ±60 second window prevents replay attacks.
  - Fernet (AES-128-CBC + HMAC-SHA256) is used to encrypt responses so that
    data (including file names and torrent metadata) is only readable by the
    holder of the corresponding secret.
  - The Fernet key is deterministically derived from the secret via SHA-256
    so no additional key material needs to be stored or exchanged.
"""

import base64
import hashlib
import hmac as _hmac
import json
import time
from typing import Any


def derive_fernet_key(secret: str) -> bytes:
    """Derive a Fernet-compatible 32-byte key from a shared secret.

    Uses SHA-256 with a fixed domain prefix so the same secret cannot be
    reused across different contexts without separation.
    """
    raw = hashlib.sha256(f"sf-peer-cache-v1:{secret}".encode()).digest()
    return base64.urlsafe_b64encode(raw)


def sign_request(secret: str, body_bytes: bytes) -> tuple[str, str]:
    """Sign a request body with HMAC-SHA256.

    The signature covers: timestamp + "." + SHA-256(body).
    This ties the signature to both the body content and the moment of creation.

    Returns:
        (timestamp_str, hmac_hex)
    """
    ts = str(int(time.time()))
    msg = ts.encode() + b"." + hashlib.sha256(body_bytes).digest()
    sig = _hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return ts, sig


def verify_request(
    secret: str,
    timestamp: str,
    signature: str,
    body_bytes: bytes,
    max_age: int = 60,
) -> bool:
    """Verify an HMAC-SHA256 request signature and timestamp freshness.

    Args:
        secret:    The shared peer secret.
        timestamp: Value of the X-Peer-Timestamp header.
        signature: Value of the X-Peer-Signature header (hex-encoded HMAC).
        body_bytes: Raw request body bytes.
        max_age:   Maximum age in seconds (default 60 — replay protection).

    Returns:
        True if the signature is valid and the timestamp is fresh.
        Returns False on *any* failure to prevent timing oracle attacks.
    """
    try:
        if abs(time.time() - int(timestamp)) > max_age:
            return False
        msg = timestamp.encode() + b"." + hashlib.sha256(body_bytes).digest()
        expected = _hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
        return _hmac.compare_digest(expected, signature)
    except Exception:
        return False


def encrypt_payload(secret: str, data: dict[str, Any]) -> str:
    """Fernet-encrypt a dict payload.

    Args:
        secret: The shared peer secret (used to derive the Fernet key).
        data:   Arbitrary dict to serialize and encrypt.

    Returns:
        URL-safe Fernet token string.
    """
    from cryptography.fernet import Fernet

    key = derive_fernet_key(secret)
    token = Fernet(key).encrypt(json.dumps(data, default=str).encode())
    return token.decode()


def decrypt_payload(secret: str, token: str) -> dict[str, Any]:
    """Decrypt a Fernet token produced by encrypt_payload.

    Args:
        secret: The shared peer secret.
        token:  Fernet token string.

    Returns:
        The decrypted dict.

    Raises:
        cryptography.fernet.InvalidToken: if the token is invalid or tampered.
        json.JSONDecodeError: if the decrypted payload is not valid JSON.
    """
    from cryptography.fernet import Fernet

    key = derive_fernet_key(secret)
    raw = Fernet(key).decrypt(token.encode())
    return json.loads(raw)
