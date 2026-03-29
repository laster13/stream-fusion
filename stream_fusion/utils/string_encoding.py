import base64
import hashlib
import hmac
import time as _time


def encodeb64(data):
    return base64.b64encode(data.encode('utf-8')).decode('utf-8')


def decodeb64(data):
    return base64.b64decode(data).decode('utf-8')


# ─── Fernet config encryption ──────────────────────────────────────────────

def _derive_fernet_key(secret: str) -> bytes:
    """Derives a 32-byte Fernet key from an arbitrary string via PBKDF2."""
    dk = hashlib.pbkdf2_hmac(
        'sha256',
        secret.encode('utf-8'),
        b'streamfusion-config-salt',
        iterations=100_000,
        dklen=32,
    )
    return base64.urlsafe_b64encode(dk)


def _get_fernet():
    """Returns a Fernet instance if CONFIG_SECRET_KEY is set, else None."""
    from stream_fusion.settings import settings  # late import — avoids circular imports
    if not settings.config_secret_key:
        return None
    from cryptography.fernet import Fernet
    return Fernet(_derive_fernet_key(settings.config_secret_key))


def encrypt_config(data: str) -> str:
    """Encrypts a config JSON string with Fernet. Raises RuntimeError if key is not set."""
    f = _get_fernet()
    if f is None:
        raise RuntimeError("CONFIG_SECRET_KEY is not set — cannot encrypt config.")
    return f.encrypt(data.encode('utf-8')).decode('utf-8')


def decrypt_config(token: str) -> str:
    """
    Decrypts a config token.
    Tries Fernet first, then falls back to Base64 for backward compatibility.
    """
    f = _get_fernet()
    if f is not None:
        try:
            from cryptography.fernet import InvalidToken
            return f.decrypt(token.encode('utf-8')).decode('utf-8')
        except InvalidToken:
            pass  # Not a valid Fernet token — fall through to base64 fallback
    return decodeb64(token)


# ─── CSRF token (protects /api/config/encode and admin POST routes) ────────

def _get_csrf_secret() -> str:
    """Returns the CSRF secret: config_secret_key if available, otherwise session_key."""
    from stream_fusion.settings import settings
    return settings.config_secret_key or settings.session_key


def generate_csrf_token() -> str:
    """Generates an HMAC-SHA256 signed CSRF token valid for 1 hour."""
    ts = str(int(_time.time()))
    secret = _get_csrf_secret().encode('utf-8')
    sig = hmac.new(secret, ts.encode('utf-8'), 'sha256').hexdigest()
    return f"{ts}.{sig}"


def verify_csrf_token(token: str, ttl: int = 3600) -> bool:
    """Validates a CSRF token (HMAC signature + TTL check)."""
    try:
        ts_str, sig = token.split('.', 1)
        ts = int(ts_str)
        if _time.time() - ts > ttl:
            return False
        secret = _get_csrf_secret().encode('utf-8')
        expected = hmac.new(secret, ts_str.encode('utf-8'), 'sha256').hexdigest()
        return hmac.compare_digest(expected, sig)
    except Exception:
        return False
