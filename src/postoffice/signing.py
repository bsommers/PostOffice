"""
Authentication of Control Plane traffic (events, stored state, version counter).

Signatures cover the *exact bytes* of the payload string as transmitted, never a re-serialised
form, so a verifier in any language needs only HMAC-SHA256 over the received bytes (no JSON
canonicalisation to get wrong). Each kind of message is domain-separated with a prefix, so a
signature minted for one purpose (a stored hash entry) is useless for another (a live event).
"""
import hashlib
import hmac
import json
import os
from typing import Any, Dict, Mapping, Optional, Tuple, Union

ENV_KEYS = "POSTOFFICE_EVENT_KEYS"
MIN_KEY_BYTES = 16
_ENVELOPE_KEYS = {"kid", "sig", "payload"}


class HmacSigner:
    """
    HMAC-SHA256 signer/verifier over a set of named keys.

    The first key signs; every key verifies, which makes rotation a three-step change: add the new
    key (first) to all verifiers, switch signers to it, then retire the old key.
    """

    def __init__(self, keys: Mapping[str, Union[str, bytes]]):
        if not keys:
            raise ValueError("at least one signing key is required")
        self._keys: Dict[str, bytes] = {}
        for kid, secret in keys.items():
            raw = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
            if not kid or ":" in kid or "," in kid:
                raise ValueError(f"invalid key id '{kid}'")
            if len(raw) < MIN_KEY_BYTES:
                raise ValueError(f"key '{kid}' is shorter than {MIN_KEY_BYTES} bytes")
            self._keys[kid] = raw
        self.signing_kid = next(iter(self._keys))

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> Optional["HmacSigner"]:
        """Builds a signer from ``POSTOFFICE_EVENT_KEYS="kid:secret,kid2:secret2"``; None when unset."""
        raw = (os.environ if env is None else env).get(ENV_KEYS, "").strip()
        if not raw:
            return None
        keys: Dict[str, str] = {}
        for item in raw.split(","):
            kid, sep, secret = item.strip().partition(":")
            if not sep:
                raise ValueError(f"{ENV_KEYS} entries must look like kid:secret")
            keys[kid.strip()] = secret.strip()
        return cls(keys)

    def sign(self, message: bytes) -> Tuple[str, str]:
        key = self._keys[self.signing_kid]
        return self.signing_kid, hmac.new(key, message, hashlib.sha256).hexdigest()

    def verify(self, kid: Any, sig: Any, message: bytes) -> bool:
        key = self._keys.get(kid) if isinstance(kid, str) else None
        if key is None:
            return False
        expected = hmac.new(key, message, hashlib.sha256).hexdigest()
        try:
            return hmac.compare_digest(expected, sig)
        except TypeError:  # non-string or non-ASCII signature
            return False


def _event_prefix() -> bytes:
    return b"event\x00"


def _state_prefix(hash_name: str, field: str) -> bytes:
    return b"state\x00" + hash_name.encode() + b"\x00" + field.encode() + b"\x00"


def _seal(signer: HmacSigner, prefix: bytes, payload: str) -> str:
    kid, sig = signer.sign(prefix + payload.encode("utf-8"))
    return json.dumps({"kid": kid, "sig": sig, "payload": payload})


def sign_event(signer: HmacSigner, payload: str) -> str:
    """Wraps a JSON event body in a signed envelope."""
    return _seal(signer, _event_prefix(), payload)


def sign_state(signer: HmacSigner, hash_name: str, field: str, payload: str) -> str:
    """Wraps a stored hash value; the signature is bound to its hash and field."""
    return _seal(signer, _state_prefix(hash_name, field), payload)


def sign_version(signer: HmacSigner, version: int) -> str:
    kid, sig = signer.sign(b"version\x00" + str(version).encode())
    return json.dumps({"kid": kid, "sig": sig})


def as_envelope(obj: Any) -> Optional[Dict[str, str]]:
    """Returns the object if it is a signed envelope, else None (legacy unsigned shape)."""
    if isinstance(obj, dict) and set(obj) == _ENVELOPE_KEYS and all(isinstance(v, str) for v in obj.values()):
        return obj
    return None


def verify_event(signer: HmacSigner, env: Mapping[str, str]) -> bool:
    return signer.verify(env["kid"], env["sig"], _event_prefix() + env["payload"].encode("utf-8"))


def verify_state(signer: HmacSigner, hash_name: str, field: str, env: Mapping[str, str]) -> bool:
    return signer.verify(env["kid"], env["sig"], _state_prefix(hash_name, field) + env["payload"].encode("utf-8"))


def verify_version(signer: HmacSigner, version: int, entry: str) -> bool:
    try:
        obj = json.loads(entry)
        return signer.verify(obj["kid"], obj["sig"], b"version\x00" + str(version).encode())
    except (ValueError, KeyError, TypeError):
        return False
