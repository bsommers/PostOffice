from dataclasses import dataclass, asdict, field, fields
from typing import Optional, Dict, Any, Union


@dataclass
class SecurityConfig:
    """
    Unified security configuration across messaging protocols.
    Supports TLS/mTLS encryption and authentication (username/password, SASL).
    """
    tls_enabled: bool = False
    ca_cert: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    client_key_password: Optional[str] = field(default=None, repr=False)
    insecure_skip_verify: bool = False
    username: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)
    sasl_mechanism: Optional[str] = None

    def __post_init__(self):
        if bool(self.client_cert) != bool(self.client_key):
            raise ValueError(
                "Both client_cert and client_key must be provided together for mutual TLS (mTLS)."
            )
        if self.sasl_mechanism is not None:
            self.sasl_mechanism = self.sasl_mechanism.upper()

    def to_dict(self) -> Dict[str, Any]:
        """Serializes configuration to a plain dictionary for persistence and network sync."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SecurityConfig":
        """Instantiates SecurityConfig from a dictionary, ignoring unknown extra keys."""
        valid_keys = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)


def ensure_security_config(cfg: Optional[Union[SecurityConfig, Dict[str, Any]]]) -> SecurityConfig:
    """
    Normalizes a security config parameter into a SecurityConfig instance.
    Returns a default disabled SecurityConfig if None is provided.
    """
    if cfg is None:
        return SecurityConfig()
    if isinstance(cfg, SecurityConfig):
        return cfg
    if isinstance(cfg, dict):
        return SecurityConfig.from_dict(cfg)
    raise TypeError(f"Expected SecurityConfig, dict, or None; got {type(cfg).__name__}")
