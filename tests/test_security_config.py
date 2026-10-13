import unittest
from postoffice.security import SecurityConfig, ensure_security_config


class TestSecurityConfig(unittest.TestCase):
    def test_default_values(self):
        cfg = SecurityConfig()
        self.assertFalse(cfg.tls_enabled)
        self.assertIsNone(cfg.ca_cert)
        self.assertIsNone(cfg.client_cert)
        self.assertIsNone(cfg.client_key)
        self.assertIsNone(cfg.client_key_password)
        self.assertFalse(cfg.insecure_skip_verify)
        self.assertIsNone(cfg.username)
        self.assertIsNone(cfg.password)
        self.assertIsNone(cfg.sasl_mechanism)

    def test_mtls_validation(self):
        # Client cert without client key must raise ValueError
        with self.assertRaises(ValueError):
            SecurityConfig(client_cert="/path/to/cert.pem")

        # Client key without client cert must raise ValueError
        with self.assertRaises(ValueError):
            SecurityConfig(client_key="/path/to/key.pem")

        # Both cert and key should succeed
        cfg = SecurityConfig(
            tls_enabled=True,
            ca_cert="/path/to/ca.pem",
            client_cert="/path/to/cert.pem",
            client_key="/path/to/key.pem",
        )
        self.assertTrue(cfg.tls_enabled)
        self.assertEqual(cfg.client_cert, "/path/to/cert.pem")
        self.assertEqual(cfg.client_key, "/path/to/key.pem")

    def test_to_dict_and_from_dict(self):
        cfg = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            client_cert="/certs/client.pem",
            client_key="/certs/client.key",
            client_key_password="secret_passphrase",
            insecure_skip_verify=True,
            username="app_user",
            password="app_password",
            sasl_mechanism="SCRAM-SHA-512",
        )
        data = cfg.to_dict()
        self.assertEqual(data["tls_enabled"], True)
        self.assertEqual(data["ca_cert"], "/certs/ca.pem")
        self.assertEqual(data["client_cert"], "/certs/client.pem")
        self.assertEqual(data["client_key"], "/certs/client.key")
        self.assertEqual(data["client_key_password"], "secret_passphrase")
        self.assertEqual(data["insecure_skip_verify"], True)
        self.assertEqual(data["username"], "app_user")
        self.assertEqual(data["password"], "app_password")
        self.assertEqual(data["sasl_mechanism"], "SCRAM-SHA-512")

        restored = SecurityConfig.from_dict(data)
        self.assertEqual(cfg, restored)

    def test_ensure_security_config(self):
        # None returns default
        cfg = ensure_security_config(None)
        self.assertIsInstance(cfg, SecurityConfig)
        self.assertFalse(cfg.tls_enabled)

        # Dict converts to SecurityConfig
        dict_data = {"tls_enabled": True, "username": "admin", "password": "password123"}
        cfg2 = ensure_security_config(dict_data)
        self.assertIsInstance(cfg2, SecurityConfig)
        self.assertTrue(cfg2.tls_enabled)
        self.assertEqual(cfg2.username, "admin")

        # Existing SecurityConfig instance returns itself
        cfg3 = ensure_security_config(cfg2)
        self.assertIs(cfg3, cfg2)


if __name__ == "__main__":
    unittest.main()
