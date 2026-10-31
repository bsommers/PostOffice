import json
import pathlib
import re
import subprocess
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PROTO_PATH = REPO_ROOT / "api" / "postoffice" / "v1" / "postoffice.proto"
OPENAPI_PATH = REPO_ROOT / "api" / "openapi.json"
HEADER_PATH = REPO_ROOT / "include" / "postoffice.h"


class TestPhase1IdlContracts(unittest.TestCase):
    def test_proto_file_exists_and_declares_v1_package(self):
        self.assertTrue(PROTO_PATH.is_file(), f"Missing {PROTO_PATH}")
        text = PROTO_PATH.read_text(encoding="utf-8")
        self.assertIn('syntax = "proto3";', text)
        self.assertIn("package postoffice.v1;", text)
        self.assertIn("additive-only", text.lower())

    def test_proto_services_and_rpcs_complete(self):
        text = PROTO_PATH.read_text(encoding="utf-8")
        expected_services = {
            "AdminService": {
                "RegisterBroker",
                "RemoveBroker",
                "UpdateBrokerSecurity",
                "AddRoute",
                "RemoveRoute",
                "AddSubscription",
                "SetIngress",
                "SetDlq",
                "SetUnroutablePolicy",
                "RegisterSchema",
                "GetSchema",
                "ClearState",
            },
            "ObserveService": {
                "GetHealth",
                "GetReady",
                "GetSenderReport",
                "GetStats",
                "GetMetrics",
            },
            "StreamService": {
                "Publish",
                "Subscribe",
            },
        }
        for service_name, expected_rpcs in expected_services.items():
            svc_match = re.search(
                rf"service\s+{service_name}\s*\{{(.*?)\}}", text, flags=re.DOTALL
            )
            self.assertIsNotNone(svc_match, f"Missing service {service_name}")
            svc_body = svc_match.group(1)
            found_rpcs = set(re.findall(r"rpc\s+(\w+)\s*\(", svc_body))
            self.assertEqual(
                found_rpcs,
                expected_rpcs,
                f"RPC mismatch in {service_name}: {found_rpcs ^ expected_rpcs}",
            )

    def test_proto_messages_have_unique_positive_field_numbers(self):
        text = PROTO_PATH.read_text(encoding="utf-8")
        messages = re.findall(r"message\s+(\w+)\s*\{(.*?)\}", text, flags=re.DOTALL)
        self.assertGreaterEqual(len(messages), 20)
        for msg_name, body in messages:
            numbers = [
                int(n)
                for n in re.findall(r"=\s*(\d+)\s*;", body)
            ]
            self.assertEqual(
                len(numbers),
                len(set(numbers)),
                f"Duplicate field number in message {msg_name}: {numbers}",
            )
            for num in numbers:
                self.assertGreaterEqual(num, 1, f"Invalid field number in {msg_name}")
                self.assertNotIn(
                    num,
                    range(19000, 20000),
                    f"Reserved protobuf field number used in {msg_name}: {num}",
                )

    def test_openapi_spec_valid_and_mirrors_services(self):
        self.assertTrue(OPENAPI_PATH.is_file(), f"Missing {OPENAPI_PATH}")
        spec = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
        self.assertEqual(spec.get("openapi"), "3.1.0")
        self.assertEqual(spec.get("info", {}).get("version"), "1.0.0")

        sec_schemes = spec.get("components", {}).get("securitySchemes", {})
        self.assertIn("bearerAuth", sec_schemes)
        self.assertIn("mtlsAuth", sec_schemes)

        expected_paths = {
            "/v1/admin/brokers",
            "/v1/admin/brokers/{name}",
            "/v1/admin/brokers/{name}/security",
            "/v1/admin/routes",
            "/v1/admin/subscriptions",
            "/v1/admin/ingress",
            "/v1/admin/dlq",
            "/v1/admin/unroutable-policy",
            "/v1/admin/schemas/{schema_id}",
            "/v1/admin/state",
            "/v1/observe/healthz",
            "/v1/observe/readyz",
            "/v1/observe/senders",
            "/v1/observe/stats",
            "/v1/observe/metrics",
            "/v1/stream/publish",
            "/v1/stream/subscribe",
        }
        paths = set(spec.get("paths", {}).keys())
        self.assertTrue(
            expected_paths.issubset(paths),
            f"Missing OpenAPI paths: {expected_paths - paths}",
        )

        schemas = spec.get("components", {}).get("schemas", {})
        for required_schema in (
            "ErrorResponse",
            "BrokerConfig",
            "RouteSpec",
            "SubscriptionSpec",
            "IngressRules",
            "DlqConfig",
            "UnroutablePolicySpec",
            "SchemaRegistration",
            "HealthResponse",
            "ReadyResponse",
            "SenderReportResponse",
            "StatsResponse",
            "PublishRequest",
            "PublishResponse",
            "StreamFrame",
            "StreamAck",
        ):
            self.assertIn(required_schema, schemas, f"Missing schema {required_schema}")

    def test_c_abi_header_compiles_under_c99_and_cpp17(self):
        self.assertTrue(HEADER_PATH.is_file(), f"Missing {HEADER_PATH}")
        header_text = HEADER_PATH.read_text(encoding="utf-8")
        for symbol in (
            "PO_ABI_VERSION",
            "po_status_t",
            "po_engine_t",
            "po_firewall_t",
            "po_trie_t",
            "po_governor_t",
            "po_codec_t",
            "po_client_t",
            "po_owned_buf_t",
            "po_buf_free",
            "po_abi_version",
            "po_trie_new",
            "po_trie_free",
            "po_trie_add_route",
            "po_trie_match",
            "po_firewall_new",
            "po_firewall_free",
            "po_firewall_check",
            "po_governor_new",
            "po_governor_free",
            "po_governor_try_acquire",
            "po_governor_release",
            "po_codec_encode_confluent",
            "po_codec_decode_confluent",
            "po_dlq_fingerprint",
        ):
            self.assertIn(symbol, header_text, f"Missing C ABI symbol: {symbol}")

        harness_code = """
#include "postoffice.h"
#include <stddef.h>

int verify_abi_signatures(void) {
    if (PO_ABI_VERSION != 1) return 1;
    po_slice_t slice = { (const uint8_t*)"sensors/temp", 12 };
    po_owned_buf_t out = { NULL, 0, 0 };
    (void)slice;
    (void)out;
    return (int)PO_OK;
}
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            c_file = pathlib.Path(tmpdir) / "test_abi.c"
            cpp_file = pathlib.Path(tmpdir) / "test_abi.cpp"
            c_file.write_text(harness_code, encoding="utf-8")
            cpp_file.write_text(harness_code, encoding="utf-8")

            c_proc = subprocess.run(
                [
                    "gcc",
                    "-std=c99",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-pedantic",
                    "-I",
                    str(HEADER_PATH.parent),
                    "-fsyntax-only",
                    str(c_file),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                c_proc.returncode,
                0,
                f"gcc C99 syntax check failed:\n{c_proc.stderr}",
            )

            cpp_proc = subprocess.run(
                [
                    "g++",
                    "-std=c++17",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-pedantic",
                    "-I",
                    str(HEADER_PATH.parent),
                    "-fsyntax-only",
                    str(cpp_file),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                cpp_proc.returncode,
                0,
                f"g++ C++17 syntax check failed:\n{cpp_proc.stderr}",
            )


if __name__ == "__main__":
    unittest.main()
