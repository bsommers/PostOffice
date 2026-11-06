"""
Hermetic verification suite for PostOffice Polyglot Client SDKs:
- C SDK (gcc against include/postoffice.h and libpostoffice.so)
- Go SDK (go test stdlib)
- Rust SDK (cargo test postoffice-sdk)
- JS/TS SDK (node ESM)
"""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


class TestPolyglotSDKs(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.gcc = shutil.which("gcc")
        if not cls.gcc:
            raise unittest.SkipTest("gcc compiler not available in environment")

        go_candidate = pathlib.Path("/usr/local/go/bin/go")
        cls.go = str(go_candidate) if go_candidate.is_file() else shutil.which("go")
        if not cls.go:
            raise unittest.SkipTest("go toolchain not available in environment")

        node_candidate = pathlib.Path("/home/bill/.nvm/versions/node/v26.8.1/bin/node")
        cls.node = str(node_candidate) if node_candidate.is_file() else shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node runtime not available in environment")

        cls.cargo = shutil.which("cargo")
        if not cls.cargo:
            raise unittest.SkipTest("cargo toolchain not available in environment")

        cls.so_dir = REPO_ROOT / "native" / "libpostoffice" / "target" / "release"
        so_file = cls.so_dir / "libpostoffice.so"
        if not so_file.is_file():
            # Build release cdylib if missing
            proc = subprocess.run(
                [cls.cargo, "build", "--release", "--manifest-path", str(REPO_ROOT / "native" / "libpostoffice" / "Cargo.toml")],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise AssertionError(f"Failed building libpostoffice.so:\n{proc.stderr}")

    def test_c_sdk_compilation_and_execution(self) -> None:
        """Compile and execute C client SDK exercising postoffice.h entry points."""
        c_src = REPO_ROOT / "sdks" / "c" / "postoffice_sdk.c"
        include_dir = REPO_ROOT / "include"

        with tempfile.TemporaryDirectory() as tmp_dir:
            exe_path = pathlib.Path(tmp_dir) / "postoffice_c_sdk"
            compile_cmd = [
                self.gcc,
                "-Wall",
                "-Wextra",
                "-O2",
                f"-I{include_dir}",
                str(c_src),
                f"-L{self.so_dir}",
                "-lpostoffice",
                f"-Wl,-rpath,{self.so_dir}",
                "-o",
                str(exe_path),
            ]
            compile_res = subprocess.run(compile_cmd, capture_output=True, text=True, check=False)
            self.assertEqual(
                compile_res.returncode,
                0,
                f"C SDK compilation failed:\nstdout: {compile_res.stdout}\nstderr: {compile_res.stderr}",
            )

            run_res = subprocess.run([str(exe_path)], capture_output=True, text=True, check=False)
            self.assertEqual(
                run_res.returncode,
                0,
                f"C SDK run failed:\nstdout: {run_res.stdout}\nstderr: {run_res.stderr}",
            )
            self.assertIn("C_SDK_OK", run_res.stdout)

    def test_go_sdk_conformance(self) -> None:
        """Run Go client SDK tests against conformance vectors."""
        go_dir = REPO_ROOT / "sdks" / "go"
        res = subprocess.run(
            [self.go, "test", "-v", "./..."],
            cwd=str(go_dir),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            res.returncode,
            0,
            f"Go SDK tests failed:\nstdout: {res.stdout}\nstderr: {res.stderr}",
        )
        self.assertIn("PASS", res.stdout)
        self.assertIn("TestWireFramingConformance", res.stdout)
        self.assertIn("TestTopicMatchingConformance", res.stdout)
        self.assertIn("TestControlPlaneSigningConformance", res.stdout)
        self.assertIn("TestAdminAPIConformance", res.stdout)

    def test_rust_sdk_conformance(self) -> None:
        """Run Rust client SDK tests against conformance vectors."""
        manifest = REPO_ROOT / "sdks" / "rust" / "Cargo.toml"
        env = dict(os.environ)
        env["CARGO_TARGET_DIR"] = "/tmp/po-target-rust-sdk"

        res = subprocess.run(
            [self.cargo, "test", "--manifest-path", str(manifest)],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            res.returncode,
            0,
            f"Rust SDK tests failed:\nstdout: {res.stdout}\nstderr: {res.stderr}",
        )
        self.assertIn("test result: ok", res.stdout)
        self.assertIn("test tests::test_wire_framing_vector ... ok", res.stdout)
        self.assertIn("test tests::test_topic_matching_vector ... ok", res.stdout)
        self.assertIn("test tests::test_control_plane_signing_vector ... ok", res.stdout)
        self.assertIn("test tests::test_router_delivery_vector ... ok", res.stdout)

    def test_ts_sdk_conformance(self) -> None:
        """Run Node.js/TS client SDK tests against conformance vectors."""
        ts_dir = REPO_ROOT / "sdks" / "ts"
        res = subprocess.run(
            [self.node, "test.mjs"],
            cwd=str(ts_dir),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            res.returncode,
            0,
            f"TS SDK tests failed:\nstdout: {res.stdout}\nstderr: {res.stderr}",
        )
        self.assertIn("TS_SDK_OK", res.stdout)


if __name__ == "__main__":
    unittest.main()
