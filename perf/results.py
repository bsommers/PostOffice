"""Results document (schema ``postoffice.perf.result/v1``) with the environment needed to reproduce a run."""
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

SCHEMA = "postoffice.perf.result/v1"
VERDICTS = ("pass", "fail", "info")
_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(path: str) -> Optional[str]:
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


def _git(*args: str) -> Optional[str]:
    try:
        return subprocess.run(["git", *args], cwd=_REPO, capture_output=True, text=True, timeout=5,
                              check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def _cpu_model() -> Optional[str]:
    info = _read("/proc/cpuinfo") or ""
    for line in info.splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or None


def environment() -> Dict[str, Any]:
    return {
        "host": platform.node(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "cpu_governor": _read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        "cgroup_memory_max": _read("/sys/fs/cgroup/memory.max"),
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
    }


def make_result(scenario: str, metrics: Dict[str, Any], verdict: str = "info") -> Dict[str, Any]:
    if verdict not in VERDICTS:
        raise ValueError(f"verdict must be one of {VERDICTS}")
    return {
        "schema": SCHEMA,
        "scenario": scenario,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git": {"sha": _git("rev-parse", "HEAD"), "dirty": bool(_git("status", "--porcelain"))},
        "environment": environment(),
        "metrics": metrics,
        "verdict": verdict,
    }


def write_result(doc: Dict[str, Any], directory: str) -> str:
    os.makedirs(directory, exist_ok=True)
    stamp = doc["created_at"].replace(":", "").replace("-", "").split(".")[0]
    path = os.path.join(directory, f"{doc['scenario']}-{stamp}.json")
    with open(path, "w") as f:
        json.dump(doc, f, indent=2, sort_keys=True)
    return path
