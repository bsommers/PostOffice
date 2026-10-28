"""
Control Plane load test: how fast a Data Plane worker processes control events.

Hermetic (in-memory Redis stand-in, no network); run it by hand, it is not collected by unittest:

    PYTHONPATH=src .venv/bin/python tests/load_control_plane.py [events]

Each scenario publishes `events` distinct `set_ingress` events (so none is skipped as identical)
through a real ControlPlane, then times DataPlane._process_update_message over them. Numbers
measure the validation/signature/apply pipeline on this machine, not Redis or broker latency.
"""
import logging
import os
import sys
import time
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from fakes_redis import FakeBus  # noqa: E402

from postoffice.control_plane import ControlPlane  # noqa: E402
from postoffice.data_plane import DataPlane  # noqa: E402
from postoffice.signing import HmacSigner  # noqa: E402

KEY = "load-test-key-0123456789abcdef"
RESULTS = ("applied", "rate_limited", "oversize", "invalid", "bad_signature", "unsigned", "stale")


def run(label, events, signed=False, mode=None, unlimited=True, max_event_bytes=None, pad=0):
    bus = FakeBus()
    bus.delete = lambda *keys: None
    signer = HmacSigner({"load": KEY}) if signed else None
    with patch("postoffice.control_plane.redis.Redis", return_value=bus), \
            patch("postoffice.data_plane.redis.Redis", return_value=bus):
        cp = ControlPlane(signer=signer)
        kwargs = {"signer": signer, "signing_mode": mode}
        if unlimited:
            kwargs.update(event_rate=1e9, event_burst=1e9)
        if max_event_bytes:
            kwargs["max_event_bytes"] = max_event_bytes
        dp = DataPlane(**kwargs)
    dp._version = 0
    pubsub = dp.pubsub
    bus.pubsubs.append(pubsub)
    for i in range(events):
        config = {"rules": [{"type": "max_payload_bytes", "max_bytes": 1000 + i}]}
        if pad:
            config["ledger"] = {}
            config["rules"] += [{"type": "topic_shape", "max_depth": 8}] * (pad // 80)
        cp.set_ingress(config)
    messages = list(pubsub.queue)
    start = time.perf_counter()
    for raw in messages:
        dp._process_update_message({"type": "message", "data": raw})
    elapsed = time.perf_counter() - start
    registry = dp.po.router.metrics.registry
    counts = {r: int(registry.get_sample_value("postoffice_control_events_total", {"result": r}) or 0)
              for r in RESULTS}
    counts = {k: v for k, v in counts.items() if v}
    dp.po.stop()
    avg_kib = sum(len(m) for m in messages) / len(messages) / 1024
    print(f"{label:<34} {events / elapsed:>10,.0f} ev/s  {elapsed / events * 1e6:>8.1f} us/ev  "
          f"{avg_kib:>6.1f} KiB/ev  {counts}")


def main():
    logging.disable(logging.CRITICAL)  # rejections are logged per event kind; keep the report readable
    events = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    print(f"Control Plane load test: {events:,} events per scenario\n")
    run("unsigned, mode off", events)
    run("signed, enforce", events, signed=True)
    run("signed, observe", events, signed=True, mode="observe")
    run("signed, enforce, 50-rule configs", events, signed=True, pad=4000)
    run("default limits (20/s, burst 100)", events, unlimited=False)
    run("oversize rejection (cap 64 B)", events, unlimited=True, max_event_bytes=64)


if __name__ == "__main__":
    main()
