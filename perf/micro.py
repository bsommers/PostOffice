"""
T1 micro-benchmarks for the current Python components (PERFORMANCE_TEST_PLAN §2 T1, slice P-A).

Manual run, not collected by unittest; numbers are this machine's, single process:

    PYTHONPATH=src .venv/bin/python -m perf.micro [--quick] [--out perf/results]

Every component runs with limits set high enough that it always allows, so the timing is the
pass-through path a legitimate 1 KB message pays. Budget context: at 10,000 msg/s per worker the whole
per-message budget is 100 us; the 5 ms p99 SLO is bridge-added latency.
"""
import argparse
import json
import statistics
import threading
import time
from typing import Callable, Dict, List

from perf import results
from postoffice.dlq import BudgetedDlq
from postoffice.ingress.firewall import Firewall
from postoffice.ingress.rules import MessageContext
from postoffice.ratelimit import TokenBucket
from postoffice.trie import TopicTrie

PAYLOAD = json.dumps({"device_id": "d-17", "temp": 21.5, "pad": "x" * 960}).encode()[:1024]
TOPIC = "devices/d-17/telemetry"
BIG = 1e12  # rate/burst that never runs out


def _time(fn: Callable[[], None], n: int, repeats: int) -> Dict[str, float]:
    """Median and spread of ns/op over ``repeats`` runs of ``n`` calls (after one warm-up run)."""
    def once() -> float:
        t0 = time.perf_counter_ns()
        for _ in range(n):
            fn()
        return (time.perf_counter_ns() - t0) / n
    once()
    runs = [once() for _ in range(repeats)]
    return {"ns_per_op": round(statistics.median(runs), 1), "min": round(min(runs), 1),
            "max": round(max(runs), 1), "ops_per_s": round(1e9 / statistics.median(runs))}


def bench_trie(n: int, repeats: int) -> Dict[str, dict]:
    out = {}
    queries = ["sensors/facility/site_3/temp", "devices/gateway/5", "telemetry/line_7/a/voltage"]
    for size in (10, 1_000, 100_000):
        trie = TopicTrie()
        # Distinct filters, so each query matches a handful: this times lookup, not result-list size.
        for i in range(size):
            pattern = (f"sensors/+/site_{i}/temp" if i % 10 == 0 else f"telemetry/line_{i}/#" if i % 7 == 0
                       else f"devices/+/{i}" if i % 5 == 0 else f"sensors/facility/site_{i}/temp")
            trie.insert(pattern, i)
        it = iter(range(1 << 62))
        out[f"filters={size}"] = _time(lambda: trie.match(queries[next(it) % 3]), n, repeats)
    return out


def bench_token_bucket(n: int, repeats: int) -> Dict[str, dict]:
    out = {"threads=1": _time(TokenBucket(BIG, BIG).try_consume, n, repeats)}
    bucket, threads = TokenBucket(BIG, BIG), 16
    per = max(1, n // threads)

    def worker():
        for _ in range(per):
            bucket.try_consume()
    runs: List[float] = []
    for _ in range(repeats):
        ts = [threading.Thread(target=worker) for _ in range(threads)]
        t0 = time.perf_counter_ns()
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        runs.append((time.perf_counter_ns() - t0) / (per * threads))
    med = statistics.median(runs)
    out[f"threads={threads}"] = {"ns_per_op": round(med, 1), "min": round(min(runs), 1),
                                 "max": round(max(runs), 1), "ops_per_s": round(1e9 / med)}
    return out


def bench_dlq(n: int, repeats: int) -> Dict[str, dict]:
    plain = BudgetedDlq(msgs_per_sec=BIG, bytes_per_sec=BIG)
    dedupe = BudgetedDlq(msgs_per_sec=BIG, bytes_per_sec=BIG, dedupe_window=60.0)
    it = iter(range(1 << 62))
    return {
        "check": _time(lambda: plain.check("mqtt", len(PAYLOAD)), n, repeats),
        "admit+dedupe": _time(lambda: dedupe.admit("mqtt", len(PAYLOAD), fingerprint=str(next(it))), n, repeats),
    }


def _chain(length: int) -> List[dict]:
    base = [
        {"type": "max_payload_bytes", "max_bytes": 1 << 20},
        {"type": "topic_shape", "deny": ["$SYS/#", "admin/+/secret"]},
        {"type": "rate_limit", "rate": BIG, "key": "broker"},
        {"type": "rate_limit", "rate": BIG, "key": "topic"},
        {"type": "rate_limit", "rate": BIG, "key": "identity", "template": "devices/{device_id}/telemetry"},
    ]
    return [dict(base[i % len(base)]) for i in range(length)]


def bench_firewall(n: int, repeats: int) -> Dict[str, dict]:
    ctx = MessageContext("mqtt", TOPIC, len(PAYLOAD), PAYLOAD)
    out = {}
    for length in (1, 5, 20):
        fw = Firewall.from_dict({"rules": _chain(length)})
        out[f"rules={length}"] = _time(lambda: fw.evaluate(ctx), n, repeats)
    guarded = Firewall.from_dict({"rules": _chain(5) + [{"type": "content_guard", "topic": "devices/#",
                                                         "required_fields": ["device_id", "temp"]}]})
    out["rules=5+content_guard"] = _time(lambda: guarded.evaluate(ctx), n, repeats)
    ledgered = Firewall.from_dict({"rules": _chain(5), "ledger": {}})
    out["rules=5+ledger"] = _time(lambda: ledgered.evaluate(ctx), n, repeats)
    return out


BENCHES = {"topic_trie.match": bench_trie, "token_bucket.try_consume": bench_token_bucket,
           "budgeted_dlq": bench_dlq, "firewall.evaluate": bench_firewall}


def main(argv=None) -> Dict[str, Dict[str, dict]]:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--quick", action="store_true", help="fewer iterations (smoke run)")
    ap.add_argument("--out", default=None, help="directory for the results JSON")
    args = ap.parse_args(argv)
    n, repeats = (2_000, 3) if args.quick else (50_000, 7)
    metrics = {}
    print(f"{'benchmark':<28} {'case':<24} {'ns/op':>10} {'ops/s':>12}  (median of {repeats} x {n:,})")
    for name, fn in BENCHES.items():
        metrics[name] = fn(n, repeats)
        for case, m in metrics[name].items():
            print(f"{name:<28} {case:<24} {m['ns_per_op']:>10,.0f} {m['ops_per_s']:>12,}")
    if args.out:
        doc = results.make_result("t1-micro-python", {"iterations": n, "repeats": repeats, "benchmarks": metrics})
        print("wrote", results.write_result(doc, args.out))
    return metrics


if __name__ == "__main__":
    main()
