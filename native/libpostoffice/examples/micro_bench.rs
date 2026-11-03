//! Native T1 Micro-benchmark runner for `libpostoffice` (`perf/micro.py --compare-native`).

use std::env;
use std::time::Instant;

use postoffice::codec::dlq_fingerprint;
use postoffice::firewall::{Firewall, MessageContext, TokenBucket};
use postoffice::governor::MemoryGovernor;
use postoffice::trie::TopicTrie;

const TOPIC: &str = "devices/d-17/telemetry";
const BIG: f64 = 1e12;

fn make_payload() -> Vec<u8> {
    let mut s = format!(
        "{{\"device_id\": \"d-17\", \"temp\": 21.5, \"pad\": \"{}\"}}",
        "x".repeat(960)
    );
    s.truncate(1024);
    s.into_bytes()
}

fn time_op<F: FnMut()>(mut f: F, n: usize, repeats: usize) -> (f64, f64, f64, u64) {
    for _ in 0..n {
        f();
    }
    let mut runs = Vec::with_capacity(repeats);
    for _ in 0..repeats {
        let t0 = Instant::now();
        for _ in 0..n {
            f();
        }
        runs.push((t0.elapsed().as_nanos() as f64) / (n as f64));
    }
    runs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = runs[runs.len() / 2];
    let min = runs[0];
    let max = *runs.last().unwrap();
    let ops = (1e9 / med.max(0.1)).round() as u64;
    ((med * 10.0).round() / 10.0, (min * 10.0).round() / 10.0, (max * 10.0).round() / 10.0, ops)
}

fn fmt_metric((med, min, max, ops): (f64, f64, f64, u64)) -> String {
    format!(
        "{{\"ns_per_op\":{:.1},\"min\":{:.1},\"max\":{:.1},\"ops_per_s\":{}}}",
        med, min, max, ops
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let quick = args.iter().any(|a| a == "--quick");
    let (n, repeats) = if quick { (500, 3) } else { (50_000, 7) };
    let payload = make_payload();

    // 1. TopicTrie
    let queries = [
        "sensors/facility/site_3/temp",
        "devices/gateway/5",
        "telemetry/line_7/a/voltage",
    ];
    let mut trie_entries = Vec::new();
    let trie_sizes: &[usize] = if quick { &[10, 1_000] } else { &[10, 1_000, 100_000] };
    for &size in trie_sizes {
        let mut trie = TopicTrie::new();
        for i in 0..size {
            let pat = if i % 10 == 0 {
                format!("sensors/+/site_{}/temp", i)
            } else if i % 7 == 0 {
                format!("telemetry/line_{}/#", i)
            } else if i % 5 == 0 {
                format!("devices/+/{}", i)
            } else {
                format!("sensors/facility/site_{}/temp", i)
            };
            let _ = trie.add_route(&pat, "1", false);
        }
        let mut idx = 0usize;
        let m = time_op(
            || {
                let q = queries[idx % 3];
                idx = idx.wrapping_add(1);
                std::hint::black_box(trie.match_topic(q));
            },
            n,
            repeats,
        );
        trie_entries.push(format!("\"filters={}\":{}", size, fmt_metric(m)));
    }

    // 2. TokenBucket + MemoryGovernor
    let mut tb = TokenBucket::new(BIG, BIG, 0.0).unwrap();
    let tb_1 = time_op(
        || {
            std::hint::black_box(tb.try_consume(1.0, 0.0));
        },
        n,
        repeats,
    );
    let gov = MemoryGovernor::new(1 << 30, 0.80).unwrap();
    let gov_m = time_op(
        || {
            if gov.try_acquire(1024) {
                gov.release(1024);
            }
        },
        n,
        repeats,
    );

    // 3. DLQ fingerprint
    let fp_m = time_op(
        || {
            std::hint::black_box(dlq_fingerprint(b"mqtt", b"devices/d-17", b"err", &payload));
        },
        n,
        repeats,
    );

    // 4. Firewall
    let mut fw_entries = Vec::new();
    for &len in &[1usize, 5, 20] {
        let cfg = make_fw_config(len, false, false);
        let mut fw = Firewall::from_json_str(&cfg).unwrap();
        let m = time_op(
            || {
                let ctx = MessageContext {
                    broker: "mqtt",
                    sender: None,
                    topic: TOPIC,
                    payload: &payload,
                    now: 1.0,
                };
                std::hint::black_box(fw.evaluate(&ctx));
            },
            n,
            repeats,
        );
        fw_entries.push(format!("\"rules={}\":{}", len, fmt_metric(m)));
    }
    {
        let cfg = make_fw_config(5, true, false);
        let mut fw = Firewall::from_json_str(&cfg).unwrap();
        let m = time_op(
            || {
                let ctx = MessageContext {
                    broker: "mqtt",
                    sender: None,
                    topic: TOPIC,
                    payload: &payload,
                    now: 1.0,
                };
                std::hint::black_box(fw.evaluate(&ctx));
            },
            n,
            repeats,
        );
        fw_entries.push(format!("\"rules=5+content_guard\":{}", fmt_metric(m)));
    }
    {
        let cfg = make_fw_config(5, false, true);
        let mut fw = Firewall::from_json_str(&cfg).unwrap();
        let m = time_op(
            || {
                let ctx = MessageContext {
                    broker: "mqtt",
                    sender: None,
                    topic: TOPIC,
                    payload: &payload,
                    now: 1.0,
                };
                std::hint::black_box(fw.evaluate(&ctx));
            },
            n,
            repeats,
        );
        fw_entries.push(format!("\"rules=5+ledger\":{}", fmt_metric(m)));
    }

    println!(
        "{{\"topic_trie.match\":{{{}}},\"token_bucket.try_consume\":{{\"threads=1\":{},\"memory_governor\":{}}},\"budgeted_dlq\":{{\"dlq_fingerprint\":{}}},\"firewall.evaluate\":{{{}}}}}",
        trie_entries.join(","),
        fmt_metric(tb_1),
        fmt_metric(gov_m),
        fmt_metric(fp_m),
        fw_entries.join(",")
    );
}

fn make_fw_config(len: usize, guard: bool, ledger: bool) -> String {
    let base = [
        r#"{"type":"max_payload_bytes","max_bytes":1048576}"#,
        r#"{"type":"topic_shape","deny":["$SYS/#","admin/+/secret"]}"#,
        r#"{"type":"rate_limit","rate":1000000000000.0,"key":"broker"}"#,
        r#"{"type":"rate_limit","rate":1000000000000.0,"key":"topic"}"#,
        r#"{"type":"rate_limit","rate":1000000000000.0,"key":"identity","template":"devices/{device_id}/telemetry"}"#,
    ];
    let mut rules: Vec<&str> = (0..len).map(|i| base[i % base.len()]).collect();
    if guard {
        rules.push(
            r#"{"type":"content_guard","topic":"devices/#","required_fields":["device_id","temp"]}"#,
        );
    }
    let ledger_part = if ledger { r#","ledger":{}"# } else { "" };
    format!("{{\"rules\":[{}]{}}}", rules.join(","), ledger_part)
}
