use std::env;
use std::io::{self, Read, Write};
use std::process;
use postoffice_loadgen::header;
use postoffice_loadgen::mqtt::{PubConfig, SinkConfig, run_in_memory_loopback, run_publisher, run_sink};

fn parse_arg(args: &[String], flag: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn usage() -> ! {
    eprintln!(
        "Usage:
  postoffice-loadgen encode --producer <u32> --seq <u64> --ts <u64> [--size <usize>] [--body <str>]
  postoffice-loadgen decode
  postoffice-loadgen pub --addr <host:port> --topic <str> --rate <u64> --count <u64> [--size <usize>] [--producer <u32>] [--qos <0|1>]
  postoffice-loadgen sink --addr <host:port> --topic <str> --count <u64> [--timeout-ms <u64>] [--producer <u32>] [--qos <0|1>]
  postoffice-loadgen loopback [--rate <u64>] [--count <u64>] [--size <usize>] [--qos <0|1>]"
    );
    process::exit(2);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        usage();
    }
    match args[1].as_str() {
        "encode" => {
            let producer: u32 = parse_arg(&args, "--producer", "1").parse().unwrap();
            let seq: u64 = parse_arg(&args, "--seq", "0").parse().unwrap();
            let ts: u64 = parse_arg(&args, "--ts", "0").parse().unwrap();
            let body = parse_arg(&args, "--body", "");
            let size: Option<usize> = args
                .windows(2)
                .find(|w| w[0] == "--size")
                .map(|w| w[1].parse().unwrap());
            let bytes = header::encode(producer, seq, ts, body.as_bytes(), size).unwrap();
            io::stdout().write_all(&bytes).unwrap();
        }
        "decode" => {
            let mut buf = Vec::new();
            io::stdin().read_to_end(&mut buf).unwrap();
            match header::decode(&buf) {
                Some(h) => {
                    println!(
                        "{{\"producer\":{},\"seq\":{},\"send_ts_ns\":{},\"intact\":{},\"body_len\":{}}}",
                        h.producer, h.seq, h.send_ts_ns, h.intact, h.body_len
                    );
                }
                None => {
                    println!("null");
                    process::exit(1);
                }
            }
        }
        "pub" => {
            let cfg = PubConfig {
                addr: parse_arg(&args, "--addr", "127.0.0.1:1883"),
                topic: parse_arg(&args, "--topic", "perf/test"),
                producer_id: parse_arg(&args, "--producer", "1").parse().unwrap(),
                rate_per_sec: parse_arg(&args, "--rate", "1000").parse().unwrap(),
                count: parse_arg(&args, "--count", "100").parse().unwrap(),
                payload_size: parse_arg(&args, "--size", "1024").parse().unwrap(),
                qos: parse_arg(&args, "--qos", "0").parse().unwrap(),
            };
            let stats = run_publisher(&cfg).unwrap_or_else(|e| {
                eprintln!("pub error: {e}");
                process::exit(1);
            });
            println!("{}", stats.to_json());
        }
        "sink" => {
            let cfg = SinkConfig {
                addr: parse_arg(&args, "--addr", "127.0.0.1:1883"),
                topic: parse_arg(&args, "--topic", "perf/test"),
                producer_id: parse_arg(&args, "--producer", "1").parse().unwrap(),
                expected_count: parse_arg(&args, "--count", "100").parse().unwrap(),
                timeout_ms: parse_arg(&args, "--timeout-ms", "5000").parse().unwrap(),
                qos: parse_arg(&args, "--qos", "0").parse().unwrap(),
            };
            let report = run_sink(&cfg, None).unwrap_or_else(|e| {
                eprintln!("sink error: {e}");
                process::exit(1);
            });
            println!("{}", report.to_json());
            if !report.passed {
                process::exit(1);
            }
        }
        "loopback" => {
            let rate: u64 = parse_arg(&args, "--rate", "10000").parse().unwrap();
            let count: u64 = parse_arg(&args, "--count", "200").parse().unwrap();
            let size: usize = parse_arg(&args, "--size", "1024").parse().unwrap();
            let qos: u8 = parse_arg(&args, "--qos", "0").parse().unwrap();
            let (pub_stats, report) = run_in_memory_loopback(rate, count, size, 1, qos);
            println!(
                "{{\"publisher\":{},\"oracle\":{}}}",
                pub_stats.to_json(),
                report.to_json()
            );
            if !report.passed {
                process::exit(1);
            }
        }
        _ => usage(),
    }
}
