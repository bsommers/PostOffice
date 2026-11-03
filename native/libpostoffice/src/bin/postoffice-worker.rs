use std::env;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use postoffice::engine::Engine;
use postoffice::redis_sync::converge_from_redis;

fn now_sec() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut redis_host = env::var("REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mut redis_port: u16 = env::var("REDIS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);
    let mut sync_once = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--redis-host" if i + 1 < args.len() => {
                redis_host = args[i + 1].clone();
                i += 2;
            }
            "--redis-port" if i + 1 < args.len() => {
                redis_port = args[i + 1].parse().unwrap_or(6379);
                i += 2;
            }
            "--sync-once" => {
                sync_once = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    let engine = Engine::new(100);
    let mut state = engine.state.lock().unwrap();
    match converge_from_redis(&mut state, &redis_host, redis_port, now_sec()) {
        Ok(report) => {
            println!("{}", report.to_json_string());
            if sync_once {
                return;
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            process::exit(1);
        }
    }
}
