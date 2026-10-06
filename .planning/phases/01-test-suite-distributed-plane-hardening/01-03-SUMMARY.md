---
phase: 01-test-suite-distributed-plane-hardening
plan: 03
subsystem: architecture-research
tags:
  - architecture
  - research-spike
  - benchmarks
  - language-evaluation
  - go
  - rust
  - zig
  - python

requires:
  - 01-02-PLAN.md
provides:
  - Exhaustive multi-language rewrite research spike report in .planning/research/LANGUAGE_REWRITE_SPIKE.md
affects:
  - ROADMAP.md
  - Phase 3 planning

tech-stack:
  added: []
  patterns:
    - Comparative trade-off evaluation of network messaging runtimes
    - Hybrid Rust PyO3 migration roadmap vs pure Go rewrite architecture

key-files:
  created:
    - .planning/research/LANGUAGE_REWRITE_SPIKE.md
  modified: []

key-decisions:
  - "Concluded Zig is unviable for PostOffice due to lack of mature native AMQP and Kafka SDKs and pre-1.0 language churn"
  - "Concluded C/C++ provide no performance benefit over Rust while introducing memory safety liabilities and CMake build friction"
  - "Selected Go as the top recommendation if a full rewrite is required (9.2/10), due to pure-Go broker drivers (franz-go, paho.golang, amqp091-go), goroutine concurrency, and single static binary deployment"
  - "Selected Rust as the top choice for zero-GC ultra-low latency or embedded edge deployments (9.0/10)"
  - "Recommended retaining Python 3.12 for milestone v1.1 and implementing Phase 3 Radix Trie optimization before considering language rewrites"

requirements-completed:
  - SPIKE-LANG-01

coverage:
  - id: D1
    description: "Multi-language rewrite research spike evaluating Go, Rust, Zig, C, and C++ against Python"
    requirement: "SPIKE-LANG-01"
    verification:
      - kind: doc
        ref: ".planning/research/LANGUAGE_REWRITE_SPIKE.md"
        status: pass
    human_judgment: false
---

# Plan 01-03 Summary

Completed the Multi-Language Rewrite Research Spike evaluating whether to rewrite PostOffice in Go, Rust, Zig, C, or C++ versus retaining Python.

## Key Findings
1. **Python Baseline:** Excellent developer velocity and dynamic configuration. Bottlenecks: GIL contention across broker worker threads, string tokenization allocations in topic matching, and ~25,000 msgs/sec ceiling.
2. **Go (Golang - 9.2/10):** The ideal rewrite candidate. Pure Go driver ecosystem (`twmb/franz-go`, `rabbitmq/amqp091-go`, `paho.golang`) eliminates C dependencies; goroutines scale seamlessly; provides 300k-500k msgs/sec in a ~15MB static scratch container.
3. **Rust (9.0/10):** Superior choice for hard-realtime/zero-jitter (<100μs) or embedded edge gateways (<16MB RAM). Slightly higher complexity curve for async lifecycle management.
4. **Zig (4.0/10):** Unviable. Absence of native AMQP/Kafka SDKs forces unsafe C FFI wrapping.
5. **C / C++ (3.5/10):** Unviable. High bug and security vulnerability surface with zero performance advantages over Rust.

## Recommendations
- **Milestone v1.1:** Retain Python; proceed to Phase 3 (Radix Trie optimization).
- **If Rewrite Mandated:** Select **Go** for cloud/data plane deployments, or **Rust** for resource-constrained edge gateways.
