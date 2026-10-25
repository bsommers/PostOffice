# Fit, Limits and Design Decisions

[← Use cases](README.md) | [← Project README](../../README.md)

This page is deliberately candid. PostOffice's goal is to route and translate messages across protocols without losing them. The code pursues that, but a review on 2026-10-25 found places where the current implementation does not yet fully deliver it. Verify these against your requirements before a production rollout.

## Table of Contents
- [Delivery guarantees today](#delivery-guarantees-today)
- [Topic dialects](#topic-dialects)
- [Operational limits](#operational-limits)
- [Security posture](#security-posture)
- [Adoption checklist](#adoption-checklist)
- [Tracked improvements](#tracked-improvements)

## Delivery guarantees today

| Area | Current behaviour | Implication |
|---|---|---|
| Overall model | At-least-once intent; no exactly-once | Make consumers idempotent |
| MQTT and AMQP egress | The source message may be acknowledged when the publish is handed to the client library, before the target broker confirms | A target failure right after hand-off can lose a message |
| Kafka consume | Auto-commit is not disabled; offsets may be committed regardless of routing result | A crash can skip messages; verify on a live cluster |
| Kafka confirms | Delivery callbacks fire when polled, which happens on publish | Confirmation can lag after a burst ends |
| MQTT/AMQP reconnect | A first-connection failure or later drop is not always retried | Monitor and restart; do not assume self-healing |
| Publish buffer | In memory, bounded by count, lost on restart | Not a durable queue |
| Unroutable messages | `nack` without requeue by default; `dlq` and `drop` are opt-in. On Kafka and MQTT a nack is effectively a skip | Watch `unroutable_*` counters; use `on_unroutable="dlq"` if you must keep them |
| Fan-out failure routing | Fixed in this review cycle: each route now uses its own DLQ | Covered by `tests/test_p0_hardening.py` |

These are scheduled as Tasks 1–4 in `.planning/reviews/IMPLEMENTATION_TASKS.md`. Until they land, treat PostOffice as suitable for telemetry and event propagation, and test failure scenarios (broker down, consumer killed) in your own environment before using it for events you cannot regenerate.

## Topic dialects

The topic matcher speaks **MQTT** syntax: `/` separators, `+` for one level, `#` for the rest. AMQP routing keys use `.` and `*`; Kafka topics are flat names. Wildcard routing therefore works as designed for MQTT/NanoMQ sources. For AMQP or Kafka sources, use exact topic strings (or the `#` catch-all) on the route and filter in the downstream system. A dialect adapter is on the roadmap.

## Operational limits

- Routing runs on each client's callback thread, so a slow target can slow ingestion from that source. Use circuit breakers and size buffers deliberately.
- The standalone Data Plane worker syncs a snapshot and then listens on Redis Pub/Sub. Events published in the short gap, or while the worker's Redis connection was down, are not replayed. A resync design is planned.
- The worker uses an in-memory schema registry; share schemas across workers by constructing `PostOffice` with `RedisSchemaRegistry` in library mode.
- Wildcards cost depends on topic depth, not on route count.

## Security posture

From `.security-reports/scan-2026-10-25.md` (grade B, no Critical or High findings):

- **Redis is a privileged control channel.** Anyone who can write to it can change routes or read stored broker credentials. Isolate it, authenticate it, and encrypt it. Worker and admin clients currently take only host and port; password and TLS options are planned.
- **Metrics listener** binds all interfaces without authentication by default. Restrict by network policy until the default changes.
- **Credentials** are redacted from logs and `repr` as of this review cycle; they are still stored in Redis. Prefer secrets injected via the environment.
- **TLS verification is on by default.** `insecure_skip_verify` exists; do not use it outside development.

## Adoption checklist

1. Write down which messages you can regenerate and which you cannot.
2. Decide the DLQ topic and who monitors it. Set it on `PostOffice` and on critical routes.
3. Make consumers idempotent.
4. Enable circuit breakers and size publish buffers for your longest expected outage.
5. Scrape metrics; alert on DLQ growth and breaker open state.
6. Secure Redis and the metrics port.
7. Run your own failure drills with `docker compose up -d`: stop a target broker, kill a consumer, restart the worker.
8. Pin dependency versions (`requirements.txt` is currently unpinned).

## Tracked improvements

See `.planning/reviews/ARCHITECTURE_REVIEW_PLAN.md` and `.planning/reviews/IMPLEMENTATION_TASKS.md` for the full list: real publish confirms, reconnect supervision, Kafka offset handling, convergent Data Plane sync, topic dialects, durable buffers, and packaging/dependency pinning.
