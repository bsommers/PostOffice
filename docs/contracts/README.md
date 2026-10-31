# PostOffice v1 Frozen Contracts

This directory contains the language-neutral wire, storage, telemetry, and behavioral contracts frozen in Phase 1 of the PostOffice Native Rewrite (`docs/adr/0001-native-data-plane-language.md`). Both the Python reference implementation (`src/postoffice/`) and the Rust native engine (`libpostoffice` / `postoffice-worker`) MUST conform identically to these specifications and pass 100% of the deterministic vectors in `conformance/`.

## Contract Index

| Contract Document | Scope |
|---|---|
| [`control-plane-redis.md`](file:///home/bill/src/meso/PostOffice/docs/contracts/control-plane-redis.md) | Redis keys, hashes, Pub/Sub channel `postoffice:config_updates`, `schema_version`, convergence sequence, and D9 HMAC-SHA256 signing domains (`event\0`, `state\0`, `version\0`). |
| [`dlq-envelope.md`](file:///home/bill/src/meso/PostOffice/docs/contracts/dlq-envelope.md) | Dead-Letter Queue JSON envelope format, UTF-8 vs Base64 encoding, BLAKE2b-128 `dlq_fingerprint`, and `BudgetedDlq` rate/byte/dedupe/oversize semantics. |
| [`metrics.md`](file:///home/bill/src/meso/PostOffice/docs/contracts/metrics.md) | Prometheus metric names, types, label sets, and `LabelGuard` cardinality cap (`max_label_values=100`, overflow `"other"`). |
| [`routing-and-protocols.md`](file:///home/bill/src/meso/PostOffice/docs/contracts/routing-and-protocols.md) | `TopicTrie` (`+`, `#`, MQTT §4.7.2 `$`-prefix isolation), idempotent `add_route`, unroutable policies (`nack`, `dlq`, `drop`), firewall verdicts (`allow`, `drop`, `reject`), QoS/`delivery_mode` semantic translation, Confluent 5-byte wire framing, and per-protocol ack/nack semantics. |
| [`firewall-schema.json`](file:///home/bill/src/meso/PostOffice/docs/contracts/firewall-schema.json) | JSON Schema (Draft 2020-12) for `set_ingress` firewall configuration (`max_size`, `topic_shape`, `rate_limit`, `content`, `quarantine`, `per_broker`, `per_route`). |
| [`route-schema.json`](file:///home/bill/src/meso/PostOffice/docs/contracts/route-schema.json) | JSON Schema (Draft 2020-12) for `add_route` single-target and fan-out route definitions. |
| [`api/postoffice/v1/postoffice.proto`](file:///home/bill/src/meso/PostOffice/api/postoffice/v1/postoffice.proto) | Protobuf v3 IDL (`AdminService`, `ObserveService`, `StreamService`). |
| [`api/openapi.json`](file:///home/bill/src/meso/PostOffice/api/openapi.json) | OpenAPI 3.1.0 REST/JSON specification mirroring `postoffice.v1`. |
| [`include/postoffice.h`](file:///home/bill/src/meso/PostOffice/include/postoffice.h) | Stable C99/C++ ABI header (`PO_ABI_VERSION == 1`) for `libpostoffice`. |
