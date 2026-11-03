/*
 * PostOffice Native Core & Client SDK — Stable C ABI (`libpostoffice`)
 *
 * Ownership & Memory Safety Contract:
 *   1. All engine, trie, firewall, governor, codec, and client objects are opaque
 *      handles (`po_*_t*`) allocated and freed exclusively by `libpostoffice`.
 *   2. Input slices (`po_slice_t`) are borrowed only for the duration of the
 *      function call; `libpostoffice` never retains caller pointers after return.
 *   3. Output buffers (`po_owned_buf_t`) returned by `libpostoffice` MUST be
 *      released exclusively by calling `po_buf_free(&buf)` — callers MUST NEVER
 *      pass `buf.data` to system `free()` directly.
 *   4. Every function returns a `po_status_t` (`PO_OK == 0` on success, negative
 *      error codes on failure) and never unwinds across the FFI boundary.
 */

#ifndef POSTOFFICE_H
#define POSTOFFICE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Stable ABI version constant. Bumped only on breaking ABI changes. */
#define PO_ABI_VERSION 1U

/** Status codes returned across all C ABI entry points. */
typedef enum po_status_t {
    PO_OK = 0,
    PO_ERR_INVALID_ARGUMENT = -1,
    PO_ERR_NOT_FOUND = -2,
    PO_ERR_OUT_OF_MEMORY = -3,
    PO_ERR_OVERLOAD_SHED = -4,
    PO_ERR_FIREWALL_DROP = -5,
    PO_ERR_FIREWALL_REJECT = -6,
    PO_ERR_WIRE_FORMAT = -7,
    PO_ERR_SIGNATURE_INVALID = -8,
    PO_ERR_UNSUPPORTED_VERSION = -9,
    PO_ERR_IO = -10,
    PO_ERR_INTERNAL = -99
} po_status_t;

/** Firewall verdict kind matching `FirewallVerdictKind` in `postoffice.proto`. */
typedef enum po_verdict_kind_t {
    PO_VERDICT_ALLOW = 1,
    PO_VERDICT_DROP = 2,
    PO_VERDICT_REJECT = 3
} po_verdict_kind_t;

/** Borrowed read-only byte slice (not required to be NUL-terminated). */
typedef struct po_slice_t {
    const uint8_t *data;
    size_t len;
} po_slice_t;

/**
 * Heap buffer owned by `libpostoffice`.
 * Must be released via `po_buf_free(&buf)`.
 */
typedef struct po_owned_buf_t {
    uint8_t *data;
    size_t len;
    size_t cap;
} po_owned_buf_t;

/** Structured firewall verdict populated by `po_firewall_check`. */
typedef struct po_firewall_verdict_t {
    po_verdict_kind_t kind;
    char rule[32];
    char reason[128];
} po_firewall_verdict_t;

/** Opaque handle types. */
typedef struct po_engine_t po_engine_t;
typedef struct po_trie_t po_trie_t;
typedef struct po_firewall_t po_firewall_t;
typedef struct po_governor_t po_governor_t;
typedef struct po_codec_t po_codec_t;
typedef struct po_client_t po_client_t;
typedef struct po_signer_t po_signer_t;
typedef struct po_dlq_budget_t po_dlq_budget_t;

/** Return the compiled `PO_ABI_VERSION` of the loaded shared library. */
uint32_t po_abi_version(void);

/** Release a buffer allocated by `libpostoffice` and zero its fields. */
void po_buf_free(po_owned_buf_t *buf);

/* =========================================================================
 * TopicTrie (`po_trie_t`)
 * ========================================================================= */

po_status_t po_trie_new(po_trie_t **out_trie);
void po_trie_free(po_trie_t *trie);

/**
 * Add an idempotent route pattern (`+` and `#` MQTT wildcards; `$` system
 * topics are never matched by root `+` or `#`).
 * `out_added` is set to `true` if inserted, `false` if duplicate.
 */
po_status_t po_trie_add_route(
    po_trie_t *trie,
    po_slice_t pattern,
    po_slice_t route_json,
    bool *out_added
);

po_status_t po_trie_remove_route(
    po_trie_t *trie,
    po_slice_t pattern,
    po_slice_t target_broker,
    po_slice_t target_topic,
    size_t *out_removed_count
);

/**
 * Match `topic` against `trie` and write a JSON array of matched route
 * objects into `out_routes_json`. Release with `po_buf_free(out_routes_json)`.
 */
po_status_t po_trie_match(
    const po_trie_t *trie,
    po_slice_t topic,
    po_owned_buf_t *out_routes_json
);

/* =========================================================================
 * Ingress Firewall + ContentGuard + SenderLedger (`po_firewall_t`)
 * ========================================================================= */

po_status_t po_firewall_new(
    po_slice_t rules_json,
    po_firewall_t **out_firewall
);
void po_firewall_free(po_firewall_t *firewall);

po_status_t po_firewall_check(
    po_firewall_t *firewall,
    po_slice_t sender_id,
    po_slice_t topic,
    po_slice_t payload,
    double now_monotonic_sec,
    po_firewall_verdict_t *out_verdict
);

po_status_t po_firewall_evaluate(
    po_firewall_t *firewall,
    po_slice_t broker,
    po_slice_t sender_id,
    po_slice_t topic,
    po_slice_t payload,
    double now_monotonic_sec,
    po_owned_buf_t *out_verdict_json
);

po_status_t po_firewall_sender_report(
    const po_firewall_t *firewall,
    uint32_t limit,
    double now_monotonic_sec,
    po_owned_buf_t *out_report_json
);

/* =========================================================================
 * MemoryGovernor (`po_governor_t`)
 * ========================================================================= */

po_status_t po_governor_new(
    uint64_t budget_bytes,
    double shed_ratio,
    po_governor_t **out_governor
);
void po_governor_free(po_governor_t *governor);

po_status_t po_governor_try_acquire(
    po_governor_t *governor,
    uint64_t bytes
);

po_status_t po_governor_release(
    po_governor_t *governor,
    uint64_t bytes
);

uint64_t po_governor_in_use_bytes(const po_governor_t *governor);
uint64_t po_governor_shed_total(const po_governor_t *governor);
uint64_t po_governor_detect_host_ram_bytes(void);

/* =========================================================================
 * Wire Codec & DLQ Fingerprinting (`po_codec_t`)
 * ========================================================================= */

po_status_t po_codec_new(po_codec_t **out_codec);
void po_codec_free(po_codec_t *codec);

po_status_t po_codec_register_schema(
    po_codec_t *codec,
    uint32_t schema_id,
    po_slice_t name,
    po_slice_t schema_type,
    po_slice_t definition_json
);

po_status_t po_codec_serialize(
    const po_codec_t *codec,
    po_slice_t record_json,
    po_slice_t format_type,
    po_slice_t schema_name,
    bool wire_format,
    po_owned_buf_t *out_bytes
);

po_status_t po_codec_deserialize(
    const po_codec_t *codec,
    po_slice_t payload,
    po_slice_t format_type,
    po_slice_t schema_name,
    bool wire_format,
    po_owned_buf_t *out_record_json
);

/** Encode Confluent 5-byte wire header (`0x00` + big-endian uint32 schema_id + payload). */
po_status_t po_codec_encode_confluent(
    uint32_t schema_id,
    po_slice_t payload,
    po_owned_buf_t *out_framed
);

/** Decode Confluent 5-byte wire header into `out_schema_id` and `out_payload`. */
po_status_t po_codec_decode_confluent(
    po_slice_t framed,
    uint32_t *out_schema_id,
    po_owned_buf_t *out_payload
);

/**
 * Compute the 32-character lowercase hex BLAKE2b-128 DLQ fingerprint
 * (`source_broker \0 source_topic \0 error \0 payload[:4096]`).
 * `out_hex` must point to a caller buffer of at least 33 bytes (32 hex + NUL).
 */
po_status_t po_dlq_fingerprint(
    po_slice_t source_broker,
    po_slice_t source_topic,
    po_slice_t error_msg,
    po_slice_t payload,
    char out_hex[33]
);

/* =========================================================================
 * Platform Client Handle (`po_client_t`)
 * ========================================================================= */

po_status_t po_client_connect(
    po_slice_t endpoint_url,
    po_slice_t bearer_token,
    po_client_t **out_client
);
void po_client_free(po_client_t *client);

po_status_t po_client_publish(
    po_client_t *client,
    po_slice_t broker,
    po_slice_t topic,
    po_slice_t payload,
    po_slice_t sender_id,
    po_owned_buf_t *out_response_json
);

/* =========================================================================
 * Phase 3: D9 Signing, Action Validation, DLQ Budget, & Engine (`po_engine_t`)
 * ========================================================================= */

po_status_t po_signer_new(po_slice_t keyring, po_signer_t **out_signer);
void po_signer_free(po_signer_t *signer);

po_status_t po_signer_sign_event(
    const po_signer_t *signer,
    po_slice_t payload,
    po_owned_buf_t *out_json
);

po_status_t po_signer_sign_state(
    const po_signer_t *signer,
    po_slice_t hash_name,
    po_slice_t field,
    po_slice_t payload,
    po_owned_buf_t *out_json
);

po_status_t po_signer_sign_version(
    const po_signer_t *signer,
    int64_t version,
    po_owned_buf_t *out_json
);

po_status_t po_signer_verify_event(
    const po_signer_t *signer,
    po_slice_t envelope_json
);

po_status_t po_signer_verify_state(
    const po_signer_t *signer,
    po_slice_t hash_name,
    po_slice_t field,
    po_slice_t envelope_json
);

po_status_t po_signer_verify_version(
    const po_signer_t *signer,
    int64_t version,
    po_slice_t entry_json
);

po_status_t po_validate_action(po_slice_t action, po_slice_t data_json);

po_status_t po_dlq_format_payload(
    po_slice_t original_payload,
    po_slice_t source_broker,
    po_slice_t source_topic,
    po_slice_t error_msg,
    po_slice_t metadata_json,
    po_slice_t timestamp,
    po_owned_buf_t *out_bytes
);

po_status_t po_dlq_budget_new(
    size_t max_message_bytes,
    double msgs_per_sec,
    double bytes_per_sec,
    size_t max_sources,
    double dedupe_window_sec,
    size_t max_fingerprints,
    po_dlq_budget_t **out_budget
);
void po_dlq_budget_free(po_dlq_budget_t *budget);

po_status_t po_dlq_budget_admit(
    po_dlq_budget_t *budget,
    po_slice_t source_broker,
    po_slice_t source_topic,
    po_slice_t error_msg,
    po_slice_t payload,
    bool metadata_only,
    double now_sec,
    po_owned_buf_t *out_admission_json
);

po_status_t po_engine_new(uint32_t max_label_values, po_engine_t **out_engine);
void po_engine_free(po_engine_t *engine);

po_status_t po_engine_apply_action(
    po_engine_t *engine,
    po_slice_t action,
    po_slice_t data_json
);

po_status_t po_engine_route(
    po_engine_t *engine,
    po_slice_t source_broker,
    po_slice_t source_topic,
    po_slice_t payload,
    po_slice_t source_kwargs_json,
    double now_sec,
    po_owned_buf_t *out_outcome_json
);

po_status_t po_engine_metrics_text(
    const po_engine_t *engine,
    po_owned_buf_t *out_text
);

po_status_t po_engine_start_metrics_server(
    po_engine_t *engine,
    po_slice_t addr,
    uint16_t port,
    uint16_t *out_bound_port
);

po_status_t po_engine_stop_metrics_server(po_engine_t *engine);

po_status_t po_engine_sync_redis(
    po_engine_t *engine,
    po_slice_t host,
    uint16_t port,
    double now_sec,
    po_owned_buf_t *out_report_json
);

#ifdef __cplusplus
}
#endif

#endif /* POSTOFFICE_H */
