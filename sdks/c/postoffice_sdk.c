/*
 * PostOffice Native Client SDK Demonstration & Verification (C)
 * Links against include/postoffice.h and native/libpostoffice.so
 */

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postoffice.h"

static po_slice_t make_slice(const char *str) {
    po_slice_t s;
    s.data = (const uint8_t *)str;
    s.len = str ? strlen(str) : 0;
    return s;
}

static po_slice_t make_bytes_slice(const uint8_t *data, size_t len) {
    po_slice_t s;
    s.data = data;
    s.len = len;
    return s;
}

static void bytes_to_hex(const uint8_t *data, size_t len, char *out) {
    static const char hex_chars[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        out[i * 2] = hex_chars[(data[i] >> 4) & 0x0F];
        out[i * 2 + 1] = hex_chars[data[i] & 0x0F];
    }
    out[len * 2] = '\0';
}

static void test_abi_version(void) {
    uint32_t version = po_abi_version();
    assert(version == PO_ABI_VERSION);
    assert(version == 1U);
}

static void test_wire_codec(void) {
    const char *payload_json = "{\"id\": \"dev-1\", \"temp\": 21.5}";
    po_slice_t payload_slice = make_slice(payload_json);
    po_owned_buf_t framed_buf = {0};

    // 1. Encode Confluent 5-byte wire frame with schema_id = 1
    po_status_t rc = po_codec_encode_confluent(1, payload_slice, &framed_buf);
    assert(rc == PO_OK);
    assert(framed_buf.data != NULL);
    assert(framed_buf.len == 5 + strlen(payload_json));

    // Expected hex from conformance/vectors/wire_framing.json
    const char *expected_hex = "00000000017b226964223a20226465762d31222c202274656d70223a2032312e357d";
    char encoded_hex[128] = {0};
    bytes_to_hex(framed_buf.data, framed_buf.len, encoded_hex);
    assert(strcmp(encoded_hex, expected_hex) == 0);

    // 2. Decode back and verify schema_id and payload
    uint32_t decoded_schema_id = 0;
    po_owned_buf_t decoded_payload = {0};
    rc = po_codec_decode_confluent(
        make_bytes_slice(framed_buf.data, framed_buf.len),
        &decoded_schema_id,
        &decoded_payload
    );
    assert(rc == PO_OK);
    assert(decoded_schema_id == 1);
    assert(decoded_payload.len == strlen(payload_json));
    assert(memcmp(decoded_payload.data, payload_json, decoded_payload.len) == 0);

    po_buf_free(&decoded_payload);
    po_buf_free(&framed_buf);

    // 3. Error case: too short (< 5 bytes)
    const uint8_t short_bytes[] = {0x00, 0x00, 0x01};
    rc = po_codec_decode_confluent(
        make_bytes_slice(short_bytes, sizeof(short_bytes)),
        &decoded_schema_id,
        &decoded_payload
    );
    assert(rc == PO_ERR_WIRE_FORMAT);

    // 4. Error case: bad magic byte (first byte != 0x00)
    const uint8_t bad_magic[] = {0x01, 0x00, 0x00, 0x00, 0x01, 0x7b, 0x7d};
    rc = po_codec_decode_confluent(
        make_bytes_slice(bad_magic, sizeof(bad_magic)),
        &decoded_schema_id,
        &decoded_payload
    );
    assert(rc == PO_ERR_WIRE_FORMAT);
}

static void test_dlq_fingerprint(void) {
    char hex[33] = {0};
    po_status_t rc = po_dlq_fingerprint(
        make_slice("mqtt_a"),
        make_slice("sensors/temp"),
        make_slice("schema_validation_failed"),
        make_slice("{\"temp\": \"invalid\"}"),
        hex
    );
    assert(rc == PO_OK);
    assert(strlen(hex) == 32);
    // Deterministic: repeated call gives same fingerprint
    char hex2[33] = {0};
    rc = po_dlq_fingerprint(
        make_slice("mqtt_a"),
        make_slice("sensors/temp"),
        make_slice("schema_validation_failed"),
        make_slice("{\"temp\": \"invalid\"}"),
        hex2
    );
    assert(rc == PO_OK);
    assert(strcmp(hex, hex2) == 0);
}

static void test_topic_trie(void) {
    po_trie_t *trie = NULL;
    po_status_t rc = po_trie_new(&trie);
    assert(rc == PO_OK);
    assert(trie != NULL);

    bool added = false;
    rc = po_trie_add_route(trie, make_slice("#"), make_slice("{\"id\":\"root_hash\"}"), &added);
    assert(rc == PO_OK && added);

    rc = po_trie_add_route(trie, make_slice("sensors/+/temp"), make_slice("{\"id\":\"single_plus\"}"), &added);
    assert(rc == PO_OK && added);

    rc = po_trie_add_route(trie, make_slice("$SYS/#"), make_slice("{\"id\":\"sys_multi\"}"), &added);
    assert(rc == PO_OK && added);

    // Match normal topic: matches root_hash and single_plus
    po_owned_buf_t matches = {0};
    rc = po_trie_match(trie, make_slice("sensors/room1/temp"), &matches);
    assert(rc == PO_OK);
    assert(matches.data != NULL);
    char *match_str = strndup((const char *)matches.data, matches.len);
    assert(strstr(match_str, "root_hash") != NULL);
    assert(strstr(match_str, "single_plus") != NULL);
    free(match_str);
    po_buf_free(&matches);

    // Match $SYS topic: MQTT §5.0 isolation means root '#' does NOT match $SYS
    rc = po_trie_match(trie, make_slice("$SYS/broker/uptime"), &matches);
    assert(rc == PO_OK);
    assert(matches.data != NULL);
    match_str = strndup((const char *)matches.data, matches.len);
    assert(strstr(match_str, "sys_multi") != NULL);
    assert(strstr(match_str, "root_hash") == NULL);
    free(match_str);
    po_buf_free(&matches);

    po_trie_free(trie);
}

static void test_firewall(void) {
    po_firewall_t *fw = NULL;
    // Basic firewall configuration with permissive defaults
    const char *cfg = "{\"rules\": [], \"ledger\": {\"enforce\": false}}";
    po_status_t rc = po_firewall_new(make_slice(cfg), &fw);
    assert(rc == PO_OK);
    assert(fw != NULL);

    po_firewall_verdict_t verdict;
    memset(&verdict, 0, sizeof(verdict));
    rc = po_firewall_check(
        fw,
        make_slice("device-42"),
        make_slice("sensors/temp"),
        make_slice("{\"temp\": 22.0}"),
        1000.0,
        &verdict
    );
    assert(rc == PO_OK);
    assert(verdict.kind == PO_VERDICT_ALLOW);

    po_firewall_free(fw);
}

static void test_engine(void) {
    po_engine_t *engine = NULL;
    po_status_t rc = po_engine_new(100, &engine);
    assert(rc == PO_OK);
    assert(engine != NULL);

    // Register source broker
    rc = po_engine_apply_action(
        engine,
        make_slice("add_broker"),
        make_slice("{\"name\":\"mqtt_src\",\"protocol\":\"mqtt\",\"kwargs\":{}}")
    );
    assert(rc == PO_OK);

    // Register target broker
    rc = po_engine_apply_action(
        engine,
        make_slice("add_broker"),
        make_slice("{\"name\":\"amqp_tgt\",\"protocol\":\"amqp\",\"kwargs\":{}}")
    );
    assert(rc == PO_OK);

    // Add route
    rc = po_engine_apply_action(
        engine,
        make_slice("add_route"),
        make_slice("{\"source_broker\":\"mqtt_src\",\"source_topic\":\"telemetry/+\",\"target_broker\":\"amqp_tgt\",\"target_topic\":\"ingest\",\"target_kwargs\":{}}")
    );
    assert(rc == PO_OK);

    // Route a message
    po_owned_buf_t outcome = {0};
    rc = po_engine_route(
        engine,
        make_slice("mqtt_src"),
        make_slice("telemetry/room1"),
        make_slice("{\"val\": 42}"),
        make_slice("{}"),
        1000.0,
        &outcome
    );
    assert(rc == PO_OK);
    assert(outcome.data != NULL);

    char *out_str = strndup((const char *)outcome.data, outcome.len);
    assert(strstr(out_str, "\"acked\":true") != NULL || strstr(out_str, "\"acked\": true") != NULL);
    assert(strstr(out_str, "amqp_tgt") != NULL);
    free(out_str);
    po_buf_free(&outcome);

    po_engine_free(engine);
}

static void test_target_queue(void) {
    po_target_queue_t *queue = NULL;
    po_status_t rc = po_target_queue_new(10, 1024, 60.0, make_slice("reject"), &queue);
    assert(rc == PO_OK);
    assert(queue != NULL);

    po_owned_buf_t enq_res = {0};
    rc = po_target_queue_enqueue(
        queue,
        make_slice("telemetry/metrics"),
        make_slice("{\"cpu\": 0.25}"),
        make_slice("{}"),
        1000.0,
        &enq_res
    );
    assert(rc == PO_OK);
    char *enq_str = strndup((const char *)enq_res.data, enq_res.len);
    assert(strstr(enq_str, "\"accepted\":true") != NULL || strstr(enq_str, "\"accepted\": true") != NULL);
    free(enq_str);
    po_buf_free(&enq_res);

    po_owned_buf_t drain_res = {0};
    rc = po_target_queue_drain(queue, 10, 1000.0, &drain_res);
    assert(rc == PO_OK);
    char *drain_str = strndup((const char *)drain_res.data, drain_res.len);
    assert(strstr(drain_str, "telemetry/metrics") != NULL);
    free(drain_str);
    po_buf_free(&drain_res);

    po_target_queue_free(queue);
}

int main(void) {
    test_abi_version();
    test_wire_codec();
    test_dlq_fingerprint();
    test_topic_trie();
    test_firewall();
    test_engine();
    test_target_queue();

    printf("C_SDK_OK\n");
    return 0;
}
