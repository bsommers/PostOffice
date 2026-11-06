# PostOffice v1 Polyglot Client SDK Guide

This document describes the PostOffice v1 client SDKs for Python, C, Go, Rust, and JavaScript/TypeScript. All SDKs conform identically to the frozen protocol contracts in `docs/contracts/` and validate against deterministic test vectors in `conformance/vectors/`.

---

## 1. Conformance Matrix

| Feature / Contract | Python (`src/postoffice`) | C (`include/postoffice.h`) | Go (`sdks/go`) | Rust (`sdks/rust`) | JS/TS (`sdks/ts`) |
|---|---|---|---|---|---|
| **Confluent 5-byte Framing** (`wire_framing.json`) | `converters.py` | `po_codec_encode_confluent` / `po_codec_decode_confluent` | `EncodeConfluent` / `DecodeConfluent` | `WireCodec::encode_confluent` / `decode_confluent` | `encodeConfluent` / `decodeConfluent` |
| **MQTT Topic Wildcards & `$SYS` Isolation** (`topic_matching.json`) | `router.py` | `po_trie_new` / `po_trie_add_route` / `po_trie_match` | `MatchTopic` / `ValidatePattern` | `TopicMatcher::matches` / `validate_pattern` | `matchTopic` / `validatePattern` |
| **Control Plane HMAC Signing** (`control_plane_signing.json`) | `signing.py` | `po_signer_sign_event` / `verify_event` / `state` / `version` | `HmacSigner` (`SignEvent`, `VerifyState`, etc.) | `ControlSigner` | `HmacSigner` |
| **Router Delivery & Unroutable Policies** (`router_delivery.json`) | `router.py` / `native.py` | `po_engine_route` / `po_engine_apply_action` | HTTP Client (`ControlPlaneClient`) | `PostOfficeEngine::route` | HTTP Client (`ControlPlaneClient`) |
| **Admin REST API** (`admin_api.json`) | `admin.py` | `po_client_publish` | `ControlPlaneClient` | HTTP / RPC | `ControlPlaneClient` |
| **DLQ Fingerprinting (BLAKE2b-128)** (`dlq-envelope.md`) | `dlq.py` | `po_dlq_fingerprint` | stdlib / hash | `WireCodec::dlq_fingerprint` | Native / Crypto |

---

## 2. SDK Usage by Language

### 2.1. C SDK (`sdks/c/`)

Header: [`include/postoffice.h`](file:///home/bill/src/meso/PostOffice/include/postoffice.h)  
Library: `native/libpostoffice/target/release/libpostoffice.so`

```c
#include "postoffice.h"

// 1. Verify ABI Version
assert(po_abi_version() == PO_ABI_VERSION);

// 2. Wire Framing
po_owned_buf_t framed = {0};
po_slice_t payload = {.data = (const uint8_t*)"{\"id\":\"dev-1\"}", .len = 14};
po_codec_encode_confluent(1, payload, &framed);

uint32_t schema_id = 0;
po_owned_buf_t decoded = {0};
po_codec_decode_confluent(
    (po_slice_t){.data = framed.data, .len = framed.len},
    &schema_id,
    &decoded
);
po_buf_free(&decoded);
po_buf_free(&framed);

// 3. Topic Matching Trie
po_trie_t *trie = NULL;
po_trie_new(&trie);
bool added = false;
po_trie_add_route(trie, (po_slice_t){(const uint8_t*)"sensors/+", 9}, (po_slice_t){(const uint8_t*)"{\"id\":\"r1\"}", 11}, &added);
po_trie_free(trie);
```

Compile & Run:
```bash
gcc -Wall -Wextra -Iinclude sdks/c/postoffice_sdk.c -Lnative/libpostoffice/target/release -lpostoffice -Wl,-rpath,$(pwd)/native/libpostoffice/target/release -o /tmp/po_c_sdk
/tmp/po_c_sdk
```

---

### 2.2. Go SDK (`sdks/go/`)

Module: `github.com/bsommers/PostOffice/sdks/go`  
Dependencies: Go standard library only (`crypto/hmac`, `crypto/sha256`, `net/http`).

```go
import (
    "context"
    "github.com/bsommers/PostOffice/sdks/go"
)

// 1. Confluent Wire Framing
framed := postoffice.EncodeConfluent(1, []byte("{\"temp\":21.5}"))
schemaID, payload, err := postoffice.DecodeConfluent(framed)

// 2. MQTT Wildcard Topic Matching
matched := postoffice.MatchTopic("sensors/+/temp", "sensors/room1/temp") // true
isolated := postoffice.MatchTopic("#", "$SYS/broker/uptime")             // false (§5.0)

// 3. Control Plane HMAC-SHA256 Signing
signer, err := postoffice.NewHmacSigner("k1:first-secret-key-16b,k2:second-secret-key")
eventJSON, err := signer.SignEvent("{\"action\":\"add_route\"}")
valid := signer.VerifyEvent(eventJSON) // true

// 4. Control Plane Admin HTTP Client
client := postoffice.NewControlPlaneClient("http://127.0.0.1:8080", "bearer-token")
ack, err := client.RegisterBroker(context.Background(), postoffice.BrokerConfig{
    Name: "mqtt_in",
    Type: "mqtt",
})
```

Test:
```bash
cd sdks/go && /usr/local/go/bin/go test -v ./...
```

---

### 2.3. Rust SDK (`sdks/rust/`)

Crate: `postoffice-sdk = { path = "../../native/libpostoffice" }`  
Safe idiomatic wrappers over native zero-copy primitives.

```rust
use postoffice_sdk::{ControlSigner, PostOfficeEngine, TopicMatcher, WireCodec};

// 1. Wire Codec
let framed = WireCodec::encode_confluent(1, b"{\"id\":\"dev-1\"}");
let (schema_id, payload) = WireCodec::decode_confluent(&framed).unwrap();

// 2. Topic Matching
let matches = TopicMatcher::matches("sensors/#", "sensors/room1/temp"); // true
let sys_isolated = TopicMatcher::matches("#", "$SYS/broker/load");       // false

// 3. Control Signer
let signer = ControlSigner::from_keyring("k1:first-secret-key-16b").unwrap();
let signed_event = signer.sign_event("{\"action\":\"add_route\"}");
assert!(signer.verify_event(&signed_event));

// 4. Native Engine Execution
let engine = PostOfficeEngine::new(100);
engine.register_broker("mqtt_in", "mqtt", "{}").unwrap();
engine.register_broker("amqp_out", "amqp", "{}").unwrap();
engine.add_route("mqtt_in", "sensors/+", "amqp_out", "ingest", "{}").unwrap();
let outcome = engine.route("mqtt_in", "sensors/temp", b"{\"temp\":21.5}", "{}", 1000.0).unwrap();
```

Test:
```bash
CARGO_TARGET_DIR=/tmp/po-target-rust-sdk cargo test --manifest-path sdks/rust/Cargo.toml
```

---

### 2.4. JavaScript / TypeScript SDK (`sdks/ts/`)

Module: Zero-dependency ESM (`postoffice.mjs`) using Node `node:crypto` and `Buffer`.

```javascript
import {
  encodeConfluent,
  decodeConfluent,
  matchTopic,
  HmacSigner,
  ControlPlaneClient,
} from './postoffice.mjs';

// 1. Confluent Wire Framing
const framed = encodeConfluent(1, Buffer.from('{"temp":21.5}'));
const { schemaId, payload } = decodeConfluent(framed);

// 2. Topic Matching
const isMatch = matchTopic('sensors/+/temp', 'sensors/room1/temp'); // true
const isSysIsolated = matchTopic('#', '$SYS/broker/uptime');        // false

// 3. Control Plane HMAC Signer
const signer = new HmacSigner('k2:second-secret-key,k1:first-secret-key');
const signedEvent = signer.signEvent('{"action":"add_route"}');
const isValid = signer.verifyEvent(signedEvent); // true

// 4. Control Plane Client
const client = new ControlPlaneClient('http://127.0.0.1:8080', { token: 'jwt-token' });
const ack = await client.registerBroker({ name: 'mqtt_in', type: 'mqtt' });
```

Test:
```bash
node sdks/ts/test.mjs
```

---

## 3. Polyglot Integration Test Suite

All five client SDK implementations are continuously verified in CI and hermetic local test runs via:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests/test_polyglot_sdks.py
```
