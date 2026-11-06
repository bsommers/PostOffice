package postoffice

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadVector(t *testing.T, filename string, v interface{}) {
	path := filepath.Join("..", "..", "conformance", "vectors", filename)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed reading vector %s: %v", filename, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("failed unmarshaling %s: %v", filename, err)
	}
}

// ============================================================================
// Wire Framing Vector Verification
// ============================================================================

type WireFramingVector struct {
	FramedJSONHex string `json:"framed_json_hex"`
	ErrorCases    []struct {
		Name        string `json:"name"`
		RaisesError bool   `json:"raises_error"`
		RawHex      string `json:"raw_hex"`
	} `json:"error_cases"`
}

func TestWireFramingConformance(t *testing.T) {
	var vec WireFramingVector
	loadVector(t, "wire_framing.json", &vec)

	rawFramed, err := hex.DecodeString(vec.FramedJSONHex)
	if err != nil {
		t.Fatalf("failed decoding hex: %v", err)
	}

	schemaID, payload, err := DecodeConfluent(rawFramed)
	if err != nil {
		t.Fatalf("failed decoding confluent framed data: %v", err)
	}
	if schemaID != 1 {
		t.Errorf("expected schema_id 1, got %d", schemaID)
	}

	// Re-encode and verify identical hex
	reEncoded := EncodeConfluent(schemaID, payload)
	if hex.EncodeToString(reEncoded) != vec.FramedJSONHex {
		t.Errorf("re-encoded hex mismatch: expected %s, got %s", vec.FramedJSONHex, hex.EncodeToString(reEncoded))
	}

	// Test error cases
	for _, ec := range vec.ErrorCases {
		t.Run(ec.Name, func(t *testing.T) {
			raw, _ := hex.DecodeString(ec.RawHex)
			_, _, err := DecodeConfluent(raw)
			if ec.RaisesError && err == nil {
				t.Errorf("expected error for case %s, got nil", ec.Name)
			}
		})
	}
}

// ============================================================================
// Topic Matching Vector Verification
// ============================================================================

type TopicMatchingVector struct {
	InvalidPatterns []struct {
		Pattern string `json:"pattern"`
		Valid   bool   `json:"valid"`
	} `json:"invalid_patterns"`
	PairCases []struct {
		Pattern string `json:"pattern"`
		Topic   string `json:"topic"`
		Matches bool   `json:"matches"`
	} `json:"pair_cases"`
}

func TestTopicMatchingConformance(t *testing.T) {
	var vec TopicMatchingVector
	loadVector(t, "topic_matching.json", &vec)

	for _, ip := range vec.InvalidPatterns {
		t.Run("invalid_"+ip.Pattern, func(t *testing.T) {
			err := ValidatePattern(ip.Pattern)
			if ip.Valid && err != nil {
				t.Errorf("pattern '%s' expected valid, got error: %v", ip.Pattern, err)
			}
			if !ip.Valid && err == nil {
				t.Errorf("pattern '%s' expected invalid, got nil error", ip.Pattern)
			}
		})
	}

	for _, pc := range vec.PairCases {
		t.Run(pc.Pattern+"__"+pc.Topic, func(t *testing.T) {
			res := MatchTopic(pc.Pattern, pc.Topic)
			if res != pc.Matches {
				t.Errorf("pattern '%s' vs topic '%s': expected %v, got %v", pc.Pattern, pc.Topic, pc.Matches, res)
			}
		})
	}
}

// ============================================================================
// Control Plane Signing Vector Verification
// ============================================================================

type ControlPlaneSigningVector struct {
	Keyring                          string         `json:"keyring"`
	EventBody                        string         `json:"event_body"`
	SignedEventEnvelope              SignedEnvelope `json:"signed_event_envelope"`
	SignedStateEnvelope              SignedEnvelope `json:"signed_state_envelope"`
	SignedVersionEnvelope            SignedVersionEnvelope `json:"signed_version_envelope"`
	CrossDomainEventAsStateRejected  bool           `json:"cross_domain_event_as_state_rejected"`
}

func TestControlPlaneSigningConformance(t *testing.T) {
	var vec ControlPlaneSigningVector
	loadVector(t, "control_plane_signing.json", &vec)

	signer, err := NewHmacSigner(vec.Keyring)
	if err != nil {
		t.Fatalf("failed initializing HmacSigner: %v", err)
	}

	// 1. Sign event and verify envelope signature
	eventJSON, err := signer.SignEvent(vec.EventBody)
	if err != nil {
		t.Fatalf("failed signing event: %v", err)
	}
	var signedEv SignedEnvelope
	if err := json.Unmarshal([]byte(eventJSON), &signedEv); err != nil {
		t.Fatalf("failed unmarshaling event JSON: %v", err)
	}
	if signedEv.Kid != vec.SignedEventEnvelope.Kid {
		t.Errorf("event kid mismatch: expected %s, got %s", vec.SignedEventEnvelope.Kid, signedEv.Kid)
	}
	if signedEv.Sig != vec.SignedEventEnvelope.Sig {
		t.Errorf("event sig mismatch: expected %s, got %s", vec.SignedEventEnvelope.Sig, signedEv.Sig)
	}
	if !signer.VerifyEvent(eventJSON) {
		t.Errorf("expected signed event to verify successfully")
	}

	// 2. Sign state and verify envelope signature
	stateJSON, err := signer.SignState("postoffice:routes", "route_1", vec.EventBody)
	if err != nil {
		t.Fatalf("failed signing state: %v", err)
	}
	var signedSt SignedEnvelope
	if err := json.Unmarshal([]byte(stateJSON), &signedSt); err != nil {
		t.Fatalf("failed unmarshaling state JSON: %v", err)
	}
	if signedSt.Kid != vec.SignedStateEnvelope.Kid {
		t.Errorf("state kid mismatch: expected %s, got %s", vec.SignedStateEnvelope.Kid, signedSt.Kid)
	}
	if signedSt.Sig != vec.SignedStateEnvelope.Sig {
		t.Errorf("state sig mismatch: expected %s, got %s", vec.SignedStateEnvelope.Sig, signedSt.Sig)
	}
	if !signer.VerifyState("postoffice:routes", "route_1", stateJSON) {
		t.Errorf("expected signed state to verify successfully")
	}

	// 3. Sign version 7 and verify envelope signature
	versionJSON, err := signer.SignVersion(7)
	if err != nil {
		t.Fatalf("failed signing version: %v", err)
	}
	var signedVer SignedVersionEnvelope
	if err := json.Unmarshal([]byte(versionJSON), &signedVer); err != nil {
		t.Fatalf("failed unmarshaling version JSON: %v", err)
	}
	if signedVer.Kid != vec.SignedVersionEnvelope.Kid {
		t.Errorf("version kid mismatch: expected %s, got %s", vec.SignedVersionEnvelope.Kid, signedVer.Kid)
	}
	if signedVer.Sig != vec.SignedVersionEnvelope.Sig {
		t.Errorf("version sig mismatch: expected %s, got %s", vec.SignedVersionEnvelope.Sig, signedVer.Sig)
	}
	if !signer.VerifyVersion(7, versionJSON) {
		t.Errorf("expected signed version to verify successfully")
	}

	// 4. Cross-domain rejection test: verifying an event envelope as state MUST fail
	rejected := !signer.VerifyState("postoffice:routes", "route_1", eventJSON)
	if rejected != vec.CrossDomainEventAsStateRejected {
		t.Errorf("expected cross-domain rejection to be %v, got %v", vec.CrossDomainEventAsStateRejected, rejected)
	}
}

// ============================================================================
// Admin API Transcript Verification (via httptest.Server)
// ============================================================================

type AdminAPIVector struct {
	Transcript []struct {
		Op   string          `json:"op"`
		Resp json.RawMessage `json:"resp"`
	} `json:"transcript"`
}

func TestAdminAPIConformance(t *testing.T) {
	var vec AdminAPIVector
	loadVector(t, "admin_api.json", &vec)

	respMap := make(map[string]json.RawMessage)
	for _, entry := range vec.Transcript {
		respMap[entry.Op] = entry.Resp
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/admin/brokers":
			w.Write(respMap["RegisterBroker:mqtt_in"])
		case r.Method == http.MethodPost && r.URL.Path == "/v1/admin/routes":
			w.Write(respMap["AddRoute"])
		case r.Method == http.MethodPost && r.URL.Path == "/v1/stream/publish":
			w.Write(respMap["Publish"])
		case r.Method == http.MethodGet && r.URL.Path == "/v1/observe/healthz":
			w.Write(respMap["GetHealth"])
		case r.Method == http.MethodGet && r.URL.Path == "/v1/observe/readyz":
			w.Write(respMap["GetReady"])
		case r.Method == http.MethodGet && r.URL.Path == "/v1/observe/stats":
			w.Write(respMap["GetStats"])
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewControlPlaneClient(server.URL, "test-token")
	ctx := context.Background()

	// 1. RegisterBroker
	ack, err := client.RegisterBroker(ctx, BrokerConfig{Name: "mqtt_in", Type: "mqtt"})
	if err != nil {
		t.Fatalf("RegisterBroker failed: %v", err)
	}
	if !ack.OK || ack.Version != 1 || !strings.Contains(ack.Message, "mqtt_in") {
		t.Errorf("unexpected RegisterBroker response: %+v", ack)
	}

	// 2. AddRoute
	ack, err = client.AddRoute(ctx, RouteSpec{
		SourceBroker: "mqtt_in",
		SourceTopic:  "sensors/+",
		TargetBroker: "amqp_out",
		TargetTopic:  "ingest",
	})
	if err != nil {
		t.Fatalf("AddRoute failed: %v", err)
	}
	if !ack.OK || ack.Version != 4 {
		t.Errorf("unexpected AddRoute response: %+v", ack)
	}

	// 3. Publish
	pubResp, err := client.Publish(ctx, PublishRequest{
		Broker:  "mqtt_in",
		Topic:   "sensors/temp",
		Payload: "{\"temp\": 25.0}",
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if !pubResp.Accepted || pubResp.TargetsDelivered != 1 || pubResp.FirewallVerdict != "allow" {
		t.Errorf("unexpected Publish response: %+v", pubResp)
	}

	// 4. GetHealth
	health, err := client.GetHealth(ctx)
	if err != nil {
		t.Fatalf("GetHealth failed: %v", err)
	}
	if health.Status != "ok" || health.Version != "1.0.0" {
		t.Errorf("unexpected GetHealth response: %+v", health)
	}

	// 5. GetReady
	ready, err := client.GetReady(ctx)
	if err != nil {
		t.Fatalf("GetReady failed: %v", err)
	}
	if !ready.Ready || ready.AppliedVersion != 5 || len(ready.Brokers) != 2 {
		t.Errorf("unexpected GetReady response: %+v", ready)
	}

	// 6. GetStats
	stats, err := client.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.ActiveBrokers != 2 || stats.ActiveRoutes != 1 || stats.AppliedVersion != 5 {
		t.Errorf("unexpected GetStats response: %+v", stats)
	}
}
