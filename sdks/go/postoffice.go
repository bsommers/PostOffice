package postoffice

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	ConfluentMagicByte = 0x00
	ConfluentHeaderLen = 5
	MinKeyBytes        = 16
)

// ============================================================================
// Wire Framing (Confluent 5-byte header)
// ============================================================================

// EncodeConfluent frames a payload with a 0x00 magic byte and big-endian schema ID.
func EncodeConfluent(schemaID uint32, payload []byte) []byte {
	buf := make([]byte, ConfluentHeaderLen+len(payload))
	buf[0] = ConfluentMagicByte
	binary.BigEndian.PutUint32(buf[1:ConfluentHeaderLen], schemaID)
	copy(buf[ConfluentHeaderLen:], payload)
	return buf
}

// DecodeConfluent extracts schema ID and payload from a Confluent-framed byte slice.
func DecodeConfluent(framed []byte) (uint32, []byte, error) {
	if len(framed) < ConfluentHeaderLen {
		return 0, nil, errors.New("framed message too short: less than 5 bytes")
	}
	if framed[0] != ConfluentMagicByte {
		return 0, nil, fmt.Errorf("invalid magic byte: expected 0x00, got 0x%02x", framed[0])
	}
	schemaID := binary.BigEndian.Uint32(framed[1:ConfluentHeaderLen])
	payload := framed[ConfluentHeaderLen:]
	return schemaID, payload, nil
}

// ============================================================================
// Topic Matching (MQTT wildcards +, #, and $SYS isolation)
// ============================================================================

// ValidatePattern checks MQTT wildcard rules:
// '#' must be the last topic segment and alone in its segment.
// '+' must occupy the entire segment.
func ValidatePattern(pattern string) error {
	segments := strings.Split(pattern, "/")
	for i, seg := range segments {
		if seg == "#" && i != len(segments)-1 {
			return fmt.Errorf("multi-level wildcard '#' must be the last topic segment: '%s'", pattern)
		}
		if strings.Contains(seg, "+") && seg != "+" {
			return fmt.Errorf("single-level wildcard '+' must occupy the entire segment: '%s'", pattern)
		}
		if strings.Contains(seg, "#") && seg != "#" {
			return fmt.Errorf("multi-level wildcard '#' must occupy the entire segment: '%s'", pattern)
		}
	}
	return nil
}

// MatchTopic evaluates whether a topic matches an MQTT pattern.
// Topics beginning with '$' are isolated from root wildcards '+' and '#' (§5.0).
func MatchTopic(pattern, topic string) bool {
	p := strings.Split(pattern, "/")
	t := strings.Split(topic, "/")

	if strings.HasPrefix(topic, "$") && len(p) > 0 && (p[0] == "+" || p[0] == "#") {
		return false
	}

	for i, seg := range p {
		if seg == "#" {
			return true
		}
		if i >= len(t) || (seg != "+" && seg != t[i]) {
			return false
		}
	}

	return len(p) == len(t)
}

// ============================================================================
// Control Plane HMAC-SHA256 Signing
// ============================================================================

type SignedEnvelope struct {
	Kid     string `json:"kid"`
	Payload string `json:"payload"`
	Sig     string `json:"sig"`
}

type SignedVersionEnvelope struct {
	Kid string `json:"kid"`
	Sig string `json:"sig"`
}

type HmacSigner struct {
	signingKid string
	keys       map[string][]byte
}

func NewHmacSigner(keyring string) (*HmacSigner, error) {
	trimmed := strings.TrimSpace(keyring)
	if trimmed == "" {
		return nil, errors.New("at least one signing key is required")
	}

	keys := make(map[string][]byte)
	var firstKid string

	for _, item := range strings.Split(trimmed, ",") {
		item = strings.TrimSpace(item)
		parts := strings.SplitN(item, ":", 2)
		if len(parts) != 2 {
			return nil, errors.New("POSTOFFICE_EVENT_KEYS entries must look like kid:secret")
		}
		kid := strings.TrimSpace(parts[0])
		secret := []byte(strings.TrimSpace(parts[1]))

		if kid == "" || strings.Contains(kid, ":") || strings.Contains(kid, ",") {
			return nil, fmt.Errorf("invalid key id '%s'", kid)
		}
		if len(secret) < MinKeyBytes {
			return nil, fmt.Errorf("key '%s' is shorter than %d bytes", kid, MinKeyBytes)
		}

		if firstKid == "" {
			firstKid = kid
		}
		keys[kid] = secret
	}

	if firstKid == "" {
		return nil, errors.New("at least one signing key is required")
	}

	return &HmacSigner{
		signingKid: firstKid,
		keys:       keys,
	}, nil
}

func (s *HmacSigner) Sign(msg []byte) (string, string) {
	key := s.keys[s.signingKid]
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	sig := hex.EncodeToString(mac.Sum(nil))
	return s.signingKid, sig
}

func (s *HmacSigner) Verify(kid, sig string, msg []byte) bool {
	key, ok := s.keys[kid]
	if !ok {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(sig))
}

func (s *HmacSigner) SignEvent(payload string) (string, error) {
	msg := append([]byte("event\x00"), []byte(payload)...)
	kid, sig := s.Sign(msg)
	env := SignedEnvelope{
		Kid:     kid,
		Payload: payload,
		Sig:     sig,
	}
	data, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *HmacSigner) VerifyEvent(envelopeJSON string) bool {
	var env SignedEnvelope
	if err := json.Unmarshal([]byte(envelopeJSON), &env); err != nil {
		return false
	}
	msg := append([]byte("event\x00"), []byte(env.Payload)...)
	return s.Verify(env.Kid, env.Sig, msg)
}

func (s *HmacSigner) SignState(hashName, field, payload string) (string, error) {
	var msg []byte
	msg = append(msg, []byte("state\x00")...)
	msg = append(msg, []byte(hashName)...)
	msg = append(msg, 0)
	msg = append(msg, []byte(field)...)
	msg = append(msg, 0)
	msg = append(msg, []byte(payload)...)

	kid, sig := s.Sign(msg)
	env := SignedEnvelope{
		Kid:     kid,
		Payload: payload,
		Sig:     sig,
	}
	data, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *HmacSigner) VerifyState(hashName, field, envelopeJSON string) bool {
	var env SignedEnvelope
	if err := json.Unmarshal([]byte(envelopeJSON), &env); err != nil {
		return false
	}
	var msg []byte
	msg = append(msg, []byte("state\x00")...)
	msg = append(msg, []byte(hashName)...)
	msg = append(msg, 0)
	msg = append(msg, []byte(field)...)
	msg = append(msg, 0)
	msg = append(msg, []byte(env.Payload)...)

	return s.Verify(env.Kid, env.Sig, msg)
}

func (s *HmacSigner) SignVersion(version int64) (string, error) {
	msg := []byte(fmt.Sprintf("version\x00%d", version))
	kid, sig := s.Sign(msg)
	env := SignedVersionEnvelope{
		Kid: kid,
		Sig: sig,
	}
	data, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *HmacSigner) VerifyVersion(version int64, entryJSON string) bool {
	var env SignedVersionEnvelope
	if err := json.Unmarshal([]byte(entryJSON), &env); err != nil {
		return false
	}
	msg := []byte(fmt.Sprintf("version\x00%d", version))
	return s.Verify(env.Kid, env.Sig, msg)
}

// ============================================================================
// Control Plane HTTP Client
// ============================================================================

type BrokerConfig struct {
	Name     string                 `json:"name"`
	Type     string                 `json:"type"`
	Config   map[string]interface{} `json:"config,omitempty"`
	Security map[string]interface{} `json:"security,omitempty"`
	Kwargs   map[string]interface{} `json:"kwargs,omitempty"`
}

type RouteSpec struct {
	SourceBroker        string                 `json:"source_broker"`
	SourceTopic         string                 `json:"source_topic"`
	TargetBroker        string                 `json:"target_broker"`
	TargetTopic         string                 `json:"target_topic"`
	Targets             []map[string]interface{} `json:"targets,omitempty"`
	SemanticTranslation bool                   `json:"semantic_translation,omitempty"`
	SchemaMapping       map[string]interface{} `json:"schema_mapping,omitempty"`
	TargetKwargs        map[string]interface{} `json:"target_kwargs,omitempty"`
}

type PublishRequest struct {
	Broker   string                 `json:"broker"`
	Topic    string                 `json:"topic"`
	Payload  string                 `json:"payload"`
	IsBase64 bool                   `json:"is_base64,omitempty"`
	SenderID string                 `json:"sender_id,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	SchemaID *uint32                `json:"schema_id,omitempty"`
}

type MutationAck struct {
	OK            bool   `json:"ok"`
	Version       int64  `json:"version"`
	SchemaVersion int    `json:"schema_version"`
	Message       string `json:"message"`
}

type PublishResponse struct {
	Accepted         bool   `json:"accepted"`
	FirewallVerdict  string `json:"firewall_verdict"`
	Rule             string `json:"rule"`
	Reason           string `json:"reason"`
	TargetsDelivered int    `json:"targets_delivered"`
	TargetsFailed    int    `json:"targets_failed"`
	DlqRecorded      bool   `json:"dlq_recorded"`
}

type HealthResponse struct {
	Status   string `json:"status"`
	UptimeMS int64  `json:"uptime_ms"`
	Version  string `json:"version"`
}

type BrokerStatus struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Connected    bool   `json:"connected"`
	CircuitState string `json:"circuit_state"`
}

type ReadyResponse struct {
	Ready          bool           `json:"ready"`
	AppliedVersion int64          `json:"applied_version"`
	SigningMode    string         `json:"signing_mode"`
	Brokers        []BrokerStatus `json:"brokers"`
}

type DlqStats struct {
	TrackedFingerprints int64 `json:"tracked_fingerprints"`
	TrackedSources      int64 `json:"tracked_sources"`
}

type StatsResponse struct {
	ActiveBrokers     int      `json:"active_brokers"`
	ActiveRoutes      int      `json:"active_routes"`
	AppliedVersion    int64    `json:"applied_version"`
	MemoryBudgetBytes uint64   `json:"memory_budget_bytes"`
	MemoryInUseBytes  uint64   `json:"memory_in_use_bytes"`
	MemoryShedTotal   uint64   `json:"memory_shed_total"`
	DlqStats          DlqStats `json:"dlq_stats"`
}

type ControlPlaneClient struct {
	baseURL     string
	bearerToken string
	httpClient  *http.Client
}

func NewControlPlaneClient(baseURL string, bearerToken string) *ControlPlaneClient {
	return &ControlPlaneClient{
		baseURL:     strings.TrimRight(baseURL, "/"),
		bearerToken: bearerToken,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *ControlPlaneClient) doRequest(ctx context.Context, method, path string, body interface{}, out interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(data)
	}

	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d %s: %s", resp.StatusCode, resp.Status, string(respBytes))
	}

	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *ControlPlaneClient) RegisterBroker(ctx context.Context, config BrokerConfig) (*MutationAck, error) {
	var ack MutationAck
	if err := c.doRequest(ctx, http.MethodPost, "/v1/admin/brokers", config, &ack); err != nil {
		return nil, err
	}
	return &ack, nil
}

func (c *ControlPlaneClient) AddRoute(ctx context.Context, route RouteSpec) (*MutationAck, error) {
	var ack MutationAck
	if err := c.doRequest(ctx, http.MethodPost, "/v1/admin/routes", route, &ack); err != nil {
		return nil, err
	}
	return &ack, nil
}

func (c *ControlPlaneClient) Publish(ctx context.Context, req PublishRequest) (*PublishResponse, error) {
	var pubResp PublishResponse
	if err := c.doRequest(ctx, http.MethodPost, "/v1/stream/publish", req, &pubResp); err != nil {
		return nil, err
	}
	return &pubResp, nil
}

func (c *ControlPlaneClient) GetHealth(ctx context.Context) (*HealthResponse, error) {
	var health HealthResponse
	if err := c.doRequest(ctx, http.MethodGet, "/v1/observe/healthz", nil, &health); err != nil {
		return nil, err
	}
	return &health, nil
}

func (c *ControlPlaneClient) GetReady(ctx context.Context) (*ReadyResponse, error) {
	var ready ReadyResponse
	if err := c.doRequest(ctx, http.MethodGet, "/v1/observe/readyz", nil, &ready); err != nil {
		return nil, err
	}
	return &ready, nil
}

func (c *ControlPlaneClient) GetStats(ctx context.Context) (*StatsResponse, error) {
	var stats StatsResponse
	if err := c.doRequest(ctx, http.MethodGet, "/v1/observe/stats", nil, &stats); err != nil {
		return nil, err
	}
	return &stats, nil
}
