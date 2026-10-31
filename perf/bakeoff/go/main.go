// Phase 0 Go Bake-Off Slice (`NATIVE_REWRITE_PLAN.md` §2 & §4 Phase 0).
//
// Implements the exact same pipeline slice as `perf/bakeoff/rust`:
// 1. MemoryGovernor: atomic byte-accounted budget (sheds at 80% threshold).
// 2. Firewall: 5-rule chain (MaxPayloadBytes, TopicShape, 3 AtomicTokenBuckets) + JSON ContentGuard.
// 3. TopicTrie: MQTT `+` and `#` wildcard matcher.
// 4. Egress confirm + POB1 CRC32 verification + HdrHistogram.
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const HeaderSize = 32

var Magic = []byte("POB1")

// --- 1. Memory Governor ---

type MemoryGovernor struct {
	BudgetBytes    uint64
	ShedLimitBytes uint64
	usedBytes      atomic.Uint64
	peakBytes      atomic.Uint64
	shedCount      atomic.Uint64
}

func NewMemoryGovernor(budgetBytes uint64) *MemoryGovernor {
	return &MemoryGovernor{
		BudgetBytes:    budgetBytes,
		ShedLimitBytes: (budgetBytes * 80) / 100,
	}
}

func (g *MemoryGovernor) TryAcquire(n uint64) bool {
	for {
		cur := g.usedBytes.Load()
		next := cur + n
		if next > g.ShedLimitBytes {
			g.shedCount.Add(1)
			return false
		}
		if g.usedBytes.CompareAndSwap(cur, next) {
			for {
				pk := g.peakBytes.Load()
				if next <= pk || g.peakBytes.CompareAndSwap(pk, next) {
					break
				}
			}
			return true
		}
	}
}

func (g *MemoryGovernor) Release(n uint64) {
	g.usedBytes.Add(^(n - 1))
}

// --- 2. POB1 Header & IEEE CRC32 ---

func crc32IEEE(data []byte) uint32 {
	var crc uint32 = 0xFFFFFFFF
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			mask := -(crc & 1)
			crc = (crc >> 1) ^ (0xEDB88320 & mask)
		}
	}
	return ^crc
}

func encodePOB1(producer uint32, seq uint64, sendTsNs uint64, body []byte) []byte {
	out := make([]byte, HeaderSize+len(body))
	copy(out[0:4], Magic)
	binary.BigEndian.PutUint32(out[4:8], producer)
	binary.BigEndian.PutUint64(out[8:16], seq)
	binary.BigEndian.PutUint64(out[16:24], sendTsNs)
	binary.BigEndian.PutUint32(out[24:28], crc32IEEE(body))
	binary.BigEndian.PutUint32(out[28:32], 0)
	copy(out[HeaderSize:], body)
	return out
}

func verifyPOB1(msg []byte) (uint32, uint64, []byte, bool) {
	if len(msg) < HeaderSize || !bytes.Equal(msg[0:4], Magic) {
		return 0, 0, nil, false
	}
	producer := binary.BigEndian.Uint32(msg[4:8])
	seq := binary.BigEndian.Uint64(msg[8:16])
	expectedCRC := binary.BigEndian.Uint32(msg[24:28])
	body := msg[HeaderSize:]
	if crc32IEEE(body) != expectedCRC {
		return 0, 0, nil, false
	}
	return producer, seq, body, true
}

// --- 3. Lock-free Atomic TokenBucket & 5-Rule Firewall + JSON ContentGuard ---

type AtomicTokenBucket struct {
	tokensMilli atomic.Uint64
}

func NewAtomicTokenBucket(initial uint64) *AtomicTokenBucket {
	b := &AtomicTokenBucket{}
	b.tokensMilli.Store(initial * 1000)
	return b
}

func (b *AtomicTokenBucket) TryConsume(cost uint64) bool {
	need := cost * 1000
	for {
		cur := b.tokensMilli.Load()
		if cur < need {
			return false
		}
		if b.tokensMilli.CompareAndSwap(cur, cur-need) {
			return true
		}
	}
}

var (
	keySensorID    = []byte(`"sensor_id"`)
	keyTemperature = []byte(`"temperature"`)
	keyStatus      = []byte(`"status"`)
)

func validateJSONContent(body []byte, maxDepth int) bool {
	depth := 0
	inStr := false
	esc := false
	for _, b := range body {
		if inStr {
			if esc {
				esc = false
			} else if b == '\\' {
				esc = true
			} else if b == '"' {
				inStr = false
			}
			continue
		}
		switch b {
		case '"':
			inStr = true
		case '{', '[':
			depth++
			if depth > maxDepth {
				return false
			}
		case '}', ']':
			if depth == 0 {
				return false
			}
			depth--
		}
	}
	if inStr || depth != 0 {
		return false
	}
	return bytes.Contains(body, keySensorID) &&
		bytes.Contains(body, keyTemperature) &&
		bytes.Contains(body, keyStatus)
}

type Firewall struct {
	MaxBytes       int
	MaxTopicLen    int
	MaxTopicDepth  int
	BrokerBucket   *AtomicTokenBucket
	TopicBucket    *AtomicTokenBucket
	IdentityBucket *AtomicTokenBucket
	ContentGuard   bool
}

func NewFirewall(contentGuard bool) *Firewall {
	const big = 1_000_000_000_000
	return &Firewall{
		MaxBytes:       1 << 20,
		MaxTopicLen:    256,
		MaxTopicDepth:  16,
		BrokerBucket:   NewAtomicTokenBucket(big),
		TopicBucket:    NewAtomicTokenBucket(big),
		IdentityBucket: NewAtomicTokenBucket(big),
		ContentGuard:   contentGuard,
	}
}

func (fw *Firewall) Evaluate(topic string, msg []byte, body []byte) bool {
	if len(msg) > fw.MaxBytes {
		return false
	}
	if len(topic) == 0 || len(topic) > fw.MaxTopicLen || strings.IndexByte(topic, 0) >= 0 {
		return false
	}
	if strings.HasPrefix(topic, "$SYS/") || topic == "$SYS" {
		return false
	}
	parts := strings.Split(topic, "/")
	if len(parts) > fw.MaxTopicDepth {
		return false
	}
	if len(parts) == 3 && parts[0] == "admin" && parts[2] == "secret" {
		return false
	}
	if !fw.BrokerBucket.TryConsume(1) || !fw.TopicBucket.TryConsume(1) || !fw.IdentityBucket.TryConsume(1) {
		return false
	}
	if fw.ContentGuard && !validateJSONContent(body, 32) {
		return false
	}
	return true
}

// --- 4. TopicTrie ---

type TrieNode struct {
	children map[string]*TrieNode
	routes   []string
}

func newTrieNode() *TrieNode {
	return &TrieNode{children: make(map[string]*TrieNode)}
}

type TopicTrie struct {
	root *TrieNode
}

func NewTopicTrie() *TopicTrie {
	return &TopicTrie{root: newTrieNode()}
}

func (t *TopicTrie) Insert(pattern, target string) {
	node := t.root
	for _, part := range strings.Split(pattern, "/") {
		child, ok := node.children[part]
		if !ok {
			child = newTrieNode()
			node.children[part] = child
		}
		node = child
		if part == "#" {
			break
		}
	}
	node.routes = append(node.routes, target)
}

func (t *TopicTrie) MatchCount(topic string) int {
	if strings.HasPrefix(topic, "$") {
		return 0
	}
	parts := strings.Split(topic, "/")
	return dfsMatch(t.root, parts, 0)
}

func dfsMatch(node *TrieNode, parts []string, idx int) int {
	count := 0
	if hashNode, ok := node.children["#"]; ok {
		count += len(hashNode.routes)
	}
	if idx == len(parts) {
		return count + len(node.routes)
	}
	if exact, ok := node.children[parts[idx]]; ok {
		count += dfsMatch(exact, parts, idx+1)
	}
	if plus, ok := node.children["+"]; ok {
		count += dfsMatch(plus, parts, idx+1)
	}
	return count
}

// --- 5. HdrHistogram ---

const (
	subBucketBits  = 10
	subBucketCount = 1 << subBucketBits
	subBucketMask  = subBucketCount - 1
	unitMagnitude  = 36
)

type HdrHistogram struct {
	buckets []uint64
	count   uint64
	maxNs   uint64
}

func NewHdrHistogram() *HdrHistogram {
	total := (unitMagnitude - subBucketBits + 2) * subBucketCount
	return &HdrHistogram{buckets: make([]uint64, total)}
}

func indexFor(val uint64) int {
	const capVal = (1 << unitMagnitude) - 1
	if val > capVal {
		val = capVal
	}
	if val < subBucketCount {
		return int(val)
	}
	msb := 63 - bits.LeadingZeros64(val)
	octave := msb - subBucketBits + 1
	sub := int((val >> (msb - subBucketBits)) & subBucketMask)
	return octave*subBucketCount + sub
}

func valueFromIndex(idx int) uint64 {
	octave := idx / subBucketCount
	sub := uint64(idx % subBucketCount)
	if octave == 0 {
		return sub
	}
	return ((1 << subBucketBits) | sub) << (octave - 1)
}

func (h *HdrHistogram) Record(val uint64) {
	idx := indexFor(val)
	h.buckets[idx]++
	h.count++
	if val > h.maxNs {
		h.maxNs = val
	}
}

func (h *HdrHistogram) Merge(other *HdrHistogram) {
	for i, c := range other.buckets {
		h.buckets[i] += c
	}
	h.count += other.count
	if other.maxNs > h.maxNs {
		h.maxNs = other.maxNs
	}
}

func (h *HdrHistogram) Percentile(p float64) uint64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(math.Ceil((p / 100.0) * float64(h.count)))
	if target < 1 {
		target = 1
	}
	var cum uint64
	for i, c := range h.buckets {
		cum += c
		if cum >= target {
			v := valueFromIndex(i)
			if v > h.maxNs {
				return h.maxNs
			}
			return v
		}
	}
	return h.maxNs
}

// --- 6. Benchmark Cells ---

func make1KBPayload(seq uint64) []byte {
	prefix := []byte(`{"sensor_id":"sensor-17","temperature":23.5,"status":"OK","pad":"`)
	targetBody := 1024 - HeaderSize
	body := make([]byte, 0, targetBody)
	body = append(body, prefix...)
	for len(body)+2 < targetBody {
		body = append(body, 'x')
	}
	body = append(body, '"', '}')
	return encodePOB1(1, seq, 0, body)
}

func runCell(messages uint64, threads int, fullPipeline bool) string {
	trie := NewTopicTrie()
	trie.Insert("devices/+/telemetry", "kafka/telemetry")
	fw := NewFirewall(fullPipeline)
	gov := NewMemoryGovernor(64 * 1024 * 1024)
	msg := make1KBPayload(0)

	perThread := messages / uint64(threads)
	if perThread < 1 {
		perThread = 1
	}
	totalMsgs := perThread * uint64(threads)

	// Warm-up
	for i := 0; i < 200; i++ {
		if _, _, body, ok := verifyPOB1(msg); ok {
			_ = fw.Evaluate("devices/d-17/telemetry", msg, body)
			_ = trie.MatchCount("devices/d-17/telemetry")
		}
	}

	var delivered atomic.Uint64
	var mu sync.Mutex
	hist := NewHdrHistogram()

	t0 := time.Now()
	var wg sync.WaitGroup
	wg.Add(threads)
	for t := 0; t < threads; t++ {
		go func() {
			defer wg.Done()
			localHist := NewHdrHistogram()
			var localOK uint64
			msgLen := uint64(len(msg))
			for i := uint64(0); i < perThread; i++ {
				callT0 := time.Now()
				if !gov.TryAcquire(msgLen) {
					continue
				}
				if _, _, body, ok := verifyPOB1(msg); ok {
					if (!fullPipeline || fw.Evaluate("devices/d-17/telemetry", msg, body)) &&
						trie.MatchCount("devices/d-17/telemetry") > 0 {
						localOK++
					}
				}
				gov.Release(msgLen)
				localHist.Record(uint64(time.Since(callT0).Nanoseconds()))
			}
			delivered.Add(localOK)
			mu.Lock()
			hist.Merge(localHist)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsedNs := uint64(time.Since(t0).Nanoseconds())
	if elapsedNs == 0 {
		elapsedNs = 1
	}
	deliv := delivered.Load()
	rate := uint64(math.Round(float64(totalMsgs) * 1e9 / float64(elapsedNs)))
	return fmt.Sprintf(
		`{"msg_per_s":%d,"p50_ns":%d,"p99_ns":%d,"p99_9_ns":%d,"max_ns":%d,"sent":%d,"delivered":%d,"oracle_passed":%v}`,
		rate,
		hist.Percentile(50.0),
		hist.Percentile(99.0),
		hist.Percentile(99.9),
		hist.maxNs,
		totalMsgs,
		deliv,
		deliv == totalMsgs,
	)
}

func runOverload10x(messages uint64) string {
	gov := NewMemoryGovernor(10 * 1024)
	msg := make1KBPayload(0)
	total := messages
	if total < 100 {
		total = 100
	}
	var held uint64
	msgLen := uint64(len(msg))
	for i := uint64(0); i < total; i++ {
		if gov.TryAcquire(msgLen) {
			held++
		}
	}
	peak := gov.peakBytes.Load()
	shed := gov.shedCount.Load()
	for i := uint64(0); i < held; i++ {
		gov.Release(msgLen)
	}
	balanced := total == held+shed
	passed := balanced && held == 8 && peak <= gov.ShedLimitBytes
	return fmt.Sprintf(
		`{"sent":%d,"delivered":%d,"shed":%d,"budget_bytes":%d,"shed_limit_bytes":%d,"peak_governor_bytes":%d,"balanced":%v,"passed":%v}`,
		total, held, shed, gov.BudgetBytes, gov.ShedLimitBytes, peak, balanced, passed,
	)
}

func maxRSSKB() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmHWM:") || strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					return v
				}
			}
		}
	}
	return 0
}

func main() {
	messages := flag.Uint64("messages", 20000, "number of messages per cell")
	flag.Parse()

	c1Pass := runCell(*messages, 1, false)
	c1Full := runCell(*messages, 1, true)
	c4Full := runCell(*messages, 4, true)
	c8Full := runCell(*messages, 8, true)
	overload := runOverload10x(*messages)
	rssKB := maxRSSKB()

	fmt.Printf(
		`{"language":"go","max_rss_kb":%d,"cells":{"threads_1_passthrough":%s,"threads_1_full":%s,"threads_4_full":%s,"threads_8_full":%s,"overload_10x":%s}}`+"\n",
		rssKB, c1Pass, c1Full, c4Full, c8Full, overload,
	)
}
