package main

import "testing"

func TestMemoryGovernorShedsAt80Percent(t *testing.T) {
	gov := NewMemoryGovernor(10_000)
	if gov.ShedLimitBytes != 8_000 {
		t.Fatalf("expected 8000, got %d", gov.ShedLimitBytes)
	}
	for i := 0; i < 8; i++ {
		if !gov.TryAcquire(1_000) {
			t.Fatalf("expected acquire %d to succeed", i)
		}
	}
	if gov.TryAcquire(1_000) {
		t.Fatal("expected 9th acquire to shed")
	}
	if gov.shedCount.Load() != 1 {
		t.Fatalf("expected shedCount=1, got %d", gov.shedCount.Load())
	}
	gov.Release(1_000)
	if !gov.TryAcquire(1_000) {
		t.Fatal("expected acquire after release to succeed")
	}
}

func TestFirewallAndTrieAndPOB1(t *testing.T) {
	msg := make1KBPayload(42)
	prod, seq, body, ok := verifyPOB1(msg)
	if !ok || prod != 1 || seq != 42 {
		t.Fatalf("unexpected POB1 decode: ok=%v prod=%d seq=%d", ok, prod, seq)
	}
	fw := NewFirewall(true)
	if !fw.Evaluate("devices/d-17/telemetry", msg, body) {
		t.Fatal("expected valid telemetry to pass firewall")
	}
	if fw.Evaluate("$SYS/broker/load", msg, body) {
		t.Fatal("expected $SYS topic to be denied")
	}
	if fw.Evaluate("admin/x/secret", msg, body) {
		t.Fatal("expected admin/+/secret topic to be denied")
	}
	trie := NewTopicTrie()
	trie.Insert("devices/+/telemetry", "kafka/telemetry")
	if trie.MatchCount("devices/d-17/telemetry") != 1 {
		t.Fatal("expected trie match")
	}
	if trie.MatchCount("$SYS/d-17/telemetry") != 0 {
		t.Fatal("expected $ topic not to match")
	}
}
