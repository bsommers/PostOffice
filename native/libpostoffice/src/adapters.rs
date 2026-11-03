//! Native Protocol Adapter State Machines:
//! - `KafkaOffsetTracker`: Contiguous offset completion watermark tracker (K1–K5).
//! - `AmqpAdapterState`: Publisher confirm delivery tag & prefetch window tracker (A1–A6).
//! - `MqttAdapterState`: QoS 1/2 `PUBACK` packet identifier (`mid`) tracker (M1–M3).

use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Default, Debug)]
struct PartitionOffsets {
    next_offset: Option<i64>,
    completed: BTreeSet<i64>,
}

/// Tracks polled and completed offsets per `(topic, partition)` and advances the commit
/// watermark (`next_offset`) only across contiguous completions.
#[derive(Default, Debug)]
pub struct KafkaOffsetTracker {
    partitions: HashMap<(String, i32), PartitionOffsets>,
}

impl KafkaOffsetTracker {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }

    /// Record that `offset` was polled on `(topic, partition)`.
    pub fn poll_offset(&mut self, topic: &str, partition: i32, offset: i64) {
        let state = self
            .partitions
            .entry((topic.to_string(), partition))
            .or_default();
        if state.next_offset.is_none() {
            state.next_offset = Some(offset);
        }
    }

    /// Mark `offset` completed on `(topic, partition)`.
    /// Returns `Some(highest_contiguous_offset)` if the contiguous watermark advanced,
    /// or `None` if an earlier offset is still in-flight.
    pub fn ack_offset(&mut self, topic: &str, partition: i32, offset: i64) -> Option<i64> {
        let state = self
            .partitions
            .entry((topic.to_string(), partition))
            .or_default();
        if state.next_offset.is_none() {
            state.next_offset = Some(offset);
        }
        let next = state.next_offset.unwrap();
        if offset < next {
            return None;
        }
        state.completed.insert(offset);

        let mut cur = next;
        let mut last_contiguous: Option<i64> = None;
        while state.completed.remove(&cur) {
            last_contiguous = Some(cur);
            cur += 1;
        }
        state.next_offset = Some(cur);
        last_contiguous
    }

    /// Return the next uncommitted offset (`highest_contiguous + 1`, the Kafka commit watermark),
    /// or `-1` if the partition is not tracked.
    pub fn committed_watermark(&self, topic: &str, partition: i32) -> i64 {
        self.partitions
            .get(&(topic.to_string(), partition))
            .and_then(|s| s.next_offset)
            .unwrap_or(-1)
    }

    /// Clear tracked state for `(topic, partition)` on consumer rebalance or restart.
    pub fn reset_partition(&mut self, topic: &str, partition: i32) {
        self.partitions.remove(&(topic.to_string(), partition));
    }
}

/// AMQP publisher confirm & prefetch back-pressure tracker.
#[derive(Default, Debug)]
pub struct AmqpAdapterState {
    pub confirm_enabled: bool,
    pub prefetch_count: u16,
    pub inflight_deliveries: usize,
    pub declared_queues: HashSet<String>,
}

impl AmqpAdapterState {
    pub fn new(prefetch_count: u16) -> Self {
        Self {
            confirm_enabled: true,
            prefetch_count,
            inflight_deliveries: 0,
            declared_queues: HashSet::new(),
        }
    }

    pub fn should_declare_queue(&mut self, queue: &str) -> bool {
        self.declared_queues.insert(queue.to_string())
    }

    pub fn on_disconnect(&mut self) {
        self.declared_queues.clear();
        self.inflight_deliveries = 0;
    }
}

/// MQTT QoS 1/2 `PUBACK` packet identifier (`mid`) tracker.
#[derive(Default, Debug)]
pub struct MqttAdapterState {
    pub inflight_mids: HashSet<i32>,
}

impl MqttAdapterState {
    pub fn new() -> Self {
        Self {
            inflight_mids: HashSet::new(),
        }
    }

    pub fn track_publish(&mut self, mid: i32, qos: u8) -> bool {
        if qos == 0 {
            true
        } else {
            self.inflight_mids.insert(mid);
            false
        }
    }

    pub fn on_puback(&mut self, mid: i32) -> bool {
        self.inflight_mids.remove(&mid)
    }
}
