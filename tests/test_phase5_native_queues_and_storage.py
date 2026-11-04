import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from postoffice.base_client import (
    BaseClient,
    BufferExpiredError,
    BufferOverflowError,
)
from postoffice.native import (
    NativeDiskStoreAndForward,
    NativeTargetQueue,
)


class _DummyClient(BaseClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.published = []

    def connect(self):
        pass

    def disconnect(self):
        pass

    def subscribe(self, topic: str, **kwargs):
        pass

    def publish(self, topic: str, message: bytes, on_confirm=None, on_error=None, **kwargs):
        self.published.append((topic, message, kwargs))
        if on_confirm:
            on_confirm()


class TestPhase5NativeQueuesAndStorage(unittest.TestCase):
    def test_native_target_queue_bounded_messages_bytes_age_and_overflow_policies(self):
        # 1. "reject" overflow policy on message count and byte budget
        q_reject = NativeTargetQueue(
            max_messages=2,
            max_bytes=16,
            max_age_sec=10.0,
            overflow_policy="reject",
        )
        res1 = q_reject.enqueue("sensors/1", b"123456", now_sec=100.0)
        self.assertTrue(res1["accepted"])
        self.assertEqual(q_reject.stats()["queued_messages"], 1)
        self.assertEqual(q_reject.stats()["queued_bytes"], 6)

        # Oversize single message > max_bytes is rejected without evicting existing items
        res_oversize = q_reject.enqueue("sensors/big", b"x" * 20, now_sec=100.5)
        self.assertFalse(res_oversize["accepted"])
        self.assertEqual(res_oversize["reason"], "oversize")
        self.assertEqual(q_reject.stats()["queued_messages"], 1)

        # Second message fits
        res2 = q_reject.enqueue("sensors/2", b"abcdef", now_sec=101.0)
        self.assertTrue(res2["accepted"])
        self.assertEqual(q_reject.stats()["queued_messages"], 2)
        self.assertEqual(q_reject.stats()["queued_bytes"], 12)

        # Third message rejected by count limit (max_messages=2)
        res3 = q_reject.enqueue("sensors/3", b"zz", now_sec=102.0)
        self.assertFalse(res3["accepted"])
        self.assertEqual(res3["reason"], "count_full")

        # Advance clock past max_age_sec (10.0s) for first message (enqueued at 100.0), second at 101.0
        drained = q_reject.drain(limit=10, now_sec=110.5)
        self.assertEqual(len(drained["expired"]), 1)
        self.assertEqual(drained["expired"][0]["topic"], "sensors/1")
        self.assertEqual(len(drained["messages"]), 1)
        self.assertEqual(drained["messages"][0]["topic"], "sensors/2")
        self.assertEqual(drained["messages"][0]["payload"], b"abcdef")
        self.assertEqual(q_reject.stats()["queued_messages"], 0)
        self.assertEqual(q_reject.stats()["queued_bytes"], 0)

        # 2. "drop_oldest" overflow policy
        q_drop = NativeTargetQueue(
            max_messages=2,
            max_bytes=10,
            max_age_sec=60.0,
            overflow_policy="drop_oldest",
        )
        q_drop.enqueue("t/1", b"aaaa", now_sec=10.0)
        q_drop.enqueue("t/2", b"bbbb", now_sec=11.0)
        # Enqueue 6-byte message: requires dropping both t/1 (count_full) and t/2 (4+6 <= 10 fits, so only t/1 dropped!)
        res_d1 = q_drop.enqueue("t/3", b"cccccc", now_sec=12.0)
        self.assertTrue(res_d1["accepted"])
        self.assertEqual(len(res_d1["evicted"]), 1)
        self.assertEqual(res_d1["evicted"][0]["topic"], "t/1")

        # Enqueue 8-byte message: must drop both t/2 (4B) and t/3 (6B) to fit 8B within max_bytes=10
        res_d2 = q_drop.enqueue("t/4", b"dddddddd", now_sec=13.0)
        self.assertTrue(res_d2["accepted"])
        self.assertEqual([e["topic"] for e in res_d2["evicted"]], ["t/2", "t/3"])
        out = q_drop.drain(limit=10, now_sec=14.0)
        self.assertEqual(len(out["messages"]), 1)
        self.assertEqual(out["messages"][0]["topic"], "t/4")
        self.assertEqual(out["messages"][0]["payload"], b"dddddddd")

    def test_native_disk_store_and_forward_wear_aware_batching_size_cap_and_crash_recovery(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "edge_saf.bin"
            store = NativeDiskStoreAndForward(
                path=str(log_path),
                size_cap_bytes=512,
                batch_max_messages=3,
                batch_max_bytes=256,
                flush_interval_sec=5.0,
                overflow_policy="drop_oldest",
            )
            # First two messages stay in memory staging buffer (wear-aware batching: 0 disk flushes yet)
            r1 = store.append("edge/temp", b'{"t":21.5}', {"qos": 1}, now_sec=100.0)
            r2 = store.append("edge/temp", b'{"t":22.0}', {"qos": 1}, now_sec=101.0)
            self.assertTrue(r1["accepted"])
            self.assertTrue(r2["accepted"])
            st_before = store.stats()
            self.assertEqual(st_before["staged_messages"], 2)
            self.assertEqual(st_before["disk_flushes"], 0)

            # Third message hits batch_max_messages=3 and triggers wear-aware batch write to disk
            r3 = store.append("edge/temp", b'{"t":22.5}', {"qos": 1}, now_sec=102.0)
            self.assertTrue(r3["accepted"])
            st_after = store.stats()
            self.assertEqual(st_after["staged_messages"], 0)
            self.assertEqual(st_after["persisted_messages"], 3)
            self.assertEqual(st_after["disk_flushes"], 1)
            del store

            # Simulate torn trailing frame from power loss by appending 5 corrupt bytes to the file
            with open(log_path, "ab") as f:
                f.write(b"\x50\x4f\x53\x46\x01")

            # Re-open store: crash recovery must truncate torn trailing bytes and recover all 3 intact records
            recovered = NativeDiskStoreAndForward(
                path=str(log_path),
                size_cap_bytes=512,
                batch_max_messages=3,
                batch_max_bytes=256,
                flush_interval_sec=5.0,
                overflow_policy="drop_oldest",
            )
            self.assertEqual(recovered.stats()["persisted_messages"], 3)
            batch = recovered.drain(limit=10)
            self.assertEqual(len(batch), 3)
            self.assertEqual(batch[0]["topic"], "edge/temp")
            self.assertEqual(batch[0]["payload"], b'{"t":21.5}')
            self.assertEqual(batch[0]["kwargs"], {"qos": 1})
            self.assertEqual(batch[2]["payload"], b'{"t":22.5}')
            self.assertEqual(recovered.stats()["persisted_messages"], 0)

    def test_base_client_opt_in_disk_store_and_forward_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir) / "client_saf.log"
            router = MagicMock()
            c1 = _DummyClient(
                "edge_mqtt",
                router,
                enable_publish_buffer=True,
                max_publish_buffer_size=10,
                max_publish_buffer_bytes=4096,
                disk_store_path=str(store_path),
                disk_store_batch_messages=2,
            )
            c1.buffer_publish("factory/line1", b"payload-1", qos=1)
            c1.buffer_publish("factory/line1", b"payload-2", qos=1)
            self.assertEqual(c1.publish_buffer_size, 2)
            # Simulate process restart before flush_publish_buffer
            del c1

            c2 = _DummyClient(
                "edge_mqtt",
                router,
                enable_publish_buffer=True,
                max_publish_buffer_size=10,
                max_publish_buffer_bytes=4096,
                disk_store_path=str(store_path),
                disk_store_batch_messages=2,
            )
            self.assertEqual(c2.publish_buffer_size, 2)
            flushed = c2.flush_publish_buffer()
            self.assertEqual(flushed, 2)
            self.assertEqual(len(c2.published), 2)
            self.assertEqual(c2.published[0][0], "factory/line1")
            self.assertEqual(c2.published[0][1], b"payload-1")
            self.assertEqual(c2.published[1][1], b"payload-2")
            self.assertEqual(c2.publish_buffer_size, 0)


if __name__ == "__main__":
    unittest.main()
