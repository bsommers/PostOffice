import unittest
from postoffice.base_client import BaseClient
from postoffice.resiliency import CircuitBreaker, CircuitState, CircuitBreakerOpenError
from postoffice.router import Router
from postoffice.app import PostOffice


class FakeBrokerClient(BaseClient):
    def __init__(self, name="fake", router=None, **kwargs):
        super().__init__(name, router, **kwargs)
        self.connected = True
        self.fail_next_publishes = 0
        self.published = []

    def connect(self):
        self.connected = True
        self.flush_publish_buffer()

    def disconnect(self):
        self.connected = False

    def subscribe(self, topic: str, **kwargs):
        self.record_subscription(topic, **kwargs)

    def publish(self, topic: str, message: bytes, on_confirm=None, on_error=None, **kwargs):
        if self.fail_next_publishes > 0:
            self.fail_next_publishes -= 1
            exc = RuntimeError(f"{self.name}: Simulated target failure")
            if on_error:
                on_error(exc)
                return
            raise exc

        self.published.append((topic, message, kwargs))
        if on_confirm:
            on_confirm()


class TestCircuitBreakerUnit(unittest.TestCase):
    def test_state_transitions_closed_open_half_open_closed(self):
        now = [100.0]
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=10.0, time_fn=lambda: now[0])

        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertTrue(cb.allow_request())

        # 1st failure -> still CLOSED
        self.assertFalse(cb.record_failure())
        self.assertEqual(cb.state, CircuitState.CLOSED)

        # 2nd failure -> trips to OPEN
        self.assertTrue(cb.record_failure())
        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertFalse(cb.allow_request())

        # Advance clock by 5s (< 10s timeout) -> still OPEN
        now[0] += 5.0
        self.assertFalse(cb.allow_request())
        self.assertEqual(cb.state, CircuitState.OPEN)

        # Advance clock past 10s -> allow_request transitions to HALF_OPEN for probe
        now[0] += 6.0
        self.assertTrue(cb.allow_request())
        self.assertEqual(cb.state, CircuitState.HALF_OPEN)

        # While probe is in-flight, additional requests are blocked
        self.assertFalse(cb.allow_request())

        # If probe fails -> immediately back to OPEN
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)

        # Advance clock another 10s -> HALF_OPEN probe again
        now[0] += 10.0
        self.assertTrue(cb.allow_request())
        self.assertEqual(cb.state, CircuitState.HALF_OPEN)

        # Probe succeeds -> transitions to CLOSED (returns True)
        recovered = cb.record_success()
        self.assertTrue(recovered)
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertTrue(cb.allow_request())


class TestRouterCircuitBreakerBackpressure(unittest.TestCase):
    def test_circuit_breaker_buffers_when_open_and_flushes_on_recovery(self):
        now = [1000.0]
        router = Router()

        src = FakeBrokerClient("src", router)
        tgt = FakeBrokerClient(
            "tgt",
            router,
            enable_publish_buffer=True,
            max_publish_buffer_size=3,
            buffer_overflow_policy="reject",
        )
        dlq = FakeBrokerClient("dlq", router)

        router.register_client(src)
        router.register_client(tgt)
        router.register_client(dlq)
        router.set_dlq("dlq", "system/dlq")
        router.add_route("src", "events/+", "tgt", "downstream/events")

        cb = router.configure_circuit_breaker(
            "tgt", failure_threshold=2, recovery_timeout=15.0, time_fn=lambda: now[0]
        )

        # 1. Fail 2 messages -> trips circuit breaker to OPEN (failed messages go to DLQ)
        tgt.fail_next_publishes = 2
        router.route("src", "events/1", b"fail_1", ack_fn=lambda: None)
        router.route("src", "events/2", b"fail_2", ack_fn=lambda: None)

        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertEqual(len(dlq.published), 2)

        # 2. Send 3 messages while circuit is OPEN -> absorbed into tgt publish ring-buffer (NOT sent to DLQ!)
        acked_buffered = []
        router.route("src", "events/3", b"buf_1", ack_fn=lambda: acked_buffered.append(1))
        router.route("src", "events/4", b"buf_2", ack_fn=lambda: acked_buffered.append(2))
        router.route("src", "events/5", b"buf_3", ack_fn=lambda: acked_buffered.append(3))

        self.assertEqual(tgt.publish_buffer_size, 3)
        self.assertEqual(len(tgt.published), 0)
        self.assertEqual(len(dlq.published), 2)  # No new DLQ drops!
        self.assertEqual(acked_buffered, [])      # Upstream ACKs held until flush

        # 3. 4th message while OPEN overflows buffer (size=3) -> rejected to DLQ
        router.route("src", "events/6", b"overflow_msg", ack_fn=lambda: None)
        self.assertEqual(len(dlq.published), 3)

        # 4. Advance clock past recovery_timeout (15s) -> next message acts as HALF_OPEN probe
        now[0] += 16.0
        probe_acked = []
        router.route("src", "events/7", b"probe_msg", ack_fn=lambda: probe_acked.append(True))

        # Probe succeeded -> circuit CLOSED -> automatically flushed the 3 buffered messages!
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertEqual(tgt.publish_buffer_size, 0)
        self.assertEqual([p[1] for p in tgt.published], [b"probe_msg", b"buf_1", b"buf_2", b"buf_3"])
        self.assertEqual(probe_acked, [True])
        self.assertEqual(acked_buffered, [1, 2, 3])

    def test_circuit_breaker_open_without_publish_buffer_routes_directly_to_dlq(self):
        router = Router()
        src = FakeBrokerClient("src", router)
        # Buffering disabled (enable_publish_buffer=False)
        tgt = FakeBrokerClient("tgt", router, enable_publish_buffer=False)
        dlq = FakeBrokerClient("dlq", router)

        router.register_client(src)
        router.register_client(tgt)
        router.register_client(dlq)
        router.set_dlq("dlq", "system/dlq")
        router.add_route("src", "events/+", "tgt", "downstream/events")

        cb = router.configure_circuit_breaker("tgt", failure_threshold=1, recovery_timeout=60.0)

        # 1st message fails -> trips circuit to OPEN
        tgt.fail_next_publishes = 1
        router.route("src", "events/1", b"fail_1")
        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertEqual(cb.failure_count, 1)
        self.assertEqual(len(dlq.published), 1)

        # 2nd message arrives while OPEN (both with and without ack_fn) -> immediately routed to DLQ
        acked = []
        router.route("src", "events/2", b"blocked_1", ack_fn=lambda: acked.append(True))
        router.route("src", "events/3", b"blocked_2")
        self.assertEqual(len(dlq.published), 3)
        self.assertEqual(acked, [True])

        # Reset circuit breaker -> next message succeeds
        cb.reset()
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertEqual(cb.failure_count, 0)
        router.route("src", "events/4", b"ok_msg")
        self.assertEqual(len(tgt.published), 1)

    def test_circuit_breaker_validation_and_facade_wiring(self):
        with self.assertRaises(ValueError):
            CircuitBreaker(failure_threshold=0)
        with self.assertRaises(ValueError):
            CircuitBreaker(recovery_timeout=-1.0)

        po = PostOffice()
        with self.assertRaises(ValueError):
            po.configure_circuit_breaker("missing_broker")


if __name__ == "__main__":
    unittest.main()
