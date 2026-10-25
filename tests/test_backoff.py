import random
import unittest

from postoffice.backoff import ExponentialBackoff


class TestExponentialBackoff(unittest.TestCase):
    def test_delays_grow_exponentially_without_jitter(self):
        b = ExponentialBackoff(base=1.0, factor=2.0, cap=100.0, jitter="none")
        self.assertEqual([b.next() for _ in range(4)], [1.0, 2.0, 4.0, 8.0])

    def test_delay_never_exceeds_cap(self):
        b = ExponentialBackoff(base=1.0, factor=2.0, cap=5.0, jitter="none")
        self.assertEqual([b.next() for _ in range(5)], [1.0, 2.0, 4.0, 5.0, 5.0])

    def test_full_jitter_within_zero_and_ceiling(self):
        b = ExponentialBackoff(base=1.0, factor=2.0, cap=30.0, jitter="full", rng=random.Random(7))
        for attempt in range(10):
            ceiling = min(30.0, 1.0 * 2.0 ** attempt)
            delay = b.next()
            self.assertGreaterEqual(delay, 0.0)
            self.assertLessEqual(delay, ceiling)

    def test_full_jitter_is_deterministic_with_seeded_rng(self):
        a = ExponentialBackoff(base=1.0, rng=random.Random(1))
        b = ExponentialBackoff(base=1.0, rng=random.Random(1))
        self.assertEqual([a.next() for _ in range(5)], [b.next() for _ in range(5)])

    def test_reset_restarts_sequence(self):
        b = ExponentialBackoff(base=1.0, jitter="none")
        b.next(); b.next()
        b.reset()
        self.assertEqual(b.next(), 1.0)

    def test_large_attempt_counts_do_not_overflow(self):
        b = ExponentialBackoff(base=1.0, cap=30.0, jitter="none")
        for _ in range(5000):
            delay = b.next()
        self.assertEqual(delay, 30.0)

    def test_invalid_arguments_raise_value_error(self):
        with self.assertRaises(ValueError):
            ExponentialBackoff(base=0)
        with self.assertRaises(ValueError):
            ExponentialBackoff(base=1.0, factor=0.5)
        with self.assertRaises(ValueError):
            ExponentialBackoff(base=1.0, cap=0.5)
        with self.assertRaises(ValueError):
            ExponentialBackoff(base=1.0, jitter="sideways")


if __name__ == "__main__":
    unittest.main()
