# Testing Patterns

**Analysis Date:** 2026-10-05

## Test Framework

**Runner:**
- Standard Python `unittest` test runner
- Hermetic unit tests using `unittest.mock` (`MagicMock`, `patch`)

**Assertion Library:**
- Built-in `unittest.TestCase` assertions:
  - `assertEqual(a, b)`
  - `assertTrue(expr)` / `assertFalse(expr)`
  - `assertIn(item, container)`
  - `mock.assert_called_once_with(...)`

**Run Commands:**
```bash
# Run all tests using local venv:
PYTHONPATH=src .venv/bin/python -m unittest discover tests/

# Run all tests using uv:
PYTHONPATH=src uv run --python .venv python -m unittest discover tests/

# Run a single test file:
PYTHONPATH=src .venv/bin/python -m unittest tests/test_wildcards.py
PYTHONPATH=src .venv/bin/python -m unittest tests/test_postoffice.py

# Run with verbose output:
PYTHONPATH=src .venv/bin/python -m unittest discover -v tests/
```

## Test File Organization

**Location:**
- Dedicated `tests/` directory at the repository root.

**Naming:**
- `test_<subsystem>.py` for all test modules.
- Test classes inherit from `unittest.TestCase` (`TestPostOfficeScaffold`, `TestWildcardRouting`).
- Test methods prefixed with `test_` (`test_routing_initialization`, `test_topic_match`).

**Structure:**
```
tests/
├── test_postoffice.py     # Integration & unit test of PostOffice facade and mock routing
└── test_wildcards.py      # Unit tests for Router wildcard matching (+ and #)
```

## Mocking & Hermetic Testing Strategy

**External Broker Isolation:**
- External network broker libraries (`paho.mqtt.client`, `pika`, `confluent_kafka`) are patched via `@patch`:
  ```python
  @patch('postoffice.plugins.mqtt_client.mqtt.Client')
  @patch('postoffice.plugins.amqp_client.pika.BlockingConnection')
  @patch('postoffice.plugins.kafka_client.Consumer')
  @patch('postoffice.plugins.kafka_client.Producer')
  def test_routing_initialization(self, MockProducer, MockConsumer, MockPika, MockMqtt):
      ...
  ```
- Concrete client `publish` methods are intercepted with `MagicMock()` to verify payload forwarding and keyword argument translation across protocols.

## Test Coverage & Future Testing Needs

**Current Test Coverage:**
- `test_routing_initialization`: Verifies broker registration in facade, route registration in router, and end-to-end dispatch from MQTT -> Kafka -> AMQP.
- `test_topic_match`: Tests exact matches, single-level (`+`), multi-level (`#`), and invalid topic matching.

**Recommended Test Additions:**
- ControlPlane & DataPlane unit tests with a mock/fake Redis client (e.g. `fakeredis`).
- Error handling tests: broker connection failures, invalid configuration exceptions, unroutable topic warnings.
- End-to-end integration tests using docker compose stack (`docker compose up -d`).

---

*Testing analysis: 2026-10-05*
