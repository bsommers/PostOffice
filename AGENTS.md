# PostOffice — Agent Instructions & Workflow Contracts

## Project Overview

PostOffice is a multi-protocol messaging system and translation bridge routing data between MQTT, AMQP (RabbitMQ), Apache Kafka, and NanoMQ edge brokers. It provides a uniform programmatic interface (`PostOffice` facade in `src/postoffice/app.py`) for standalone applications and a distributed Control Plane / Data Plane architecture synchronized via Redis.

## Core Commands & Environment

- **Python Runtime:** Python 3.12+ (local environment configured with `uv` in `.venv/`)
- **Required Path:** Always set `PYTHONPATH=src` when running Python scripts or tests.
- **Run All Unit Tests:**
  ```bash
  PYTHONPATH=src .venv/bin/python -m unittest discover tests/
  ```
- **Run Single Test File:**
  ```bash
  PYTHONPATH=src .venv/bin/python -m unittest tests/test_postoffice.py
  PYTHONPATH=src .venv/bin/python -m unittest tests/test_wildcards.py
  ```
- **Local Broker Stack:**
  ```bash
  docker compose up -d
  docker compose down
  ```
- **Start Data Plane Worker:**
  ```bash
  PYTHONPATH=src .venv/bin/python src/postoffice/worker.py
  ```
- **Inject Control Plane Configuration:**
  ```bash
  PYTHONPATH=src .venv/bin/python scripts/admin.py
  ```

## Architecture & Code Organization

- **Facade:** `src/postoffice/app.py` (`PostOffice` facade)
- **Routing Engine:** `src/postoffice/router.py` (`Router` with MQTT topic wildcard matching: `+` single-level, `#` multi-level)
- **Plugin Registry:** `src/postoffice/registry.py` (`ClientRegistry` decorator and factory)
- **Base Interface:** `src/postoffice/base_client.py` (`BaseClient` abstract base class)
- **Plugins:** `src/postoffice/plugins/` (`mqtt_client.py`, `amqp_client.py`, `kafka_client.py`, `nanomq_client.py`)
- **Distributed Coordination:** `src/postoffice/control_plane.py` (Redis admin) and `src/postoffice/data_plane.py` (Worker listener)
- **Tests:** `tests/` (`test_postoffice.py`, `test_wildcards.py`) - all unit tests mock network clients hermetically

## GSD (Get Stuff Done) Workflow

Planning artifacts are tracked in `.planning/`:
- `.planning/PROJECT.md` — Project context, scope, constraints, and core value.
- `.planning/REQUIREMENTS.md` — Checkable requirement specifications.
- `.planning/ROADMAP.md` — Sequential phase definitions and goals.
- `.planning/STATE.md` — Current milestone, phase progress, and accumulated decisions.
- `.planning/codebase/` — Comprehensive 7-document codebase map (`STACK.md`, `ARCHITECTURE.md`, `STRUCTURE.md`, `CONVENTIONS.md`, `TESTING.md`, `INTEGRATIONS.md`, `CONCERNS.md`).

Workflows:
- Execute planned work using phase planning and task execution workflows.
- For ad-hoc fixes, use quick task or systematic debugging routines.
- Maintain documentation integrity and keep `.planning/STATE.md` synchronized after finishing milestones.

## Superpowers Engineering Standards

1. **Process Skills First:**
   - **Brainstorming:** Before creating components, adding functionality, or making architectural decisions, explore requirements and alternatives.
   - **Systematic Debugging:** For any unexpected failure or bug, investigate root causes before proposing patches.
2. **Test-Driven Development (TDD):**
   - Write failing hermetic unit tests before implementing changes (Red-Green-Refactor).
   - Ensure external network services remain mocked in unit tests.
3. **Verification Before Completion:**
   - Always run the test suite (`PYTHONPATH=src .venv/bin/python -m unittest discover tests/`) and confirm output before claiming work is complete.
4. **Git Hygiene & Clean Commits:**
   - Atomic, focused commits. Never commit secrets, temporary files, or unverified broken builds.
