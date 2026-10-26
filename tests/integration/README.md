# Integration tier (opt-in)

These tests talk to **real** services and are skipped unless enabled, so the default
`unittest discover tests/` stays hermetic. This directory deliberately has no `__init__.py`,
so default discovery does not descend into it.

## Redis (Control Plane / Data Plane convergence)

```bash
docker run -d --rm --name postoffice-it-redis -p 127.0.0.1:16379:6379 redis:7.2-alpine
POSTOFFICE_INTEGRATION=1 POSTOFFICE_TEST_REDIS=127.0.0.1:16379 \
  PYTHONPATH=src .venv/bin/python -m unittest discover tests/integration -v
docker stop postoffice-it-redis
```

**The tests run `FLUSHALL` and change the pubsub output-buffer limit on the target instance.**
Point `POSTOFFICE_TEST_REDIS` only at a throwaway instance.

What they prove that the in-memory fakes cannot: real subscribe/publish ordering across two
connections, `INCR` versioning, a server-side `CLIENT KILL`, and Redis disconnecting a stalled
subscriber at its output-buffer limit.
