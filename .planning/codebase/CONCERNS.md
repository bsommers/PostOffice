# Codebase Concerns

**Analysis Date:** 2026-10-05

## Tech Debt & Architecture Gaps

**1. Semantic Translation & Guarantee Mappings:**
- Issue: As detailed in `docs/ARCHITECTURE.md`, translating delivery semantics (e.g., MQTT QoS 1 to AMQP persistent delivery_mode=2 and Kafka acks=all) requires waiting for broker publisher confirms before sending protocol ACKs. Currently, `Router.route()` is synchronous and fire-and-forget, with kwargs passed directly to `publish()` without an end-to-end acknowledgement pipeline.
- Impact: Messages could theoretically be acknowledged to the ingress client before being confirmed by the egress broker, risking data loss if a crash occurs mid-flight.
- Fix approach: Implement a stateful delivery coordinator or pipeline supporting async callback confirmations (MQTT PUBACK / AMQP ack / Kafka produce callback).

**2. Asymmetric Subscription Queuing:**
- Issue: `MqttClient` queues subscriptions if called before the client is connected, automatically subscribing once connected (`_on_connect`). However, `AmqpClient` and `KafkaClient` behave differently: `AmqpClient` logs an error and drops the subscription if the channel is not yet open, while `KafkaClient` queues pending subscriptions behind a lock.
- Impact: Broker initialization timing can cause dropped subscriptions in AMQP if subscriptions are added before the background connection completes.
- Fix approach: Standardize connection-state queuing across all `BaseClient` subclasses.

**3. Linear Topic Search in Router:**
- Issue: `Router.route()` iterates through all registered routes: `for (r_client, r_topic), routes in self.routes.items(): if r_client == source_client and self._topic_match(r_topic, source_topic): ...`
- Impact: O(N) complexity per message. While completely adequate for tens or hundreds of routes, this will become a CPU bottleneck with thousands of concurrent routing rules.
- Fix approach: Implement a Radix / Trie tree for topic hierarchy matching.

## Known Bugs & Edge Cases

**1. `clear_state` in Data Plane Worker:**
- Symptoms: When `ControlPlane.clear_state()` is executed, `DataPlane` logs `Control plane issued clear_state. A restart is recommended` but does not reset its internal `PostOffice` instance, broker connections, or route tables.
- Impact: Workers continue routing old configurations until manually terminated and restarted.
- Fix approach: Implement a proper reset handler in `DataPlane` that tears down existing routes and brokers cleanly.

**2. Redis Reconnection Handling:**
- Symptoms: If the Redis instance restarts, `DataPlane._listen_for_updates()` pubsub loop can raise a connection error and exit the listener thread.
- Impact: The worker process keeps running but no longer receives dynamic route updates.
- Fix approach: Wrap `_listen_for_updates` in a reconnect backoff loop.

## Security Considerations

**1. Default Unauthenticated Development Settings:**
- Risk: Docker compose and client defaults use default unauthenticated/empty passwords (`user`/`password` for RabbitMQ, anonymous access for Mosquitto).
- Mitigation: Currently designed for local development.
- Recommendation: Ensure production deployment guides enforce TLS and authentication credentials for all broker connectors.

## Testing Gaps

**1. Control Plane & Data Plane Automated Testing:**
- Gap: Current test suite covers `PostOffice` and `Router` topic wildcards, but does not test `ControlPlane`, `DataPlane`, or `worker.py`.
- Recommendation: Add unit tests utilizing `fakeredis` or mock Redis instances to test state synchronization and live config broadcast parsing.

---

*Concerns analysis: 2026-10-05*
