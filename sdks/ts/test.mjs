import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  encodeConfluent,
  decodeConfluent,
  validatePattern,
  matchTopic,
  HmacSigner,
  ControlPlaneClient,
} from './postoffice.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const VECTORS_DIR = path.resolve(__dirname, '../../conformance/vectors');

function loadVector(filename) {
  const file = path.join(VECTORS_DIR, filename);
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

async function testWireFraming() {
  const vec = loadVector('wire_framing.json');

  // Decode framed hex
  const rawFramed = Buffer.from(vec.framed_json_hex, 'hex');
  const { schemaId, payload } = decodeConfluent(rawFramed);
  assert.equal(schemaId, 1);

  // Re-encode and verify identical hex
  const reEncoded = encodeConfluent(schemaId, payload);
  assert.equal(reEncoded.toString('hex'), vec.framed_json_hex);

  // Error cases
  for (const ec of vec.error_cases) {
    const raw = Buffer.from(ec.raw_hex, 'hex');
    if (ec.raises_error) {
      assert.throws(() => decodeConfluent(raw), /Framed message too short|Invalid magic byte/);
    }
  }
}

async function testTopicMatching() {
  const vec = loadVector('topic_matching.json');

  // Invalid patterns
  for (const ip of vec.invalid_patterns) {
    if (!ip.valid) {
      assert.throws(() => validatePattern(ip.pattern));
    } else {
      assert.doesNotThrow(() => validatePattern(ip.pattern));
    }
  }

  // Pair cases
  for (const pc of vec.pair_cases) {
    const res = matchTopic(pc.pattern, pc.topic);
    assert.equal(res, pc.matches, `pattern: ${pc.pattern}, topic: ${pc.topic}`);
  }
}

async function testControlPlaneSigning() {
  const vec = loadVector('control_plane_signing.json');
  const signer = new HmacSigner(vec.keyring);

  // 1. Event envelope
  const eventEnv = vec.signed_event_envelope;
  assert.equal(signer.verifyEvent(eventEnv), true);

  const mintedEvent = JSON.parse(signer.signEvent(vec.event_body));
  assert.equal(mintedEvent.kid, eventEnv.kid);
  assert.equal(mintedEvent.sig, eventEnv.sig);

  // 2. State envelope
  const stateEnv = vec.signed_state_envelope;
  assert.equal(signer.verifyState('postoffice:routes', 'route_1', stateEnv), true);

  const mintedState = JSON.parse(signer.signState('postoffice:routes', 'route_1', vec.event_body));
  assert.equal(mintedState.kid, stateEnv.kid);
  assert.equal(mintedState.sig, stateEnv.sig);

  // 3. Version envelope
  const versionEnv = vec.signed_version_envelope;
  assert.equal(signer.verifyVersion(7, versionEnv), true);

  const mintedVersion = JSON.parse(signer.signVersion(7));
  assert.equal(mintedVersion.kid, versionEnv.kid);
  assert.equal(mintedVersion.sig, versionEnv.sig);

  // 4. Cross-domain rejection
  const crossRejected = !signer.verifyState('postoffice:routes', 'route_1', eventEnv);
  assert.equal(crossRejected, vec.cross_domain_event_as_state_rejected);
}

async function testAdminApi() {
  const vec = loadVector('admin_api.json');
  const respMap = new Map();
  for (const entry of vec.transcript) {
    respMap.set(entry.op, entry.resp);
  }

  const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    if (req.method === 'POST' && req.url === '/v1/admin/brokers') {
      res.end(JSON.stringify(respMap.get('RegisterBroker:mqtt_in')));
    } else if (req.method === 'POST' && req.url === '/v1/admin/routes') {
      res.end(JSON.stringify(respMap.get('AddRoute')));
    } else if (req.method === 'POST' && req.url === '/v1/stream/publish') {
      res.end(JSON.stringify(respMap.get('Publish')));
    } else if (req.method === 'GET' && req.url === '/v1/observe/healthz') {
      res.end(JSON.stringify(respMap.get('GetHealth')));
    } else if (req.method === 'GET' && req.url === '/v1/observe/readyz') {
      res.end(JSON.stringify(respMap.get('GetReady')));
    } else if (req.method === 'GET' && req.url === '/v1/observe/stats') {
      res.end(JSON.stringify(respMap.get('GetStats')));
    } else {
      res.statusCode = 404;
      res.end(JSON.stringify({ error: 'not found' }));
    }
  });

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  const client = new ControlPlaneClient(`http://127.0.0.1:${port}`, { token: 'test-token' });

  try {
    // 1. RegisterBroker
    const ack1 = await client.registerBroker({ name: 'mqtt_in', type: 'mqtt' });
    assert.equal(ack1.ok, true);
    assert.equal(ack1.version, 1);

    // 2. AddRoute
    const ack2 = await client.addRoute({
      source_broker: 'mqtt_in',
      source_topic: 'sensors/+',
      target_broker: 'amqp_out',
      target_topic: 'ingest',
    });
    assert.equal(ack2.ok, true);
    assert.equal(ack2.version, 4);

    // 3. Publish
    const pubResp = await client.publish({
      broker: 'mqtt_in',
      topic: 'sensors/temp',
      payload: '{"temp": 25.0}',
    });
    assert.equal(pubResp.accepted, true);
    assert.equal(pubResp.firewall_verdict, 'allow');
    assert.equal(pubResp.targets_delivered, 1);

    // 4. GetHealth
    const health = await client.getHealth();
    assert.equal(health.status, 'ok');
    assert.equal(health.version, '1.0.0');

    // 5. GetReady
    const ready = await client.getReady();
    assert.equal(ready.ready, true);
    assert.equal(ready.applied_version, 5);
    assert.equal(ready.brokers.length, 2);

    // 6. GetStats
    const stats = await client.getStats();
    assert.equal(stats.active_brokers, 2);
    assert.equal(stats.active_routes, 1);
    assert.equal(stats.applied_version, 5);
  } finally {
    server.close();
  }
}

async function main() {
  await testWireFraming();
  await testTopicMatching();
  await testControlPlaneSigning();
  await testAdminApi();
  console.log('TS_SDK_OK');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
