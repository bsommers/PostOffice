import crypto from 'node:crypto';

export const CONFLUENT_MAGIC_BYTE = 0x00;
export const CONFLUENT_HEADER_LEN = 5;
export const MIN_KEY_BYTES = 16;

// ============================================================================
// Wire Framing (Confluent 5-byte header)
// ============================================================================

/**
 * Encodes payload into Confluent 5-byte wire frame:
 * 0x00 magic byte + 4-byte BE schemaId + payload bytes.
 */
export function encodeConfluent(schemaId, payload) {
  const payBuf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const buf = Buffer.alloc(CONFLUENT_HEADER_LEN + payBuf.length);
  buf[0] = CONFLUENT_MAGIC_BYTE;
  buf.writeUInt32BE(schemaId, 1);
  payBuf.copy(buf, CONFLUENT_HEADER_LEN);
  return buf;
}

/**
 * Decodes Confluent 5-byte wire frame into schemaId and payload buffer.
 * Throws on malformed frame (< 5 bytes or non-zero magic byte).
 */
export function decodeConfluent(framed) {
  const buf = Buffer.isBuffer(framed) ? framed : Buffer.from(framed);
  if (buf.length < CONFLUENT_HEADER_LEN) {
    throw new Error('Framed message too short: less than 5 bytes');
  }
  if (buf[0] !== CONFLUENT_MAGIC_BYTE) {
    throw new Error(`Invalid magic byte: expected 0x00, got 0x${buf[0].toString(16).padStart(2, '0')}`);
  }
  const schemaId = buf.readUInt32BE(1);
  const payload = buf.subarray(CONFLUENT_HEADER_LEN);
  return { schemaId, payload };
}

// ============================================================================
// Topic Matching (MQTT wildcards +, #, and $SYS isolation)
// ============================================================================

/**
 * Validates topic pattern according to MQTT rules:
 * '#' must be the last topic segment and stand alone.
 * '+' must stand alone in its segment.
 */
export function validatePattern(pattern) {
  const segments = pattern.split('/');
  for (let i = 0; i < segments.length; i++) {
    const seg = segments[i];
    if (seg === '#' && i !== segments.length - 1) {
      throw new Error(`Multi-level wildcard '#' must be the last topic segment: '${pattern}'`);
    }
    if (seg.includes('+') && seg !== '+') {
      throw new Error(`Single-level wildcard '+' must occupy the entire segment: '${pattern}'`);
    }
    if (seg.includes('#') && seg !== '#') {
      throw new Error(`Multi-level wildcard '#' must occupy the entire segment: '${pattern}'`);
    }
  }
  return segments;
}

/**
 * Matches an MQTT topic against a pattern.
 * Topic starting with '$' is isolated from root wildcards '+' and '#' (§5.0).
 */
export function matchTopic(pattern, topic) {
  const p = pattern.split('/');
  const t = topic.split('/');

  if (topic.startsWith('$') && p.length > 0 && (p[0] === '+' || p[0] === '#')) {
    return false;
  }

  for (let i = 0; i < p.length; i++) {
    const seg = p[i];
    if (seg === '#') {
      return true;
    }
    if (i >= t.length || (seg !== '+' && seg !== t[i])) {
      return false;
    }
  }

  return p.length === t.length;
}

// ============================================================================
// Control Plane HMAC-SHA256 Signer
// ============================================================================

export class HmacSigner {
  constructor(keyring) {
    if (!keyring || typeof keyring !== 'string' || !keyring.trim()) {
      throw new Error('At least one signing key is required');
    }
    this.keys = new Map();
    this.signingKid = null;

    const items = keyring.trim().split(',');
    for (const item of items) {
      const idx = item.indexOf(':');
      if (idx === -1) {
        throw new Error('POSTOFFICE_EVENT_KEYS entries must look like kid:secret');
      }
      const kid = item.slice(0, idx).trim();
      const secret = Buffer.from(item.slice(idx + 1).trim(), 'utf8');

      if (!kid || kid.includes(':') || kid.includes(',')) {
        throw new Error(`Invalid key id '${kid}'`);
      }
      if (secret.length < MIN_KEY_BYTES) {
        throw new Error(`Key '${kid}' is shorter than ${MIN_KEY_BYTES} bytes`);
      }

      if (!this.signingKid) {
        this.signingKid = kid;
      }
      this.keys.set(kid, secret);
    }

    if (!this.signingKid) {
      throw new Error('At least one signing key is required');
    }
  }

  sign(message) {
    const secret = this.keys.get(this.signingKid);
    const msgBuf = Buffer.isBuffer(message) ? message : Buffer.from(message, 'utf8');
    const hmac = crypto.createHmac('sha256', secret);
    hmac.update(msgBuf);
    const sig = hmac.digest('hex');
    return { kid: this.signingKid, sig };
  }

  verify(kid, sig, message) {
    const secret = this.keys.get(kid);
    if (!secret || typeof sig !== 'string' || sig.length !== 64) {
      return false;
    }
    const msgBuf = Buffer.isBuffer(message) ? message : Buffer.from(message, 'utf8');
    const hmac = crypto.createHmac('sha256', secret);
    hmac.update(msgBuf);
    const expected = hmac.digest('hex');

    try {
      return crypto.timingSafeEqual(Buffer.from(expected, 'hex'), Buffer.from(sig, 'hex'));
    } catch {
      return false;
    }
  }

  signEvent(payload) {
    const msg = Buffer.concat([Buffer.from('event\0', 'utf8'), Buffer.from(payload, 'utf8')]);
    const { kid, sig } = this.sign(msg);
    return JSON.stringify({ kid, sig, payload });
  }

  verifyEvent(envelope) {
    const env = typeof envelope === 'string' ? JSON.parse(envelope) : envelope;
    if (!env || !env.kid || !env.sig || typeof env.payload !== 'string') {
      return false;
    }
    const msg = Buffer.concat([Buffer.from('event\0', 'utf8'), Buffer.from(env.payload, 'utf8')]);
    return this.verify(env.kid, env.sig, msg);
  }

  signState(hashName, field, payload) {
    const msg = Buffer.concat([
      Buffer.from('state\0', 'utf8'),
      Buffer.from(hashName, 'utf8'),
      Buffer.from([0]),
      Buffer.from(field, 'utf8'),
      Buffer.from([0]),
      Buffer.from(payload, 'utf8'),
    ]);
    const { kid, sig } = this.sign(msg);
    return JSON.stringify({ kid, sig, payload });
  }

  verifyState(hashName, field, envelope) {
    const env = typeof envelope === 'string' ? JSON.parse(envelope) : envelope;
    if (!env || !env.kid || !env.sig || typeof env.payload !== 'string') {
      return false;
    }
    const msg = Buffer.concat([
      Buffer.from('state\0', 'utf8'),
      Buffer.from(hashName, 'utf8'),
      Buffer.from([0]),
      Buffer.from(field, 'utf8'),
      Buffer.from([0]),
      Buffer.from(env.payload, 'utf8'),
    ]);
    return this.verify(env.kid, env.sig, msg);
  }

  signVersion(version) {
    const msg = Buffer.from(`version\0${version}`, 'utf8');
    const { kid, sig } = this.sign(msg);
    return JSON.stringify({ kid, sig });
  }

  verifyVersion(version, entry) {
    const env = typeof entry === 'string' ? JSON.parse(entry) : entry;
    if (!env || !env.kid || !env.sig) {
      return false;
    }
    const msg = Buffer.from(`version\0${version}`, 'utf8');
    return this.verify(env.kid, env.sig, msg);
  }
}

// ============================================================================
// Control Plane HTTP Client
// ============================================================================

export class ControlPlaneClient {
  constructor(baseUrl, options = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.token = options.token || '';
    this.fetchFn = options.fetch || globalThis.fetch;
  }

  async _request(method, path, body = null) {
    const headers = { 'Content-Type': 'application/json' };
    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }
    const init = {
      method,
      headers,
    };
    if (body !== null) {
      init.body = JSON.stringify(body);
    }
    const res = await this.fetchFn(`${this.baseUrl}${path}`, init);
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`HTTP ${res.status} ${res.statusText}: ${errText}`);
    }
    return res.json();
  }

  async registerBroker(brokerConfig) {
    return this._request('POST', '/v1/admin/brokers', brokerConfig);
  }

  async addRoute(routeSpec) {
    return this._request('POST', '/v1/admin/routes', routeSpec);
  }

  async publish(publishRequest) {
    return this._request('POST', '/v1/stream/publish', publishRequest);
  }

  async getHealth() {
    return this._request('GET', '/v1/observe/healthz');
  }

  async getReady() {
    return this._request('GET', '/v1/observe/readyz');
  }

  async getStats() {
    return this._request('GET', '/v1/observe/stats');
  }
}
