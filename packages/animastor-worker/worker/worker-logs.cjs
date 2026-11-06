// ======================================================
// GPU Worker — fresh logs (ring buffer, redaction, /v3/logs shipping)
// (bundle 2.4.1, ORCHESTRATION_V2 §8.5)
// ======================================================
// What lives here (all pure, zero-dependency — the worker bundle stays
// self-contained; zlib is a Node builtin):
//
//   createLogBuffer()  — a bounded in-memory ring: the last MAX_LINES lines
//                        and at most MAX_BYTES bytes (whichever is smaller).
//                        Everything the worker logger writes, plus (when the
//                        worker itself started ComfyUI) its stderr lines.
//   redact(text)       — the mandatory secret scrub applied BEFORE shipping.
//   createLogShipper() — incremental upload: only lines newer than the last
//                        acknowledged send, gzip JSON, ≤ SHIP_MAX_BYTES after
//                        compression, POST /v3/logs with the worker Bearer.
//                        Errors/timeouts are silent; lines stay in the buffer.
//
// The shipper never throws and never blocks a caller: shipOnce() is an async
// fire-and-forget the task loop does not await.

'use strict';

const zlib = require('zlib');

// Ring caps: 2000 lines OR 256 KiB, whichever is hit first.
const MAX_LINES = 2000;
const MAX_BYTES = 256 * 1024;
// One POST body may not exceed 64 KiB AFTER gzip; the rest goes next time.
const SHIP_MAX_BYTES = 64 * 1024;
const SHIP_INTERVAL_MS = 30000;
const SHIP_TIMEOUT_MS = 5000;

// ======================================================
// REDACTION (applied before shipping, never after)
// ======================================================
// Rules (one test per rule, tests/worker-logs.test.cjs):
//   1. Authorization / Bearer values;
//   2. upload_token values;
//   3. keys shaped [A-Za-z0-9_-]{32,} right after key|token|secret|password[=:];
//   4. the whole .env content (NAME=VALUE lines) — value → [redacted];
//   5. any explicitly registered secret value (the worker's own credential).
const REDACTED = '[redacted]';

function redact(text, secrets = []) {
  let out = String(text == null ? '' : text);
  // 5. Known concrete secret values (defense in depth: the raw credential
  // must never leave the host even if it reaches a log line verbatim).
  for (const s of Array.isArray(secrets) ? secrets : []) {
    const v = s == null ? '' : String(s);
    if (v.length >= 8) out = out.split(v).join(REDACTED);
  }
  // 1a. `Bearer <token>` anywhere.
  out = out.replace(/(Bearer\s+)[A-Za-z0-9._~+/=-]{6,}/gi, `$1${REDACTED}`);
  // 1b. `Authorization: <value>` (header dump form). Skip a value that is
  // already the `Bearer <redacted>` form handled above.
  out = out.replace(/(authorization\s*[:=]\s*)("?)(?!Bearer\b)([^\s,;"'}]+)/gi, `$1$2${REDACTED}`);
  // 2. upload_token in JSON (`"upload_token":"..."`) or KV form.
  out = out.replace(/(upload_token"?\s*[:=]\s*["']?)([^"'\s,}]+)/gi, `$1${REDACTED}`);
  // 3. key/token/secret/password = <32+ [A-Za-z0-9_-]>.
  out = out.replace(/((?:key|token|secret|password)"?\s*[:=]\s*["']?)([A-Za-z0-9_-]{32,})(["']?)/gi, `$1${REDACTED}$3`);
  // 4. .env content: env-style assignments — redact the value, keep the name.
  out = out.replace(/^([ \t]*"?[A-Z][A-Z0-9_]{2,}\s*=\s*)(\S.*)$/gm, `$1${REDACTED}`);
  return out;
}

// ======================================================
// RING BUFFER
// ======================================================

function utf8Len(s) {
  return Buffer.byteLength(String(s), 'utf8');
}

/**
 * Bounded ring of log entries. Drop-oldest on either cap. Sequence numbers
 * are monotonic for the process lifetime so the shipper can say "only lines
 * newer than N" and ack them by seq_to.
 */
function createLogBuffer({ maxLines = MAX_LINES, maxBytes = MAX_BYTES } = {}) {
  /** @type {{seq:number, ts:string, level:string, msg:string, bytes:number}[]} */
  let entries = [];
  let totalBytes = 0;
  let nextSeq = 1;

  function append(level, msg, ts) {
    const entry = {
      seq: nextSeq++,
      ts: ts || new Date().toISOString(),
      level: String(level == null ? 'info' : level),
      msg: String(msg == null ? '' : msg),
    };
    entry.bytes = utf8Len(entry.ts) + utf8Len(entry.level) + utf8Len(entry.msg) + 8;
    entries.push(entry);
    totalBytes += entry.bytes;
    let evicted = 0;
    while (entries.length > maxLines || (totalBytes > maxBytes && entries.length > 1)) {
      const dropped = entries.shift();
      totalBytes -= dropped.bytes;
      evicted += 1;
    }
    return { seq: entry.seq, evicted };
  }

  /** Lines with seq > afterSeq, oldest first. */
  function since(afterSeq) {
    const after = Number(afterSeq) || 0;
    return entries.filter((e) => e.seq > after).map((e) => ({ ...e }));
  }

  function all() {
    return entries.map((e) => ({ ...e }));
  }

  function count() {
    return entries.length;
  }

  function sizeBytes() {
    return totalBytes;
  }

  function lastSeq() {
    return nextSeq - 1;
  }

  /** Oldest seq still present (or the next seq when empty). */
  function earliestSeq() {
    return entries.length ? entries[0].seq : nextSeq;
  }

  function caps() {
    return { maxLines, maxBytes };
  }

  function clear() {
    entries = [];
    totalBytes = 0;
  }

  return { append, since, all, count, sizeBytes, lastSeq, earliestSeq, caps, clear };
}

// ======================================================
// CHILD STDERR CAPTURE (ComfyUI, when the worker started it itself)
// ======================================================
// Reads a readable stream line by line and appends each line to the buffer.
// Used for the stdout/stderr of a ComfyUI process THIS worker spawned;
// in the shipped deployments ComfyUI is started by the installer, so this
// path is inactive unless the worker is configured to start it (COMFY_SPAWN_CMD).
function captureStream(stream, buffer, { level = 'comfy', onLine } = {}) {
  if (!stream || typeof stream.on !== 'function' || !buffer) return () => {};
  let rest = '';
  const flush = (line) => {
    if (line === '') return;
    buffer.append(level, line);
    if (typeof onLine === 'function') onLine(line);
  };
  const onData = (chunk) => {
    rest += chunk.toString('utf8');
    let idx;
    while ((idx = rest.indexOf('\n')) >= 0) {
      flush(rest.slice(0, idx).replace(/\r$/, ''));
      rest = rest.slice(idx + 1);
    }
    // Guard against a pathological line with no newline.
    if (rest.length > MAX_BYTES) { flush(rest); rest = ''; }
  };
  const onEnd = () => { if (rest.length) { flush(rest); rest = ''; } };
  stream.on('data', onData);
  stream.on('end', onEnd);
  stream.on('error', () => {});
  return () => {
    stream.removeListener('data', onData);
    stream.removeListener('end', onEnd);
  };
}

// ======================================================
// SHIPPER — incremental POST /v3/logs
// ======================================================

/**
 * @param {object} o
 * @param {ReturnType<createLogBuffer>} o.buffer
 * @param {string} o.hubUrl            hub base (.../gpu)
 * @param {string} o.token             worker credential (Bearer)
 * @param {string} o.workerId          label; the hub derives identity from the token
 * @param {number} [o.intervalMs]
 * @param {number} [o.maxBytes]        per-POST gzip ceiling (64 KiB)
 * @param {number} [o.timeoutMs]
 * @param {Function} [o.fetchImpl]     (url, options) => Response (tests)
 * @param {string[]} [o.secrets]       concrete values to scrub
 * @param {Function} [o.onWarn]        (msg) — throttled diagnostics (default silent)
 */
function createLogShipper({
  buffer,
  hubUrl,
  token,
  workerId,
  intervalMs = SHIP_INTERVAL_MS,
  maxBytes = SHIP_MAX_BYTES,
  timeoutMs = SHIP_TIMEOUT_MS,
  fetchImpl,
  secrets = [],
  onWarn = null,
} = {}) {
  if (!buffer) throw new Error('createLogShipper: buffer is required');
  const doFetch = fetchImpl || ((url, options) => fetch(url, options));

  let ackedSeq = 0;      // last seq the hub accepted
  let inFlight = null;   // avoid overlapping POSTs
  let stopped = false;
  let timer = null;

  function payloadLines(entries) {
    return entries.map((e) => ({ ts: e.ts, level: e.level, msg: redact(e.msg, secrets) }));
  }

  function encode(entries) {
    const body = {
      worker_id: workerId,
      seq_from: entries[0].seq,
      seq_to: entries[entries.length - 1].seq,
      lines: payloadLines(entries),
    };
    return zlib.gzipSync(Buffer.from(JSON.stringify(body), 'utf8'));
  }

  /**
   * Pick the largest prefix of `pending` whose gzip stays ≤ maxBytes.
   * Returns { entries, body } or null when there is nothing to send.
   */
  function buildBatch(pending) {
    if (!pending.length) return null;
    // First bound by raw JSON size (gzip of normal text is smaller than the
    // raw input, so this almost always already fits).
    const chosen = [];
    let raw = 64; // envelope
    for (const e of pending) {
      const approx = utf8Len(e.msg) + utf8Len(e.ts) + utf8Len(e.level) + 48;
      if (chosen.length && raw + approx > maxBytes) break;
      chosen.push(e);
      raw += approx;
    }
    let body = encode(chosen);
    while (body.length > maxBytes && chosen.length > 1) {
      chosen.pop();
      body = encode(chosen);
    }
    if (body.length > maxBytes) {
      // A single line bigger than the whole budget: truncate its message so
      // the pipeline keeps moving (the tail is dropped, not stuck forever).
      const only = { ...chosen[0] };
      let budget = Math.max(256, Math.floor(maxBytes / 2));
      while (budget > 64) {
        only.msg = String(only.msg).slice(0, budget);
        body = encode([only]);
        if (body.length <= maxBytes) break;
        budget = Math.floor(budget / 2);
      }
      chosen[0] = only;
    }
    return { entries: chosen, body };
  }

  async function post(body) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await doFetch(`${hubUrl}/v3/logs`, {
        method: 'POST',
        headers: {
          'Authorization': token ? `Bearer ${token}` : undefined,
          'Content-Type': 'application/json',
          'Content-Encoding': 'gzip',
        },
        body,
        signal: controller.signal,
      });
      return !!(res && res.ok);
    } catch (_) {
      // Network error / timeout — silent by contract; lines stay in the buffer.
      return false;
    } finally {
      clearTimeout(id);
    }
  }

  /** Send at most one 64 KiB batch of new lines. Never throws. */
  async function shipOnce({ force = false } = {}) {
    if (stopped && !force) return { ok: false, sent: 0, skipped: 'stopped' };
    if (inFlight) return inFlight;
    const pending = buffer.since(ackedSeq);
    const batch = buildBatch(pending);
    if (!batch) return { ok: true, sent: 0, empty: true };
    inFlight = (async () => {
      const ok = await post(batch.body);
      if (ok) {
        ackedSeq = batch.entries[batch.entries.length - 1].seq;
        return { ok: true, sent: batch.entries.length };
      }
      if (typeof onWarn === 'function') onWarn(`log ship failed (${batch.entries.length} line(s) kept)`);
      return { ok: false, sent: 0 };
    })();
    try { return await inFlight; } finally { inFlight = null; }
  }

  /**
   * One-shot full-buffer flush (send_log_tail): resend from the oldest line
   * still present, in 64 KiB chunks, until nothing is pending or a POST fails.
   */
  async function shipAll() {
    ackedSeq = buffer.earliestSeq() - 1;
    let sent = 0;
    for (let i = 0; i < 100; i++) {
      const r = await shipOnce({ force: true });
      if (r.empty || !r.ok) break;
      sent += r.sent;
    }
    return { ok: true, sent };
  }

  function lastAckedSeq() {
    return ackedSeq;
  }

  /** Start the 30 s tick. The callback is fire-and-forget (never awaited). */
  function start() {
    if (timer || stopped) return;
    timer = setInterval(() => { shipOnce().catch(() => {}); }, intervalMs);
    if (typeof timer.unref === 'function') timer.unref();
  }

  function stop() {
    stopped = true;
    if (timer) { clearInterval(timer); timer = null; }
  }

  return { shipOnce, shipAll, start, stop, lastAckedSeq };
}

module.exports = {
  MAX_LINES,
  MAX_BYTES,
  SHIP_MAX_BYTES,
  SHIP_INTERVAL_MS,
  SHIP_TIMEOUT_MS,
  REDACTED,
  redact,
  createLogBuffer,
  captureStream,
  createLogShipper,
};
