// ======================================================
// GPU Worker - v2.4.1 (protocol 3 + fresh worker logs: ring buffer, redaction, hub /v3/logs)
// ======================================================
// 2.4.1 (ORCHESTRATION_V2 §8.5): every line the worker logger writes (plus
// the stderr of a ComfyUI THIS worker started) also goes into a bounded
// in-memory ring (2000 lines / 256 KiB). Every 30 s only the new lines are
// POSTed gzip'd to the hub /v3/logs (Bearer, ≤ 64 KiB per body, secrets
// scrubbed BEFORE sending); errors are silent and the lines stay buffered.
// The task loop never waits for the sender. WORKER_LOG_SHIP=on|off (default
// on). `send_log_tail:true` in a /v3/beacon answer triggers a one-shot flush
// of the whole buffer in 64 KiB chunks. See worker-logs.cjs.
// 2.4.0 (ORCHESTRATION_V2 §8, stage 1c): every loop iteration asks the hub
// relay GET /v3/task/next first; on {task:null} / 404 / 503 (old hub, backend
// off) it makes ONE v2 /task/next pop exactly as 2.3.0. A protocol-3 task's
// result is streamed with PUT upload_url (Bearer upload_token, no base64), an
// execution error goes to error_url with the same token — both straight to the
// backend. POST /v3/beacon every 10 s next to the v2 beacon; a cancelled
// current job → ComfyUI /interrupt, nothing is uploaded. The worker owns no
// status-changing timeout (I3): task.timeout_ms only interrupts ComfyUI and is
// reported as error_class=timeout. WORKER_PROTOCOL=v2only|both (default both).
// CJS (CommonJS) — Node 20+ (global fetch). Zero runtime dependencies: the
// bundle runs as-is, there is nothing to npm-install.
// Job Protocol v2 comes from the GENERATED copy of the canonical
// @animastor/contracts package (Phase 9D, blocker B2 — option B):
//   packages/animastor-worker/worker/job-protocol-v2.cjs  (regenerate: node packages/animastor-worker/tools/sync-protocol.cjs)
// It is byte-parity guarded against the canonical source — never edit it.

const { execSync, spawn } = require("child_process");
const os = require("os");
const fs = require("fs");
const fsp = require("fs").promises;
const path = require("path");
const http = require("http");
const https = require("https");
const crypto = require("crypto");
const { cleanupJobArtifacts } = require("./worker-cleanup.cjs");
const journal = require("./worker-cleanup-journal.cjs");
const { loadDotEnv } = require("./worker-env.cjs");
const { PROTOCOL_VERSION, JOB_ID_SPLIT_RE } = require("./job-protocol-v2.cjs");
const { createLogBuffer, createLogShipper, captureStream } = require("./worker-logs.cjs");

// Bundle self-containment: load ./.env next to the worker (cp .env.example
// .env). Real environment variables always win over the file.
loadDotEnv(__dirname);

// ======================================================
// CONFIG
// ======================================================

const HUB_URL = process.env.HUB_URL || "https://animastor.org/gpu";
const COMFY_PORT = process.env.COMFY_PORT || 8188;
const WORKER_TYPE = process.env.WORKER_TYPE || "image";

// PW-4 (FAIL CLOSED): the worker credential (`wrk.<worker_id>.<secret>`,
// issued once at registration in Animastor) is REQUIRED. Every hub call
// carries `Authorization: Bearer <token>` and the hub derives identity,
// workspace and MODE from the registry — the worker never chooses its own
// mode. No credential → the worker refuses to start: a missing credential
// must never silently become a system/share worker.
const ANIMASTOR_WORKER_TOKEN = process.env.ANIMASTOR_WORKER_TOKEN || null;

// Backend API base for the startup credential verification. Derived from
// HUB_URL by default (…/gpu → …/api/v1); override with ANIMASTOR_API_URL.
const ANIMASTOR_API_URL = process.env.ANIMASTOR_API_URL
  || HUB_URL.replace(/\/gpu\/?$/, "") + "/api/v1";

const NOTEBOOK_PATH = process.env.NOTEBOOK_PATH || "";
// COMFY_URL — the full ComfyUI API base (scheme://host:port[/prefix]) as
// probed by the installer / start-worker.sh. When set it wins over
// COMFY_PORT + NOTEBOOK_PATH, so a reverse-proxy prefix is used only when the
// probe proved ComfyUI answers there (never guessed).
const COMFY_URL = (process.env.COMFY_URL || "").replace(/\/+$/, "");
const WORKER_ID = process.env.WORKER_ID || "gpu-" + os.hostname();
// Canonical bundle version lives in ./package.json (the hub publishes the
// same value with the worker-bundle artifact). WORKER_VERSION env overrides.
function readBundleVersion() {
  try { return require("./package.json").version || null; } catch (_) { return null; }
}
const WORKER_VERSION = process.env.WORKER_VERSION || readBundleVersion();
const WORKER_IMAGE_TAG = process.env.WORKER_IMAGE_TAG || null;
// PROTOCOL_VERSION comes from the generated copy of @animastor/contracts
// (top of file) — the frozen Job Protocol v2 value (2) lives there only.

const RESULT_TIMEOUT_MS = Number(process.env.RESULT_TIMEOUT_MS || 600000);
// Видео-генерация длинная по своей природе (LTX: 5-10 мин, на слабом GPU —
// 20-30+ мин). Дефолт для видео НЕ может быть 10 мин — иначе нормальная
// долгая генерация убивается как timeout. Приоритет: task.timeout_ms
// (приходит от backend через gpu-hub, layer-config per-type timeout);
// fallback для видео — 2 часа (реальный потолок — dispatch-lease backend'а).
const VIDEO_RESULT_TIMEOUT_MS = Number(process.env.VIDEO_RESULT_TIMEOUT_MS || 7200000);
const TASK_SLEEP_MS = Number(process.env.TASK_SLEEP_MS || 2000);
const BEACON_INTERVAL_MS = Number(process.env.BEACON_INTERVAL_MS || 10000);

// 2.4.0: protocol selection. both (default) = v3 relay first, then one v2
// pop per iteration; v2only = exactly the 2.3.0 loop (no /v3 calls at all).
const WORKER_PROTOCOL = (() => {
  const v = String(process.env.WORKER_PROTOCOL || "both").trim().toLowerCase();
  return v === "v2only" ? "v2only" : "both";
})();
const V3_PROTOCOL_VERSION = 3;
// PUT result / POST error to the backend: attempts on network error / 5xx.
const V3_DELIVERY_ATTEMPTS = Number(process.env.V3_DELIVERY_ATTEMPTS || 5);
const V3_DELIVERY_BACKOFF_MS = Number(process.env.V3_DELIVERY_BACKOFF_MS || 1000);
// Socket idle timeout of one upload attempt (not a job timeout).
const V3_UPLOAD_IDLE_MS = Number(process.env.V3_UPLOAD_IDLE_MS || 120000);

const COMFY_INPUT_DIR = process.env.COMFY_INPUT_DIR || "/home/jovyan/ComfyUI/input";
const COMFY_OUTPUT_DIR = path.resolve(COMFY_INPUT_DIR, "../output");

// 2.4.1 (ORCHESTRATION_V2 §8.5): fresh logs to the hub.
//   WORKER_LOG_SHIP=on|off   (default on) — the whole shipping feature.
//   LOG_SHIP_INTERVAL_MS=30000 — only NEW lines with each tick.
//   LOG_SHIP_MAX_BYTES, LOG_SHIP_TIMEOUT_MS — one body / one request.
const WORKER_LOG_SHIP = !/^(off|0|false|no)$/i.test(String(process.env.WORKER_LOG_SHIP || "on").trim());
const LOG_SHIP_INTERVAL_MS = Number(process.env.LOG_SHIP_INTERVAL_MS || 30000);
const LOG_SHIP_MAX_BYTES = Number(process.env.LOG_SHIP_MAX_BYTES || 64 * 1024);
const LOG_SHIP_TIMEOUT_MS = Number(process.env.LOG_SHIP_TIMEOUT_MS || 5000);
// Optional: this worker starts ComfyUI itself and ships its stderr. In the
// shipped deployments ComfyUI is started by the installer, so unset = OFF.
const COMFY_SPAWN_CMD = process.env.COMFY_SPAWN_CMD || null;

// The ring buffer holds every logger line (and ComfyUI stderr, when spawned
// here). It is process-local memory only — nothing is written to disk.
const logBuffer = createLogBuffer();
// Both the worker credential and any configured spawn command are scrubbed
// from every shipped line (belt-and-braces next to the pattern rules).
const LOG_SECRETS = [ANIMASTOR_WORKER_TOKEN].filter(Boolean);
// Created lazily in startLogShipping() so tests can import this file without
// touching the network.
let logShipper = null;

// ======================================================
// UTILS
// ======================================================

function log(level, msg, data) {
  const line = `[${new Date().toISOString()}] [${level}] ${msg}`;
  console.log(line);
  logBuffer.append(level, msg);
  if (data !== undefined) {
    const extra = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
    console.log(extra);
    logBuffer.append(level, extra);
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function fetchTimeout(url, options = {}, timeout = 30000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);

  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(id);
  }
}

function comfyUrl(p) {
  if (COMFY_URL) return `${COMFY_URL}${p}`;
  return `http://127.0.0.1:${COMFY_PORT}${NOTEBOOK_PATH}${p}`;
}

// PW-4: hub request headers — always Bearer-authenticated (the startup gate
// guarantees a credential is present; defense-in-depth keeps it unconditional).
function hubHeaders() {
  const headers = { "Content-Type": "application/json" };
  if (ANIMASTOR_WORKER_TOKEN) headers["Authorization"] = `Bearer ${ANIMASTOR_WORKER_TOKEN}`;
  return headers;
}

// FAIL CLOSED: an auth rejection from the hub/backend is terminal. Retrying
// an invalid credential forever would only hide the misconfiguration — the
// operator must fix ANIMASTOR_WORKER_TOKEN.
function authFailed(source, status) {
  log("error", "Worker authentication failed — check ANIMASTOR_WORKER_TOKEN");
  log("error", `${source} rejected the credential (HTTP ${status}). The token may be wrong, rotated or revoked.`);
  log("error", "Create/rotate a worker in Animastor (Settings → Workers) and set the new token.");
  process.exit(1);
}

// ======================================================
// STARTUP CREDENTIAL VERIFICATION
// ======================================================
// The registry is the source of truth: the worker learns its identity and
// mode from the backend (POST /api/v1/worker/verify). It never decides for
// itself whether it is private/share/system — it only confirms.

async function verifyCredential() {
  let res;
  try {
    // worker_version is diagnostics only: the backend persists it on the
    // worker row (workers.version) so Settings can show which bundle a node
    // actually runs. It never participates in authorization.
    res = await fetchTimeout(`${ANIMASTOR_API_URL}/worker/verify`, {
      method: "POST",
      headers: hubHeaders(),
      body: JSON.stringify({ worker_version: WORKER_VERSION })
    });
  } catch (err) {
    // Network failure — the backend may be temporarily down. The hub still
    // enforces the credential on every call, so warn and continue.
    log("warn", `Credential verification unavailable (${err.message}) — continuing, the hub will enforce auth`);
    return;
  }
  if (res.status === 401 || res.status === 403) {
    authFailed("Animastor backend", res.status);
    return;
  }
  if (!res.ok) {
    log("warn", `Credential verification returned HTTP ${res.status} — continuing, the hub will enforce auth`);
    return;
  }
  try {
    const data = await res.json();
    log("info", "✓ Credential accepted");
    if (data.workspace_name) log("info", `✓ Workspace: ${data.workspace_name}`);
    log("info", `✓ Mode: ${String(data.mode || "").toUpperCase()}`);
    log("info", `✓ Worker type (registry): ${data.worker_type}`);
    if (data.worker_type && data.worker_type !== WORKER_TYPE) {
      log("warn", `WORKER_TYPE=${WORKER_TYPE} differs from the registry type ${data.worker_type} — the registry wins at the hub`);
    }
  } catch (_) { /* cosmetic only */ }
}

// ======================================================
// FIND OUTPUT NODES (Save*)
// ======================================================

// RESULT CONTRACT (relied upon by the backend — animastor-docs
// architecture/VOICE_REFERENCES.md §«Контракт результата воркера»):
// a job returns ONE file. For audio it is the first output reported in the
// ComfyUI history by a node whose class_type starts with "SaveAudio"
// (ui.audio[0].filename; nodes in workflow-key order). Custom output nodes
// opt in by their class name — e.g. SaveAudioAnimastorReference
// (comfyui-nodes/animastor-voice-ref) returns a WAV with a RIFF LIST/INFO
// JSON chunk. Do not change this selection without updating that document.
function findOutputNodes(workflow) {
  const result = { image: [], audio: [], video: [] };

  for (const [id, node] of Object.entries(workflow || {})) {
    const type = node.class_type || "";

    if (type.startsWith("SaveImage")) result.image.push(id);
    if (type.startsWith("SaveAudio")) result.audio.push(id);
    if (type.startsWith("SaveVideo") || type.startsWith("CreateVideo")) result.video.push(id);
  }

  return result;
}

// ======================================================
// GPU INFO
// ======================================================

function getGPUInfo() {
  try {
    const gpu = execSync(
      "nvidia-smi --query-gpu=name,memory.total --format=csv,noheader"
    ).toString().trim();

    const [name, vram] = gpu.split(",");
    return { name: name.trim(), vram: vram.trim() };
  } catch (err) {
    log("error", "nvidia-smi failed", err.message);
    return { name: "unknown", vram: "unknown" };
  }
}

// ======================================================
// WAIT COMFY with exponential backoff
// ======================================================

async function waitForComfyUI() {
  // "ComfyUI ready" is a stable marker: the installer and start-worker.sh
  // report the worker READY only after this exact line appears in the log.
  log("info", `Waiting for ComfyUI at ${comfyUrl("")}`);
  let attempts = 0;

  while (true) {
    try {
      const res = await fetchTimeout(comfyUrl("/system_stats"));
      if (res.ok) {
        log("info", "ComfyUI ready");
        await sleep(3000);
        return;
      }
      // A non-2xx answer (e.g. 404 behind a wrong reverse-proxy prefix) used
      // to be retried silently forever — say what is wrong.
      log("warn", `ComfyUI not ready (attempt ${attempts + 1}): HTTP ${res.status} from ${comfyUrl("/system_stats")}`);
    } catch (err) {
      log("warn", `ComfyUI not ready (attempt ${attempts + 1}): ${err.message}`);
    }

    attempts++;
    const backoff = Math.min(1000 * (1 << Math.min(attempts, 5)), 30000);
    process.stdout.write(".");
    await sleep(backoff);
  }
}

// ======================================================
// NODE-CLASS READINESS (GPU E2E 2026-11-02)
// ======================================================
// "ComfyUI answered" is not "this worker can run its profile": on 02.11 a
// worker 2.2.0 took 6 audio dispatches in 90 s from a ComfyUI without the
// qwen-tts nodes; ComfyUI rejected every prompt ("No prompt_id") and each
// rejection became a retry on the backend. Readiness is now: ComfyUI API
// answers AND /object_info lists every node class of the worker's profile.
// Until then the worker does not ask for tasks; one log line per distinct
// reason; the check is repeated with backoff.
//
// The class list (no list → readiness = ComfyUI API only, as before):
//   1. REQUIRED_NODE_CLASSES=ClassA,ClassB   (environment / .env)
//   2. node-classes.json next to worker.cjs (NODE_CLASSES_FILE overrides):
//        { "profiles": { "audio/qwen-tts": ["ClassA", "ClassB"] } }
//      — only profiles of this WORKER_TYPE ("<type>/…") are used.
const NODE_CLASSES_FILE = process.env.NODE_CLASSES_FILE || path.join(__dirname, "node-classes.json");
const NODE_CLASS_RECHECK_MIN_MS = Number(process.env.NODE_CLASS_RECHECK_MIN_MS || 5000);
const NODE_CLASS_RECHECK_MAX_MS = Number(process.env.NODE_CLASS_RECHECK_MAX_MS || 300000);

function loadRequiredNodeClasses() {
  const fromEnv = String(process.env.REQUIRED_NODE_CLASSES || "")
    .split(",").map((c) => c.trim()).filter(Boolean);
  if (fromEnv.length > 0) {
    return { classes: [...new Set(fromEnv)], source: "REQUIRED_NODE_CLASSES", profiles: [] };
  }
  let doc = null;
  try {
    doc = JSON.parse(fs.readFileSync(NODE_CLASSES_FILE, "utf8"));
  } catch (err) {
    if (err && err.code !== "ENOENT") {
      log("warn", `Node classes: cannot read ${NODE_CLASSES_FILE} (${err.message}) — readiness = ComfyUI API only`);
    }
    return { classes: [], source: null, profiles: [] };
  }
  const classes = new Set();
  const profiles = [];
  const byProfile = (doc && typeof doc.profiles === "object" && doc.profiles) || {};
  for (const [profile, list] of Object.entries(byProfile)) {
    if (!String(profile).startsWith(`${WORKER_TYPE}/`) || !Array.isArray(list)) continue;
    profiles.push(profile);
    for (const c of list) if (typeof c === "string" && c.trim()) classes.add(c.trim());
  }
  return { classes: [...classes], source: path.basename(NODE_CLASSES_FILE), profiles };
}

async function fetchObjectInfoClasses() {
  const res = await fetchTimeout(comfyUrl("/object_info"), {}, 60000);
  if (!res.ok) throw new Error(`HTTP ${res.status} from ${comfyUrl("/object_info")}`);
  const data = await res.json();
  if (!data || typeof data !== "object") throw new Error("/object_info is not a JSON object");
  return new Set(Object.keys(data));
}

/**
 * Block until ComfyUI's /object_info lists every required class. Returns
 * immediately when no class list is configured. One log line per distinct
 * reason (not per attempt); backoff NODE_CLASS_RECHECK_MIN_MS → MAX_MS.
 */
async function waitForNodeClasses() {
  const required = loadRequiredNodeClasses();
  if (required.classes.length === 0) {
    if (noListWarned) return required;
    noListWarned = true;
    log("warn", `Node classes: no profile list (REQUIRED_NODE_CLASSES / ${path.basename(NODE_CLASSES_FILE)}) — readiness = ComfyUI API only; run \`animastor doctor\` to write the list`);
    return required;
  }
  const what = required.profiles.length ? required.profiles.join(", ") : required.source;
  let delay = NODE_CLASS_RECHECK_MIN_MS;
  let lastReason = null;
  while (true) {
    let reason;
    try {
      const have = await fetchObjectInfoClasses();
      const missing = required.classes.filter((c) => !have.has(c));
      if (missing.length === 0) {
        log("info", `Node classes ready: ${required.classes.length}/${required.classes.length} (${what})`);
        return required;
      }
      reason = `ComfyUI lacks node classes ${missing.join(", ")} required by ${what}`;
    } catch (err) {
      reason = `ComfyUI /object_info unavailable (${err.message})`;
    }
    if (reason !== lastReason) {
      log("warn", `Not taking tasks: ${reason} — re-checking with backoff (up to ${Math.round(NODE_CLASS_RECHECK_MAX_MS / 1000)} s)`);
      lastReason = reason;
    }
    await sleep(delay);
    delay = Math.min(delay * 2, NODE_CLASS_RECHECK_MAX_MS);
  }
}

// Set when ComfyUI rejected a prompt (e.g. a node vanished after a ComfyUI
// restart): the next loop iteration re-runs the readiness gate before it
// asks the hub for another task.
let nodeReadinessStale = false;
// The "no class list" warning is printed once per process.
let noListWarned = false;

// ======================================================
// LOG SHIPPING (bundle 2.4.1, ORCHESTRATION_V2 §8.5)
// ======================================================

/**
 * Create the incremental shipper and start its 30 s tick. The tick callback
 * is not awaited anywhere: shipping can never delay the task loop.
 */
function startLogShipping() {
  if (!WORKER_LOG_SHIP) {
    log("info", "Worker log shipping: disabled (WORKER_LOG_SHIP=off)");
    return;
  }
  logShipper = createLogShipper({
    buffer: logBuffer,
    hubUrl: HUB_URL,
    token: ANIMASTOR_WORKER_TOKEN,
    workerId: WORKER_ID,
    intervalMs: LOG_SHIP_INTERVAL_MS,
    maxBytes: LOG_SHIP_MAX_BYTES,
    timeoutMs: LOG_SHIP_TIMEOUT_MS,
    secrets: LOG_SECRETS,
  });
  logShipper.start();
  log("info", `Worker log shipping: on (every ${Math.round(LOG_SHIP_INTERVAL_MS / 1000)} s, ≤ ${Math.round(LOG_SHIP_MAX_BYTES / 1024)} KiB gzip per POST)`);
}

/** One-shot full-buffer flush requested by the backend (send_log_tail). */
function shipLogTail() {
  if (!WORKER_LOG_SHIP || !logShipper) return;
  log("info", "Log tail requested by the backend — flushing the buffer");
  logShipper.shipAll().then((r) => {
    if (r && r.sent) log("info", `Log tail sent: ${r.sent} line(s)`);
  }).catch(() => { /* silent by contract */ });
}

/**
 * Optional: this worker starts ComfyUI itself. Its stdout/stderr then join the
 * same ring buffer, so a fresh log line from ComfyUI reaches the hub too.
 * In the shipped deployments ComfyUI is started by the installer/start-worker,
 * so COMFY_SPAWN_CMD is normally unset and this is a no-op.
 */
let comfyChild = null;
function maybeStartComfyChild() {
  if (!COMFY_SPAWN_CMD) return;
  try {
    comfyChild = spawn(COMFY_SPAWN_CMD, { shell: true, stdio: ["ignore", "pipe", "pipe"] });
    log("info", "ComfyUI spawned by the worker — its output joins the log buffer");
    captureStream(comfyChild.stdout, logBuffer, { level: "comfy" });
    captureStream(comfyChild.stderr, logBuffer, { level: "comfy" });
    comfyChild.on("error", (err) => log("error", "ComfyUI spawn failed", err && err.message || err));
    comfyChild.on("exit", (code, signal) => log("warn", `ComfyUI exited (code=${code}, signal=${signal || "-"})`));
  } catch (err) {
    log("error", "ComfyUI spawn failed", err && err.message || err);
  }
}

// ======================================================
// BEACON
// ======================================================

let beaconHealthy = false;

async function sendBeacon(gpuSnapshot) {
  try {
    const gpu = gpuSnapshot || getGPUInfo();

    const res = await fetchTimeout(`${HUB_URL}/beacon`, {
      method: "POST",
      headers: hubHeaders(),
      body: JSON.stringify({
        id: WORKER_ID,
        type: WORKER_TYPE,
        gpu: gpu.name,
        vram: gpu.vram,
        version: WORKER_VERSION,
        image_tag: WORKER_IMAGE_TAG,
        protocol_version: PROTOCOL_VERSION
      })
    });
    if (res.status === 401 || res.status === 403) {
      authFailed("GPU hub /beacon", res.status);
      return;
    }
    if (!res.ok) {
      throw new Error(`Hub rejected beacon: HTTP ${res.status}`);
    }
    // Stable marker (logged on the first accepted beacon and after every
    // recovery): the hub has recorded the heartbeat, Animastor shows ONLINE.
    if (!beaconHealthy) log("info", "Hub heartbeat accepted — worker is ONLINE");
    beaconHealthy = true;
  } catch (err) {
    beaconHealthy = false;
    log("error", "Beacon failed", err.message);
  }
}

// ======================================================
// GET TASK with backoff
// ======================================================

async function getTask() {
  try {
    const res = await fetchTimeout(
      `${HUB_URL}/task/next?worker=${WORKER_ID}&type=${WORKER_TYPE}`,
      { headers: hubHeaders() }
    );

    if (res.status === 401 || res.status === 403) {
      authFailed("GPU hub /task/next", res.status);
      return null;
    }
    if (!res.ok) return null;

    const data = await res.json();
    return data?.task || null;

  } catch (err) {
    log("warn", "getTask failed", err.message);
    return null;
  }
}

// ======================================================
// SAVE IMAGE (async)
// ======================================================

async function saveBase64ImageSafe(base64, filename) {
  const clean = base64.includes(",") ? base64.split(",")[1] : base64;
  const buffer = Buffer.from(clean, "base64");

  await fsp.mkdir(COMFY_INPUT_DIR, { recursive: true }).catch(err => log("warn", "mkdir", err.message));

  const filePath = path.join(COMFY_INPUT_DIR, filename);
  await fsp.writeFile(filePath, buffer);

  return { path: filePath, expectedSize: buffer.length };
}

// ======================================================
// WAIT FILE READY
// ======================================================

async function waitForFileReady(filePath, expectedSize, timeout = 5000) {
  const start = Date.now();

  while (true) {
    if (Date.now() - start > timeout) {
      throw new Error(`File not ready: ${filePath}`);
    }

    try {
      await fsp.access(filePath);
      const stats = await fsp.stat(filePath);
      if (stats.size === expectedSize && stats.size > 0) {
        await sleep(50);
        return true;
      }
    } catch (err) {
      log("warn", `waitForFileReady error`, err.message);
    }

    await sleep(100);
  }
}

// ======================================================
// RUN WORKFLOW
// ======================================================

// 2.2.3 (GPU retry policy, 2026-11-02): an Error may carry the optional
// structured fields sent with POST /task/error — `errorClass`
// (deterministic | transient | oom) and `comfyStatus` (HTTP status of
// ComfyUI POST /prompt). The backend classifies by them first and falls back
// to the reason text for older workers; the text stays as before.
function classifiedError(message, errorClass, comfyStatus) {
  const err = new Error(message);
  if (errorClass) err.errorClass = errorClass;
  if (Number.isInteger(comfyStatus)) err.comfyStatus = comfyStatus;
  return err;
}

const OOM_RE = /cuda out of memory|outofmemoryerror|cuda error: out of memory|allocation on device/i;

async function runWorkflow(workflow) {
  const body = workflow?.prompt
    ? { ...workflow, client_id: WORKER_ID }
    : { prompt: workflow, client_id: WORKER_ID };

  let res;
  try {
    res = await fetchTimeout(
      comfyUrl("/prompt"),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      }
    );
  } catch (err) {
    // ComfyUI unreachable / request aborted — the attempt may succeed later.
    throw classifiedError(`ComfyUI /prompt request failed: ${err && err.message || err}`, "transient");
  }

  const text = await res.text();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw classifiedError(
      "Invalid JSON from ComfyUI: " + text.slice(0, 500),
      res.status >= 500 ? "transient" : null,
      res.status
    );
  }

  if (!data.prompt_id) {
    log("error", "ComfyUI error", data);
    nodeReadinessStale = true;
    // Keep the "No prompt_id" prefix (backend/log compatibility) and carry
    // ComfyUI's own reason (e.g. invalid_prompt: node X does not exist).
    const e = data && data.error;
    const detail = e && typeof e === "object"
      ? [e.type, e.message].filter(Boolean).join(": ")
      : (typeof e === "string" ? e : "");
    // ComfyUI answered and rejected the prompt (validation / node_errors):
    // deterministic. A 5xx without prompt_id is ComfyUI itself failing.
    const cls = res.status >= 500 ? "transient" : "deterministic";
    throw classifiedError(
      detail ? `No prompt_id: ${String(detail).slice(0, 300)}` : "No prompt_id",
      cls,
      res.status
    );
  }

  return data.prompt_id;
}

// 2.2.3: ComfyUI finished the prompt with an execution error (history
// status_str "error", message "execution_error"). Before 2.2.3 the worker kept
// polling until the result timeout; now it fails right away with ComfyUI's
// exception. CUDA OOM → errorClass "oom"; other execution errors carry no
// class (the backend treats them as unknown → no retry).
function executionErrorOf(entry) {
  const st = entry && entry.status;
  if (!st || st.status_str !== "error") return null;
  let exType = "";
  let exMsg = "";
  for (const m of Array.isArray(st.messages) ? st.messages : []) {
    if (Array.isArray(m) && m[0] === "execution_error" && m[1]) {
      exType = m[1].exception_type || "";
      exMsg = m[1].exception_message || "";
      if (m[1].node_type) exMsg = `${m[1].node_type}: ${exMsg}`;
      break;
    }
  }
  const detail = [exType, exMsg].filter(Boolean).join(": ").replace(/\s+/g, " ").trim();
  const message = `ComfyUI execution error${detail ? `: ${detail.slice(0, 300)}` : ""}`;
  return classifiedError(message, OOM_RE.test(detail) ? "oom" : null);
}

// ======================================================
// WAIT RESULT (with backoff)
// ======================================================

async function waitResult(prompt_id, workflow, timeoutMs, opts = {}) {
  const start = Date.now();
  const outputsMap = findOutputNodes(workflow);
  const isVideoJob = outputsMap.video.length > 0;
  const effectiveTimeoutMs = timeoutMs || (isVideoJob ? VIDEO_RESULT_TIMEOUT_MS : RESULT_TIMEOUT_MS);
  log("debug", `waitResult: ${isVideoJob ? 'video' : 'other'} prompt=${prompt_id} timeoutMs=${effectiveTimeoutMs} (task.timeout_ms=${timeoutMs || 'none'})`);

  let videoDir, beforeFiles, videoPrefix;
  if (isVideoJob) {
    videoDir = path.join(COMFY_OUTPUT_DIR, 'video');
    try {
      beforeFiles = new Set((await fsp.readdir(videoDir)).filter(f => f.endsWith('.mp4')));
    } catch (err) {
      log("warn", "waitResult readdir", err && err.message || err);
      beforeFiles = new Set();
    }
    for (const id of outputsMap.video) {
      const prefix = workflow?.[id]?.inputs?.filename_prefix;
      if (prefix) {
        videoPrefix = path.basename(prefix);
        break;
      }
    }
  }

  let pollDelay = 500;
  while (true) {
    // 2.4.0: a protocol-3 job cancelled via the beacon stops waiting at once.
    if (opts.shouldAbort && opts.shouldAbort()) throw cancelledError();
    if (Date.now() - start > effectiveTimeoutMs) {
      try {
        const res = await fetchTimeout(comfyUrl(`/history/${prompt_id}`));
        const d = await res.json();
        log("error", `Timeout after ${Math.round(effectiveTimeoutMs / 60000)}min: last history response`, JSON.stringify(d).slice(0, 2000));
      } catch (err) {
        log("error", "Timeout: failed to fetch history", err.message);
      }
      throw classifiedError(`Timeout waiting result (${Math.round(effectiveTimeoutMs / 60000)}min)`, opts.timeoutErrorClass || "transient");
    }

    try {
      const res = await fetchTimeout(comfyUrl(`/history/${prompt_id}`));
      const data = await res.json();
      const execErr = executionErrorOf(data?.[prompt_id]);
      if (execErr) {
        log("error", `ComfyUI execution error for prompt ${prompt_id}`, execErr.message);
        throw execErr;
      }
      const outputs = data?.[prompt_id]?.outputs || {};

      for (const id of outputsMap.image) {
        const node = outputs[id];
        if (node?.images?.length > 0) return { type: "image", meta: node.images[0] };
      }

      for (const id of outputsMap.audio) {
        const node = outputs[id];
        if (node?.audio) {
          const a = Array.isArray(node.audio) ? node.audio[0] : node.audio;
          if (a?.filename) return { type: "audio", meta: a };
          if (a?.data || typeof a === "string") return { type: "audio_base64", data: a.data || a };
        }
      }

      if (isVideoJob) {
        for (const id of outputsMap.video) {
          const node = outputs[id];
          for (const key of ['videos', 'video', 'gifs', 'result', 'files', 'media']) {
            const arr = node?.[key];
            if (Array.isArray(arr) && arr[0]?.filename) {
              return { type: "video", meta: arr[0] };
            }
          }
        }

        for (const node of Object.values(outputs)) {
          for (const arr of Object.values(node || {})) {
            if (Array.isArray(arr) && arr[0]?.filename?.endsWith('.mp4')) {
              return { type: "video", meta: arr[0] };
            }
          }
        }

        if (data?.[prompt_id]?.status?.completed && videoDir) {
          const allFiles = await fsp.readdir(videoDir).catch(() => []);
          const mp4Files = allFiles.filter(f => f.endsWith('.mp4'));
          const newFiles = mp4Files.filter(f => !beforeFiles.has(f));
          const matched = videoPrefix
            ? newFiles.filter(f => f.startsWith(videoPrefix))
            : newFiles;
          if (matched.length > 0) {
            const newest = matched.sort().pop();
            log("info", `FS video: ${newest}`);
            return { type: "video", meta: { filename: newest, subfolder: 'video', type: 'output' } };
          }
        }
      }

      // Reset poll delay on successful response
      pollDelay = 500;
    } catch (err) {
      if (err && err.message && err.message.startsWith("ComfyUI execution error")) throw err;
      log("warn", "waitResult poll failed", err.message);
      // Exponential backoff on error: 1s → 2s → 4s → 8s cap
      pollDelay = Math.min(pollDelay * 2, 8000);
    }

    await sleep(pollDelay);
  }
}

// ======================================================
// DOWNLOAD RESULT (OOM-safe: читаем с диска, не через HTTP re-download)
// ======================================================
// ComfyUI уже сохранил результат на диск в COMFY_OUTPUT_DIR.
// Вместо повторного HTTP download (который держит 2x файл в памяти:
// arrayBuffer + base64), читаем локально.
// Для файлов > 50MB логируем предупреждение — они всё равно будут
// загружены в память как base64 (protocol limitation).

const MIME_MAP = {
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp',
  '.mp3': 'audio/mp3', '.wav': 'audio/wav', '.ogg': 'audio/ogg', '.flac': 'audio/flac',
  '.mp4': 'video/mp4', '.webm': 'video/webm', '.avi': 'video/avi', '.mov': 'video/quicktime',
};

async function downloadResult(result) {
  if (result.type === "audio_base64") {
    return `data:audio/mp3;base64,${result.data}`;
  }

  const f = result.meta;
  const filename = f.filename;
  const subfolder = f.subfolder || "";
  const ext = path.extname(filename).toLowerCase();
  const mime = MIME_MAP[ext] || 'application/octet-stream';

  // ── Try local filesystem first (OOM-safe, no HTTP overhead) ──
  const localPath = path.resolve(COMFY_OUTPUT_DIR, subfolder, filename);
  try {
    const stat = await fsp.stat(localPath).catch(() => null);
    if (stat && stat.isFile() && stat.size > 0) {
      if (stat.size > 50 * 1024 * 1024) {
        log("warn", `Large file (${(stat.size / 1024 / 1024).toFixed(1)}MB) — base64 will use significant memory: ${filename}`);
      }
      const buffer = await fsp.readFile(localPath);
      return `data:${mime};base64,${buffer.toString('base64')}`;
    }
  } catch (_) {}

  // ── Fallback: HTTP download from ComfyUI ──
  log("info", `Falling back to HTTP download for ${filename}`);
  let sf = subfolder;
  if (sf.includes("/")) sf = sf.split("/")[0];

  const url = comfyUrl(
    `/view?filename=${encodeURIComponent(filename)}&subfolder=${encodeURIComponent(sf)}&type=output`
  );

  const res = await fetchTimeout(url);
  if (!res.ok) throw new Error("Download failed: " + res.status);

  const buffer = await res.arrayBuffer();
  const raw = Buffer.from(buffer).toString("base64");

  return `data:${mime};base64,${raw}`;
}

// ======================================================
// SEND RESULT
// ======================================================

async function sendResult(task, data) {
  const { job_id, build_id, dispatch_id } = task;
  const res = await fetchTimeout(`${HUB_URL}/task/result`, {
    method: "POST",
    headers: hubHeaders(),
    body: JSON.stringify({
      job_id,
      build_id,
      dispatch_id,
      protocol_version: PROTOCOL_VERSION,
      result_base64: data,
      worker_version: WORKER_VERSION,
      worker_image_tag: WORKER_IMAGE_TAG
    })
  });
  if (!res.ok) {
    throw await hubRejection(res, "result");
  }
}

// 2.3.0 (2026-11-04): a rejected result / task error is logged WITH the hub's
// answer body, not only the status — hub v0.2.9 answers 409
// stale_or_unknown_dispatch with `detail` (no_running_record |
// dispatch_mismatch) + job_id / dispatch_id. The body is capped (300 chars)
// and never carries secrets (the hub echoes only ids and error codes).
const HUB_BODY_LOG_MAX = 300;

async function hubRejection(res, what) {
  let text = "";
  try { text = typeof res.text === "function" ? await res.text() : ""; } catch (_) { text = ""; }
  text = String(text || "").replace(/\s+/g, " ").trim().slice(0, HUB_BODY_LOG_MAX);
  let detail = null;
  try { const j = JSON.parse(text); detail = j && typeof j.detail === "string" ? j.detail : null; } catch (_) {}
  const err = new Error(`Hub rejected ${what}: HTTP ${res.status}${text ? ` ${text}` : ""}`);
  err.hubStatus = res.status;
  err.hubDetail = detail;
  return err;
}

async function sendTaskError(task, reason, meta = {}) {
  const payload = {
    job_id: task.job_id,
    build_id: task.build_id || null,
    dispatch_id: task.dispatch_id,
    protocol_version: PROTOCOL_VERSION,
    reason: String(reason || "worker_error").slice(0, 500),
    worker_version: WORKER_VERSION,
    worker_image_tag: WORKER_IMAGE_TAG
  };
  // 2.2.3: optional, additive (older hubs/backends ignore unknown fields).
  if (meta.errorClass) payload.error_class = meta.errorClass;
  if (Number.isInteger(meta.comfyStatus)) payload.comfy_status = meta.comfyStatus;
  const res = await fetchTimeout(`${HUB_URL}/task/error`, {
    method: "POST",
    headers: hubHeaders(),
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    throw await hubRejection(res, "task error");
  }
}

// ======================================================
// PROTOCOL 3 (2.4.0) — hub relay /v3, result PUT, cancel via beacon
// ======================================================

// The protocol-3 job being executed right now (null between jobs). The v3
// beacon reports its id; a cancel for it flips `cancelled` and interrupts
// ComfyUI. Nothing else is shared between the beacon and the loop.
let currentV3 = null;
let v3HubState = null;          // last /v3/task/next outcome class (log on change only)

function cancelledError() {
  const err = new Error("cancelled by the backend (beacon)");
  err.cancelled = true;
  return err;
}

async function interruptComfy(promptId, why) {
  try {
    await fetchTimeout(comfyUrl("/interrupt"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(promptId ? { prompt_id: promptId } : {}),
    }, 10000);
    log("info", `ComfyUI /interrupt sent (${why}${promptId ? `, prompt ${promptId}` : ""})`);
  } catch (err) {
    log("warn", `ComfyUI /interrupt failed (${why})`, err && err.message || err);
  }
}

function noteV3State(state, detail) {
  if (state === v3HubState) return;
  v3HubState = state;
  if (state === "ok") log("info", "Hub protocol 3 (/v3) available");
  else log("info", `Hub protocol 3 unavailable (${detail}) — serving v2 /task/next`);
}

async function sendBeaconV3(gpuSnapshot) {
  const job = currentV3;
  try {
    const gpu = gpuSnapshot || getGPUInfo();
    const res = await fetchTimeout(`${HUB_URL}/v3/beacon`, {
      method: "POST",
      headers: hubHeaders(),
      body: JSON.stringify({
        gpu: gpu.name,
        vram: gpu.vram,
        version: WORKER_VERSION,
        image_tag: WORKER_IMAGE_TAG,
        current_job_id: job ? job.jobId : null,
      }),
    }, 15000);
    if (res.status === 401) {
      authFailed("GPU hub /v3/beacon", res.status);
      return;
    }
    if (!res.ok) return;          // old hub (404) / backend off (503): the v2 beacon still runs
    const data = await res.json().catch(() => ({}));
    // 2.4.1 (§8.5): the backend asked ONCE for the whole log buffer. The flush
    // is fire-and-forget — the task loop is never blocked by log shipping.
    if (data && data.send_log_tail === true) shipLogTail();
    const cancel = Array.isArray(data && data.cancel) ? data.cancel : [];
    if (job && !job.cancelled && cancel.includes(job.jobId) && currentV3 === job) {
      job.cancelled = true;
      log("info", `Job ${job.jobId} cancelled by the backend — interrupting ComfyUI, no upload`);
      // no prompt yet: runV3Task interrupts right after /prompt answers (never a blind interrupt)
      if (job.promptId) await interruptComfy(job.promptId, "cancel");
    }
  } catch (err) {
    log("warn", "v3 beacon failed", err && err.message || err);
  }
}

/** One relay call. → task | null; never throws. 401 = the hub rejected OUR credential → terminal. */
async function getTaskV3() {
  try {
    const res = await fetchTimeout(`${HUB_URL}/v3/task/next?type=${encodeURIComponent(WORKER_TYPE)}`, { headers: hubHeaders() }, 20000);
    if (res.status === 401) {
      authFailed("GPU hub /v3/task/next", res.status);
      return null;
    }
    if (!res.ok) {
      noteV3State(`http_${res.status}`, `HTTP ${res.status}`);
      return null;
    }
    const data = await res.json();
    noteV3State("ok");
    return data && data.task ? data.task : null;
  } catch (err) {
    noteV3State("error", err && err.message || String(err));
    return null;
  }
}

const MIME_BY_TYPE = { audio: "audio/mpeg", image: "image/png", video: "video/mp4" };

/**
 * Where the result bytes come from — never base64 on the wire:
 *   ComfyUI output file on disk (COMFY_OUTPUT_DIR) → stream;
 *   not on disk → stream ComfyUI /view;
 *   inline audio data from history (rare) → one buffer.
 */
async function resultSource(result) {
  if (result.type === "audio_base64") {
    const raw = String(result.data || "");
    const buf = Buffer.from(raw.includes(",") ? raw.split(",")[1] : raw, "base64");
    return { kind: "buffer", buffer: buf, size: buf.length, contentType: "audio/mpeg", label: "inline audio" };
  }
  const f = result.meta || {};
  const ext = path.extname(f.filename || "").toLowerCase();
  const contentType = MIME_MAP[ext] || MIME_BY_TYPE[result.type] || "application/octet-stream";
  const localPath = path.resolve(COMFY_OUTPUT_DIR, f.subfolder || "", f.filename || "");
  const st = await fsp.stat(localPath).catch(() => null);
  if (st && st.isFile() && st.size > 0) {
    return { kind: "file", path: localPath, size: st.size, contentType, label: f.filename };
  }
  let sf = f.subfolder || "";
  if (sf.includes("/")) sf = sf.split("/")[0];
  const url = comfyUrl(`/view?filename=${encodeURIComponent(f.filename || "")}&subfolder=${encodeURIComponent(sf)}&type=output`);
  return { kind: "url", url, size: null, contentType, label: `${f.filename} (ComfyUI /view)` };
}

function openHttpGetStream(urlStr) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const mod = u.protocol === "https:" ? https : http;
    const req = mod.get(u, (res) => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`ComfyUI /view HTTP ${res.statusCode}`)); }
      const len = Number(res.headers["content-length"]);
      resolve({ stream: res, size: Number.isFinite(len) && len > 0 ? len : null });
    });
    req.setTimeout(60000, () => req.destroy(new Error("ComfyUI /view timeout")));
    req.on("error", reject);
  });
}

/** One HTTP request with a streamed / buffered body. → {status, text}; rejects on network error. */
function httpSend(urlStr, method, headers, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const mod = u.protocol === "https:" ? https : http;
    const req = mod.request(u, { method, headers }, (res) => {
      let text = "";
      res.setEncoding("utf8");
      res.on("data", (c) => { if (text.length < 2000) text += c; });
      res.on("end", () => resolve({ status: res.statusCode, text }));
      res.on("error", reject);
    });
    req.setTimeout(V3_UPLOAD_IDLE_MS, () => req.destroy(new Error(`no progress for ${V3_UPLOAD_IDLE_MS}ms`)));
    req.on("error", reject);
    if (body == null) return req.end();
    if (Buffer.isBuffer(body) || typeof body === "string") return req.end(body);
    body.on("error", (e) => req.destroy(e));
    body.pipe(req);
  });
}

function bodyText(text) {
  return String(text || "").replace(/\s+/g, " ").trim().slice(0, HUB_BODY_LOG_MAX);
}

/**
 * Deliver to the backend with the job's upload token. Retries ONLY network
 * errors / 5xx (up to V3_DELIVERY_ATTEMPTS, backoff 1→2→4→8 s, cap 15 s);
 * 409 (not the current attempt any more) / 401 (token) and every other 4xx
 * stop at once — the job is dropped, the backend has already decided.
 * → { ok, status, attempts, stopped? }
 */
async function deliverV3(what, url, method, token, makeBody, extraHeaders = {}) {
  let delay = V3_DELIVERY_BACKOFF_MS;
  for (let attempt = 1; attempt <= V3_DELIVERY_ATTEMPTS; attempt++) {
    let r = null;
    let netErr = null;
    try {
      const { body, headers } = await makeBody();
      r = await httpSend(url, method, { Authorization: `Bearer ${token}`, ...extraHeaders, ...headers }, body);
    } catch (err) {
      netErr = err;
    }
    if (r && r.status >= 200 && r.status < 300) return { ok: true, status: r.status, attempts: attempt };
    if (r && r.status < 500) {
      const why = r.status === 409 ? "not the current attempt (409)" : r.status === 401 ? "upload token rejected (401)" : `HTTP ${r.status}`;
      log("warn", `${what} rejected: ${why} — dropping the job${r.text ? `: ${bodyText(r.text)}` : ""}`);
      return { ok: false, status: r.status, attempts: attempt, stopped: true };
    }
    const reason = r ? `HTTP ${r.status}${r.text ? ` ${bodyText(r.text)}` : ""}` : (netErr && netErr.message) || String(netErr);
    if (attempt === V3_DELIVERY_ATTEMPTS) {
      log("error", `${what} failed after ${attempt} attempt(s): ${reason}`);
      return { ok: false, status: r ? r.status : null, attempts: attempt };
    }
    log("warn", `${what} attempt ${attempt}/${V3_DELIVERY_ATTEMPTS} failed: ${reason} — retry in ${delay}ms`);
    await sleep(delay);
    delay = Math.min(delay * 2, 15000);
  }
  return { ok: false, status: null, attempts: V3_DELIVERY_ATTEMPTS };
}

async function putResultV3(task, src) {
  return deliverV3(`Result PUT ${task.job_id}#${task.attempt}`, task.upload_url, "PUT", task.upload_token, async () => {
    if (src.kind === "buffer") {
      return { body: src.buffer, headers: { "Content-Type": src.contentType, "Content-Length": String(src.size) } };
    }
    if (src.kind === "file") {
      const st = await fsp.stat(src.path);
      return { body: fs.createReadStream(src.path), headers: { "Content-Type": src.contentType, "Content-Length": String(st.size) } };
    }
    const g = await openHttpGetStream(src.url);
    const headers = { "Content-Type": src.contentType };
    if (g.size) headers["Content-Length"] = String(g.size);
    return { body: g.stream, headers };
  });
}

async function sendErrorV3(task, reason, meta = {}) {
  if (!task.error_url || !task.upload_token) {
    log("error", `Cannot report error for ${task.job_id}: task has no error_url / upload_token`);
    return { ok: false };
  }
  const payload = { reason: String(reason || "worker_error").slice(0, 500), worker_version: WORKER_VERSION };
  payload.error_class = meta.errorClass || null;
  if (Number.isInteger(meta.comfyStatus)) payload.comfy_status = meta.comfyStatus;
  const json = JSON.stringify(payload);
  return deliverV3(`Error report ${task.job_id}#${task.attempt}`, task.error_url, "POST", task.upload_token, async () => ({
    body: json, headers: { "Content-Type": "application/json", "Content-Length": String(Buffer.byteLength(json)) },
  }));
}

// ======================================================
// V3 INPUT FILES (2.5.0, ORCHESTRATION_V2 §8.1, stage 3)
// ======================================================
// task.input_files = [{name, url, sha256, bytes}]: frames of a video group (and
// any future input) are fetched by link with the job's upload token — no
// base64 in the task. Each file lands in COMFY_INPUT_DIR under `name` (the name
// the workflow's LoadImage nodes use) via a temp file + rename after the sha256
// and size match; it is removed after the job like the v2 base64 inputs.
const SAFE_INPUT_NAME = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,200}$/;
const V3_INPUT_ATTEMPTS = Number(process.env.V3_INPUT_ATTEMPTS) > 0 ? Number(process.env.V3_INPUT_ATTEMPTS) : 3;

function inputError(message, errorClass, extra = {}) {
  return Object.assign(new Error(message), { errorClass, ...extra });
}

function httpGetBuffer(urlStr, headers, maxBytes) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const mod = u.protocol === "https:" ? https : http;
    const req = mod.get(u, { headers }, (res) => {
      const chunks = [];
      let size = 0;
      res.on("data", (c) => {
        size += c.length;
        if (size > maxBytes) { req.destroy(new Error(`input larger than ${maxBytes} bytes`)); return; }
        chunks.push(c);
      });
      res.on("end", () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on("error", reject);
    });
    req.setTimeout(V3_UPLOAD_IDLE_MS, () => req.destroy(new Error(`no progress for ${V3_UPLOAD_IDLE_MS}ms`)));
    req.on("error", reject);
  });
}

/** Download every input of a v3 task into COMFY_INPUT_DIR. → created paths (also journalled). */
async function fetchInputsV3(task, ctx, created) {
  const list = Array.isArray(task.input_files) ? task.input_files : [];
  if (!list.length) return created;
  await fsp.mkdir(COMFY_INPUT_DIR, { recursive: true });
  for (const f of list) {
    const name = f && typeof f.name === "string" ? f.name : "";
    if (!SAFE_INPUT_NAME.test(name) || name.includes("..")) throw inputError(`bad input name ${JSON.stringify(name)}`, "deterministic");
    if (typeof f.url !== "string" || !/^https?:\/\//.test(f.url)) throw inputError(`input ${name}: no url`, "deterministic");
    const want = typeof f.sha256 === "string" ? f.sha256 : null;
    const maxBytes = Number.isInteger(f.bytes) && f.bytes > 0 ? f.bytes : 256 * 1024 * 1024;
    let got = null;
    let delay = V3_DELIVERY_BACKOFF_MS;
    for (let attempt = 1; attempt <= V3_INPUT_ATTEMPTS; attempt++) {
      let r = null; let netErr = null;
      try { r = await httpGetBuffer(f.url, { Authorization: `Bearer ${task.upload_token}` }, maxBytes); } catch (err) { netErr = err; }
      if (r && r.status === 200) { got = r.body; break; }
      if (r && r.status < 500) {
        let code = "";
        try { code = JSON.parse(r.body.toString("utf8")).code || ""; } catch (_) { /* not json */ }
        // 409 stale_attempt / 401: the backend no longer wants this attempt → drop the job silently
        if ((r.status === 409 && code === "stale_attempt") || r.status === 401) throw inputError(`input ${name}: HTTP ${r.status} ${code}`.trim(), "deterministic", { dropJob: true });
        throw inputError(`input ${name}: HTTP ${r.status} ${code}`.trim(), "deterministic");
      }
      const why = r ? `HTTP ${r.status}` : (netErr && netErr.message) || String(netErr);
      if (attempt === V3_INPUT_ATTEMPTS) throw inputError(`input ${name}: ${why}`, "transient");
      log("warn", `Input ${name} attempt ${attempt}/${V3_INPUT_ATTEMPTS} failed: ${why} — retry in ${delay}ms`);
      await sleep(delay);
      delay = Math.min(delay * 2, 15000);
    }
    if (want && crypto.createHash("sha256").update(got).digest("hex") !== want) throw inputError(`input ${name}: sha256 mismatch`, "transient");
    if (Number.isInteger(f.bytes) && f.bytes > 0 && got.length !== f.bytes) throw inputError(`input ${name}: ${got.length} bytes, expected ${f.bytes}`, "transient");
    const filePath = path.join(COMFY_INPUT_DIR, name);
    const tmp = `${filePath}.${process.pid}.part`;
    created.push(filePath);
    await journal.addInputFile(ctx, filePath);
    await fsp.writeFile(tmp, got);
    await fsp.rename(tmp, filePath);
    log("info", `Input ready: ${name} (${Math.round(got.length / 1024)}KB, sha256 ok)`);
  }
  return created;
}

function v3TaskProblem(task) {
  if (task.protocol_version !== V3_PROTOCOL_VERSION) return `protocol_version=${task.protocol_version || "missing"}`;
  if (!task.job_id || !Number.isInteger(task.attempt)) return "job_id / attempt missing";
  if (!task.upload_url || !task.upload_token) return "upload_url / upload_token missing";
  if (!task.params || typeof task.params !== "object") return "params missing";
  return null;
}

async function runV3Task(task) {
  const problem = v3TaskProblem(task);
  if (problem) {
    log("error", `Rejecting incompatible protocol-3 task ${task.job_id || "?"}: ${problem}`);
    await sendErrorV3(task, `incompatible_task_protocol:${problem}`, { errorClass: "deterministic" });
    return;
  }
  const jobId = task.job_id;
  const dispatchId = `v3.${task.attempt}`;
  const job = { jobId, attempt: task.attempt, cancelled: false, promptId: null };
  currentV3 = job;
  log("info", `Task ${jobId} attempt ${task.attempt} (protocol 3, kind=${task.kind || "?"}, type=${WORKER_TYPE}, timeout_ms=${task.timeout_ms || "default"})`);
  // report the job right away (current_job_id) instead of waiting up to 10 s
  sendBeaconV3().catch(() => {});

  let outputPath = null;
  let delivered = false;
  const inputFiles = [];
  try {
    await journal.createJob({ jobId, dispatchId, log });
    // 2.5.0: input files by link (video frames) before the workflow starts
    await fetchInputsV3(task, { jobId, dispatchId, log }, inputFiles);
    job.promptId = await runWorkflow(task.params);
    if (job.cancelled) {
      await interruptComfy(job.promptId, "cancel");
      throw cancelledError();
    }
    let result;
    try {
      // I3: timeout_ms only bounds how long we wait / when we interrupt ComfyUI;
      // the status belongs to the backend (deadline in gen_jobs).
      result = await waitResult(job.promptId, task.params, task.timeout_ms, {
        shouldAbort: () => job.cancelled,
        timeoutErrorClass: "timeout",
      });
    } catch (err) {
      if (err && err.errorClass === "timeout") await interruptComfy(job.promptId, "timeout");
      throw err;
    }
    if (result.meta && result.meta.filename) {
      outputPath = path.resolve(COMFY_OUTPUT_DIR, result.meta.subfolder || "", result.meta.filename);
      await journal.setOutputAndGenerated({ jobId, dispatchId, log }, outputPath);
    }
    if (job.cancelled) throw cancelledError();
    const src = await resultSource(result);
    log("debug", `result for ${jobId}: ${src.label}, ${src.size != null ? Math.round(src.size / 1024) + "KB" : "size unknown"}, ${src.contentType}`);
    const r = await putResultV3(task, src);
    if (r.ok) {
      await journal.setDelivered({ jobId, dispatchId, log });
      delivered = true;
      log("info", `Done ${jobId} (protocol 3, HTTP ${r.status}, ${r.attempts} attempt(s))`);
    } else if (r.stopped) {
      // the backend refused this attempt — the output is not needed any more
      delivered = true;
    }
  } catch (err) {
    if (err && err.cancelled) {
      log("info", `Job ${jobId} stopped: cancelled — no upload, no error report`);
      delivered = true;   // nothing to keep for a cancelled job
    } else if (err && err.dropJob) {
      log("warn", `Job ${jobId} dropped: ${err.message} — the backend no longer serves this attempt`);
      delivered = true;
    } else {
      log("error", `Failed ${jobId}`, err && err.message || err);
      await sendErrorV3(task, err && err.message || err || "worker_error", {
        errorClass: err && err.errorClass,
        comfyStatus: err && err.comfyStatus,
      });
    }
  } finally {
    if (currentV3 === job) currentV3 = null;
    try {
      const toCleanOutput = delivered ? outputPath : null;
      const cleanupResult = await cleanupJobArtifacts({ inputFiles, outputFile: toCleanOutput });
      for (const f of cleanupResult.failed) log("warn", `Cleanup ${jobId}: failed to remove ${f.path}: ${f.reason}`);
      if (cleanupResult.failed.length === 0) await journal.removeJob({ jobId, dispatchId, log });
    } catch (cleanupErr) {
      log("warn", `Cleanup ${jobId}: error: ${cleanupErr.message}`);
    }
  }
}

// ======================================================
// LOOP (with backoff on empty queue)
// ======================================================

let emptyQueueDelay = TASK_SLEEP_MS;

function beaconTick() {
  const gpu = getGPUInfo();
  sendBeacon(gpu);
  if (WORKER_PROTOCOL !== "v2only") sendBeaconV3(gpu);
}

async function workerLoop() {
  // First heartbeat right away (not after BEACON_INTERVAL_MS): the worker
  // shows ONLINE as soon as it is actually serving.
  log("info", `Protocol mode: ${WORKER_PROTOCOL === "v2only" ? "v2only (no /v3 calls)" : "both (v3 relay first, then v2)"}`);
  beaconTick();
  setInterval(beaconTick, BEACON_INTERVAL_MS);

  while (true) {
    if (nodeReadinessStale) {
      nodeReadinessStale = false;
      await waitForNodeClasses();
    }
    // 2.4.0: one v3 relay call, then ONE v2 pop; sleep only when both are empty.
    const v3task = WORKER_PROTOCOL === "v2only" ? null : await getTaskV3();
    if (v3task) {
      emptyQueueDelay = TASK_SLEEP_MS;
      await runV3Task(v3task);
      continue;
    }
    const task = await getTask();

    if (!task) {
      await sleep(emptyQueueDelay);
      emptyQueueDelay = Math.min(emptyQueueDelay * 2, 15000);
      continue;
    }
    emptyQueueDelay = TASK_SLEEP_MS; // reset on task received
    await runV2Task(task);
  }
}

/** One protocol-2 task — exactly the 2.3.0 handling. */
async function runV2Task(task) {
  log("info", `Task ${task.job_id} (type=${WORKER_TYPE}, timeout_ms=${task.timeout_ms || 'default'})`);

  if (task.protocol_version !== PROTOCOL_VERSION || !task.dispatch_id) {
    const reason = `incompatible_task_protocol:${task.protocol_version || 'missing'}`;
    log("error", `Rejecting incompatible task: protocol=${task.protocol_version}, dispatch=${task.dispatch_id || 'missing'}`);
    if (task.job_id && task.build_id && task.dispatch_id) {
      try {
        await sendTaskError(task, reason, { errorClass: "deterministic" });
      } catch (sendErr) {
        log("error", "Failed to report incompatible task", sendErr.message);
      }
    }
    await sleep(TASK_SLEEP_MS);
    return;
  }

  // ── Per-job artifact tracking (cleanup after job) ──
  // Точечная уборка: удаляются ТОЛЬКО файлы этой job. Output удаляется
  // только после успешной доставки результата (downloadResult + sendResult),
  // чтобы при ошибке sendResult() единственный результат не потерялся.
  // Journal (worker-local persistent) фиксирует lifecycle
  // CREATED→GENERATED→DELIVERED→CLEANED, чтобы после crash restart мог
  // дочистить файлы job, результат которой уже доставлен в hub.
  const createdInputFiles = [];
  let outputPath = null;
  let outputDelivered = false;
  const jobId = task.job_id;
  const dispatchId = task.dispatch_id;

  try {
    // Journal: CREATED — создаётся ДО первого временного input-файла.
    await journal.createJob({ jobId, dispatchId, log });

    if (task.assets?.images) {
      const [jobBase] = task.job_id.split(JOB_ID_SPLIT_RE);
      const scenePrefix = jobBase.replace(/_g\d+$/, '');
      for (const [unitId, base64] of Object.entries(task.assets.images)) {
        const filename = `${scenePrefix}_${unitId}.png`;
        const filePath = path.join(COMFY_INPUT_DIR, filename);
        createdInputFiles.push(filePath);
        const { expectedSize } = await saveBase64ImageSafe(base64, filename);
        // Journal: каждый фактически созданный reference image.
        await journal.addInputFile({ jobId, dispatchId, log }, filePath);
        log("info", `Multi-image saved: ${filename}`);
        await waitForFileReady(filePath, expectedSize);
        log("info", `Multi-image ready: ${filename}`);
      }
    } else if (task.assets?.image) {
      const [baseId] = task.job_id.split(JOB_ID_SPLIT_RE);
      const filename = `${baseId}.png`;
      const filePath = path.join(COMFY_INPUT_DIR, filename);
      createdInputFiles.push(filePath);
      const { expectedSize } = await saveBase64ImageSafe(task.assets.image, filename);
      await journal.addInputFile({ jobId, dispatchId, log }, filePath);
      log("info", `Image saved: ${filename}`);
      await waitForFileReady(filePath, expectedSize);
      log("info", `Image ready: ${filename}`);
    }

    const prompt_id = await runWorkflow(task.params);
    // timeout_ms приходит с задачей (backend → gpu-hub → worker): per-job
    // таймаут для данного типа генерации. Если нет — per-type fallback.
    const result = await waitResult(prompt_id, task.params, task.timeout_ms);

    // Точечный output этой job: COMFY_OUTPUT_DIR + subfolder + filename.
    // Это именно тот файл, который waitResult выбрал как результат (в т.ч.
    // для video — реально выбранный mp4 из history/fallback/fs-scan).
    if (result.meta && result.meta.filename) {
      outputPath = path.resolve(COMFY_OUTPUT_DIR, result.meta.subfolder || "", result.meta.filename);
      // Journal: GENERATED — известен конкретный output-файл.
      await journal.setOutputAndGenerated({ jobId, dispatchId, log }, outputPath);
    }

    const base64 = await downloadResult(result);
    log("debug", `result for ${task.job_id}: type=${result.type} size=${Math.round(base64.length / 1024)}KB`);
    await sendResult(task, base64);
    // Journal: DELIVERED — HTTP 200 от hub = результат уже durable в hub
    // Redis (animastor:result:* записан до ответа 200). Output можно удалять.
    await journal.setDelivered({ jobId, dispatchId, log });
    outputDelivered = true;
    log("info", `Done ${task.job_id}`);

  } catch (err) {
    log("error", `Failed ${task.job_id}`, err.message);

    try {
      await sendTaskError(task, err && err.message || err || "worker_error", {
        errorClass: err && err.errorClass,
        comfyStatus: err && err.comfyStatus
      });
    } catch (sendErr) {
      log("error", "Failed to send error to hub", sendErr.message);
    }
  } finally {
    // Cleanup after job: только собственные временные файлы этой job.
    // Output — ТОЛЬКО после успешной доставки результата; при ошибке
    // downloadResult()/sendResult() output сохраняется.
    try {
      const toCleanOutput = outputDelivered ? outputPath : null;
      const cleanupResult = await cleanupJobArtifacts({
        inputFiles: createdInputFiles,
        outputFile: toCleanOutput,
      });

      if (cleanupResult.cleaned > 0 || cleanupResult.failed.length > 0) {
        log("info",
          `Cleanup ${task.job_id}: removed ${cleanupResult.cleaned} artifact(s) ` +
          `(${createdInputFiles.length} input, ${toCleanOutput ? "1 output" : "0 output"})`);
      }
      for (const f of cleanupResult.failed) {
        log("warn", `Cleanup ${task.job_id}: failed to remove ${f.path}: ${f.reason}`);
      }

      if (cleanupResult.failed.length === 0) {
        // CLEANED: все файлы job удалены → journal больше не нужен.
        await journal.removeJob({ jobId, dispatchId, log });
      } else {
        // Частичный cleanup: journal остаётся — следующий recovery дочистит
        // оставшиеся файлы (например, один input из видео-набора).
        log("warn", `Cleanup ${task.job_id}: partial cleanup (${cleanupResult.failed.length} failed) — journal kept for recovery`);
      }
    } catch (cleanupErr) {
      // Cleanup никогда не должен маскировать исходную ошибку job.
      log("warn", `Cleanup ${task.job_id}: error: ${cleanupErr.message}`);
    }
  }
}

// ======================================================
// MAIN
// ======================================================

async function main() {
  // PW-4 FAIL CLOSED startup gate: without a credential the worker refuses
  // to run. There is no "system pool (no credential)" mode anymore — a
  // missing token must never silently turn this GPU into shared capacity.
  if (!ANIMASTOR_WORKER_TOKEN) {
    log("error", "Worker authentication failed — check ANIMASTOR_WORKER_TOKEN");
    log("error", "No worker credential configured. This worker cannot start:");
    log("error", "  1. Open Animastor → Settings → Workers and create a worker");
    log("error", "     (choose Private for your own workspace, or Share to volunteer it).");
    log("error", "  2. Copy the one-time credential (wrk.…).");
    log("error", "  3. Set ANIMASTOR_WORKER_TOKEN=wrk.… in ./.env (or the environment).");
    process.exit(1);
  }

  log("info", `Worker ${WORKER_TYPE} started`);
  log("info", `Worker ID: ${WORKER_ID} (label only — identity comes from the credential)`);
  log("info", `Worker version: ${WORKER_VERSION || 'unknown'}`);
  log("info", `Worker image tag: ${WORKER_IMAGE_TAG || 'unknown'}`);
  log("info", `Hub URL: ${HUB_URL}`);
  log("info", `Protocol version: ${PROTOCOL_VERSION}`);

  // 2.4.1: fresh logs. Shipping starts before the readiness gates so startup
  // diagnostics are not lost; the tick is fire-and-forget.
  startLogShipping();
  maybeStartComfyChild();

  // Confirm identity + mode against the registry before doing any work.
  await verifyCredential();

  await waitForComfyUI();

  // Readiness = ComfyUI API answered AND /object_info has the profile's node
  // classes. No task is requested (and no heartbeat sent) before that.
  await waitForNodeClasses();

  // Crash-safe recovery: завершить cleanup незакрытых job упавшего worker.
  // delivered → удаляем input+output; created/generated → только input
  // (output без proof DELIVERED не трогаем — защита единственной копии).
  await journal.recoverCleanupJournal({ log });

  await workerLoop();
}

main().catch(err => {
  log("error", "Worker crashed", err.message);
  process.exit(1);
});
