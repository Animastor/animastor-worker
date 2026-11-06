// ======================================================
// animastor-worker — protocol 3 (bundle 2.4.0, ORCHESTRATION_V2 §8, stage 1c)
// ======================================================
// Spawns worker.cjs against three mocks: a fake hub (v3 relay + v2
// endpoints), a fake backend (PUT upload_url / POST error_url) and a fake
// ComfyUI (/prompt, /history, /view, /interrupt). Covered:
//   - v3 cycle: GET /v3/task/next → ComfyUI → PUT upload_url as a raw stream
//     (bytes = the output file, Content-Type by type, Content-Length, Bearer
//     upload_token; no base64, nothing to the hub's /task/result);
//   - result not on disk → streamed from ComfyUI /view;
//   - PUT retries on 5xx / network error (≤ V3_DELIVERY_ATTEMPTS), 409 / 401 →
//     stop at once, no error report;
//   - execution error → POST error_url {error_class, reason, comfy_status};
//   - task.timeout_ms → ComfyUI /interrupt + error_class=timeout (I3);
//   - cancel in the v3 beacon answer → ComfyUI /interrupt, no PUT, no error;
//   - fallback: v3 {task:null} / 404 / 503 → ONE v2 pop per iteration (v3 first);
//   - WORKER_PROTOCOL=v2only → no /v3 call at all.

const { spawn } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const { describe, it, expect } = require('./harness.cjs');

const BUNDLE_DIR = path.join(__dirname, '..', 'worker');
const JOB = '7c0d9a3e-1111-4222-8333-444455556666';
const TOKEN = 'v1.1999999999.test-upload-token';
const AUDIO = Buffer.concat([Buffer.from('ID3'), crypto.randomBytes(256 * 1024)]);   // 256 KiB "mp3"
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');

function copyBundle() {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'worker-v3-'));
    for (const f of fs.readdirSync(BUNDLE_DIR)) {
        if (f === 'node_modules' || f === '.env') continue;
        fs.copyFileSync(path.join(BUNDLE_DIR, f), path.join(tmp, f));
    }
    return tmp;
}

function listen(handler) {
    return new Promise((resolve) => {
        const srv = http.createServer(handler);
        srv.listen(0, '127.0.0.1', () => resolve(srv));
    });
}

function readBody(req) {
    return new Promise((resolve) => {
        const chunks = [];
        req.on('data', (c) => chunks.push(c));
        req.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

const json = (res, status, obj) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };

/** ComfyUI mock. opts.prompt: 'ok' | 'reject'; opts.history: 'done' | 'never'; opts.onDisk: bool */
async function fakeComfy(opts, st) {
    return listen(async (req, res) => {
        const body = await readBody(req);
        st.comfy.push(`${req.method} ${req.url.split('?')[0]}`);
        if (req.url === '/system_stats') return json(res, 200, { system: {}, devices: [] });
        if (req.url === '/prompt' && req.method === 'POST') {
            st.prompts.push(JSON.parse(body.toString() || '{}'));
            if (opts.prompt === 'reject') return json(res, 400, { error: { type: 'invalid_prompt', message: 'node QwenTTS does not exist' }, node_errors: {} });
            return json(res, 200, { prompt_id: `p-${st.prompts.length}`, number: 1, node_errors: {} });
        }
        if (req.url.startsWith('/history/')) {
            const id = req.url.slice('/history/'.length);
            if (opts.history === 'never') return json(res, 200, {});
            return json(res, 200, { [id]: {
                outputs: { '3': { audio: [{ filename: 'v3_00001_.mp3', subfolder: 'audio', type: 'output' }] } },
                status: { status_str: 'success', completed: true, messages: [] },
            } });
        }
        if (req.url.startsWith('/view')) {
            st.views += 1;
            res.writeHead(200, { 'Content-Type': 'audio/mpeg', 'Content-Length': AUDIO.length });
            return res.end(AUDIO);
        }
        if (req.url === '/interrupt' && req.method === 'POST') {
            st.interrupts.push(JSON.parse(body.toString() || '{}'));
            return json(res, 200, {});
        }
        res.writeHead(404); res.end();
    });
}

/** Backend mock: PUT result / POST error. opts.putStatuses: list consumed per attempt (default 200). */
async function fakeBackend(opts, st) {
    return listen(async (req, res) => {
        if (opts.dropConnections) { req.socket.destroy(); st.puts.push({ dropped: true }); return; }
        const body = await readBody(req);
        const rec = { method: req.method, url: req.url, headers: req.headers, size: body.length, sha: sha(body), text: body.length < 4096 ? body.toString() : null };
        if (req.method === 'PUT') {
            st.puts.push(rec);
            const status = (opts.putStatuses && opts.putStatuses.length) ? opts.putStatuses.shift() : 200;
            return json(res, status, status === 200 ? { status: 200, accepted: true } : { code: `s${status}` });
        }
        if (req.method === 'POST' && req.url.includes('/error')) {
            st.errors.push({ ...rec, body: JSON.parse(body.toString() || '{}') });
            return json(res, 200, { status: 200 });
        }
        res.writeHead(404); res.end();
    });
}

/** Hub mock. opts.v3: 'task' | 'none' | 404 | 503; opts.v2Tasks: v2 tasks queue; opts.cancelWhenCurrent */
async function fakeHub(opts, st, backendPort) {
    let v3Served = false;
    return listen(async (req, res) => {
        const body = await readBody(req);
        const p = req.url.split('?')[0];
        st.hub.push(`${req.method} ${p}`);
        if (p === '/v3/task/next') {
            if (opts.v3 === 404 || opts.v3 === 503) return json(res, opts.v3, { error: 'nope' });
            if (opts.v3 === 'task' && !v3Served) {
                v3Served = true;
                const base = `http://127.0.0.1:${backendPort}/internal/gen/jobs/${JOB}`;
                return json(res, 200, { task: {
                    job_id: JOB, attempt: 1, kind: 'audio_chunk', protocol_version: 3, job_type: 'audio',
                    params: { '3': { class_type: 'SaveAudio', inputs: { filename_prefix: 'audio/v3' } } },
                    timeout_ms: opts.timeoutMs || 60000,
                    upload_token: TOKEN, upload_url: `${base}/result?attempt=1`, error_url: `${base}/error?attempt=1`,
                } });
            }
            return json(res, 200, { task: null });
        }
        if (p === '/v3/beacon') {
            const b = JSON.parse(body.toString() || '{}');
            st.v3beacons.push(b);
            const seen = st.v3beacons.filter((x) => x.current_job_id === JOB).length;
            const cancel = opts.cancelWhenCurrent && b.current_job_id === JOB && seen >= (opts.cancelAfter || 1) ? [JOB] : [];
            return json(res, 200, { cancel });
        }
        if (p === '/task/next') {
            const t = (opts.v2Tasks || []).shift();
            return json(res, 200, t ? { task: t } : { task: null });
        }
        if (p === '/task/result' || p === '/task/error') {
            (p === '/task/result' ? st.v2results : st.v2errors).push(JSON.parse(body.toString() || '{}'));
            return json(res, 200, { ok: true });
        }
        return json(res, 200, { ok: true });   // /beacon
    });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function until(fn, ms) {
    const end = Date.now() + ms;
    while (Date.now() < end) { if (fn()) return true; await sleep(100); }
    return fn();
}

async function scenario(opts, doneWhen, timeoutMs = 20000) {
    const st = { comfy: [], prompts: [], interrupts: [], views: 0, puts: [], errors: [], hub: [], v3beacons: [], v2results: [], v2errors: [] };
    const comfy = await fakeComfy(opts, st);
    const backend = await fakeBackend(opts, st);
    const hub = await fakeHub(opts, st, backend.address().port);
    const dir = copyBundle();
    const outDir = path.join(dir, 'ComfyUI', 'output');
    fs.mkdirSync(path.join(outDir, 'audio'), { recursive: true });
    if (opts.onDisk !== false) fs.writeFileSync(path.join(outDir, 'audio', 'v3_00001_.mp3'), AUDIO);
    const out = { text: '' };
    const child = spawn(process.execPath, ['worker.cjs'], {
        cwd: dir,
        env: {
            PATH: process.env.PATH, HOME: dir,
            ANIMASTOR_WORKER_TOKEN: 'wrk.v3.test',
            WORKER_TYPE: 'audio', WORKER_ID: 'v3-worker',
            ANIMASTOR_API_URL: 'http://127.0.0.1:1/api/v1',
            TASK_SLEEP_MS: '200', BEACON_INTERVAL_MS: String(opts.beaconMs || 400),
            V3_DELIVERY_BACKOFF_MS: '50',
            WORKER_JOURNAL_DIR: path.join(dir, 'journal'),
            COMFY_INPUT_DIR: path.join(dir, 'ComfyUI', 'input'),
            COMFY_URL: `http://127.0.0.1:${comfy.address().port}`,
            HUB_URL: `http://127.0.0.1:${hub.address().port}`,
            ...(opts.env || {}),
        },
    });
    child.stdout.on('data', (d) => { out.text += d; });
    child.stderr.on('data', (d) => { out.text += d; });
    try {
        const ok = await until(() => doneWhen(st, out.text), timeoutMs);
        if (opts.settleMs) await sleep(opts.settleMs);
        return { ok, st, out: out.text, outputLeft: fs.existsSync(path.join(outDir, 'audio', 'v3_00001_.mp3')) };
    } finally {
        child.kill('SIGKILL');
        comfy.close(); backend.close(); hub.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
}

const V2_TASK = {
    job_id: 'b_ch-1_sc-7_0001:audio', build_id: 'build_b', dispatch_id: 'dispatch-1', protocol_version: 2,
    params: { '3': { class_type: 'SaveAudio', inputs: { filename_prefix: 'audio/v3' } } },
};

describe('worker 2.4.0 — protocol 3 cycle', () => {
    it('v3 task → PUT upload_url: raw bytes of the output file, Bearer upload_token, Content-Type/Length; hub /task/result untouched', async () => {
        const r = await scenario({ v3: 'task' }, (st, out) => st.puts.length >= 1 && /Done 7c0d9a3e/.test(out), 20000);
        expect.ok(r.st.puts.length === 1, `PUTs: ${r.st.puts.length}\n${r.out.slice(-1500)}`);
        const put = r.st.puts[0];
        expect.equal(put.url, `/internal/gen/jobs/${JOB}/result?attempt=1`);
        expect.equal(put.headers.authorization, `Bearer ${TOKEN}`);
        expect.equal(put.headers['content-type'], 'audio/mp3');
        expect.equal(Number(put.headers['content-length']), AUDIO.length);
        expect.equal(put.size, AUDIO.length);
        expect.equal(put.sha, sha(AUDIO), 'body is the file byte-for-byte (no base64)');
        expect.notOk(put.headers['transfer-encoding'], 'sized stream, not chunked');
        expect.equal(r.st.v2results.length, 0);
        expect.equal(r.st.hub[0], 'GET /v3/task/next', `first hub call: ${r.st.hub[0]}`);
        expect.equal(r.st.errors.length, 0);
        expect.equal(r.st.prompts.length, 1);
    });

    it('output not on disk → streamed from ComfyUI /view', async () => {
        const r = await scenario({ v3: 'task', onDisk: false }, (st) => st.puts.length >= 1);
        expect.ok(r.ok, r.out.slice(-1500));
        expect.equal(r.st.views, 1);
        expect.equal(r.st.puts[0].sha, sha(AUDIO));
    });

    it('PUT 503, 502 → retried with backoff, third attempt 200', async () => {
        const r = await scenario({ v3: 'task', putStatuses: [503, 502, 200] }, (st, out) => /Done 7c0d9a3e/.test(out));
        expect.ok(r.ok, r.out.slice(-1500));
        expect.equal(r.st.puts.length, 3);
        for (const p of r.st.puts) expect.equal(p.sha, sha(AUDIO), 'every retry re-streams the whole file');
        expect.include(r.out, 'attempt 1/5 failed: HTTP 503');
        expect.include(r.out, '3 attempt(s)');
    });

    it('PUT keeps failing on network errors → exactly 5 attempts, then given up (no error report)', async () => {
        const r = await scenario({ v3: 'task', dropConnections: true }, (st, out) => /failed after 5 attempt/.test(out));
        expect.ok(r.ok, r.out.slice(-1500));
        expect.equal(r.st.puts.length, 5);
        expect.ok(r.outputLeft, 'undelivered output is kept on disk');
    });

    it('PUT 409 → stop at once: one attempt, no retry, no error report', async () => {
        const r = await scenario({ v3: 'task', putStatuses: [409], settleMs: 800 }, (st, out) => /not the current attempt \(409\)/.test(out));
        expect.ok(r.ok, r.out.slice(-1500));
        expect.equal(r.st.puts.length, 1);
        expect.equal(r.st.errors.length, 0);
    });

    it('PUT 401 → stop at once (token), the worker keeps running', async () => {
        const r = await scenario({ v3: 'task', putStatuses: [401], settleMs: 800 }, (st, out) => /upload token rejected \(401\)/.test(out));
        expect.ok(r.ok, r.out.slice(-1500));
        expect.equal(r.st.puts.length, 1);
        expect.notInclude(r.out, 'Worker authentication failed', 'a rejected upload token is not a worker-credential failure');
        expect.ok(r.st.hub.filter((h) => h === 'GET /v3/task/next').length >= 2, 'loop continues');
    });

    it('execution error → POST error_url {error_class, reason, comfy_status} with the upload token', async () => {
        const r = await scenario({ v3: 'task', prompt: 'reject' }, (st) => st.errors.length >= 1);
        expect.ok(r.ok, r.out.slice(-1500));
        const e = r.st.errors[0];
        expect.equal(e.url, `/internal/gen/jobs/${JOB}/error?attempt=1`);
        expect.equal(e.headers.authorization, `Bearer ${TOKEN}`);
        expect.equal(e.body.error_class, 'deterministic');
        expect.equal(e.body.comfy_status, 400);
        expect.include(e.body.reason, 'No prompt_id: invalid_prompt');
        expect.equal(r.st.puts.length, 0);
        expect.equal(r.st.v2errors.length, 0, 'never through the hub');
    });

    it('task.timeout_ms passes → ComfyUI /interrupt + error_class=timeout (no own status change, I3)', async () => {
        const r = await scenario({ v3: 'task', history: 'never', timeoutMs: 1500 }, (st) => st.errors.length >= 1);
        expect.ok(r.ok, r.out.slice(-1500));
        expect.equal(r.st.errors[0].body.error_class, 'timeout');
        expect.ok(r.st.interrupts.length >= 1, 'ComfyUI interrupted');
        expect.equal(r.st.interrupts[0].prompt_id, 'p-1');
        expect.equal(r.st.puts.length, 0);
    });

    it('cancel in the v3 beacon answer → ComfyUI /interrupt, no PUT, no error report, next task', async () => {
        const r = await scenario({ v3: 'task', history: 'never', cancelWhenCurrent: true, settleMs: 1200 },
            (st, out) => st.interrupts.length >= 1 && /cancelled — no upload/.test(out));
        expect.ok(r.ok, r.out.slice(-1500));
        expect.ok(r.st.v3beacons.some((b) => b.current_job_id === JOB), 'beacon reported current_job_id');
        expect.equal(r.st.interrupts[0].prompt_id, 'p-1');
        expect.equal(r.st.puts.length, 0);
        expect.equal(r.st.errors.length, 0);
        const b = r.st.v3beacons[0];
        expect.ok('gpu' in b && 'vram' in b && b.version === '2.4.0', `beacon body: ${JSON.stringify(b)}`);
        // after the cancel the loop goes on: v3 then v2 again
        const after = r.st.hub.slice(r.st.hub.lastIndexOf('GET /v3/task/next'));
        expect.ok(r.st.hub.filter((h) => h === 'GET /v3/task/next').length >= 2, 'next iteration started');
        expect.ok(after.length >= 1);
    });
});

describe('worker 2.4.0 — cancel while ComfyUI runs', () => {
    it('cancel arrives in a later beacon (prompt running) → /interrupt with that prompt_id from the beacon path', async () => {
        const r = await scenario({ v3: 'task', history: 'never', cancelWhenCurrent: true, cancelAfter: 3, settleMs: 600 },
            (st, out) => st.interrupts.length >= 1 && /cancelled — no upload/.test(out));
        expect.ok(r.ok, r.out.slice(-1500));
        expect.ok(r.st.v3beacons.filter((b) => b.current_job_id === JOB).length >= 3);
        expect.equal(r.st.interrupts.length, 1, 'one interrupt');
        expect.equal(r.st.interrupts[0].prompt_id, 'p-1');
        expect.ok(r.st.comfy.filter((c) => c.startsWith('GET /history')).length >= 1, 'was waiting for the result');
        expect.equal(r.st.puts.length + r.st.errors.length, 0);
    });
});

describe('worker 2.4.0 — v2 compatibility', () => {
    it('v3 {task:null} → one v2 pop in the same iteration; the v2 task is served as in 2.3.0', async () => {
        const r = await scenario({ v3: 'none', v2Tasks: [{ ...V2_TASK }] }, (st) => st.v2results.length >= 1);
        expect.ok(r.ok, r.out.slice(-1500));
        const calls = r.st.hub.filter((h) => h.includes('task/next'));
        expect.equal(calls[0], 'GET /v3/task/next');
        expect.equal(calls[1], 'GET /task/next');
        expect.equal(r.st.v2results[0].dispatch_id, 'dispatch-1');
        expect.match(r.st.v2results[0].result_base64, /^data:audio\/mp3;base64,/);
        expect.equal(r.st.puts.length, 0);
    });

    for (const code of [404, 503]) {
        it(`v3 HTTP ${code} (old hub / backend off) → v2 pop; both empty → v3, v2 alternate`, async () => {
            const r = await scenario({ v3: code, v2Tasks: [{ ...V2_TASK }] }, (st) => st.v2results.length >= 1 && st.hub.filter((h) => h.includes('task/next')).length >= 6);
            expect.ok(r.ok, r.out.slice(-1500));
            const calls = r.st.hub.filter((h) => h.includes('task/next'));
            for (let i = 0; i + 1 < calls.length; i += 2) {
                expect.equal(calls[i], 'GET /v3/task/next', `iteration ${i / 2}: ${calls.join(', ')}`);
                expect.equal(calls[i + 1], 'GET /task/next');
            }
            expect.include(r.out, `Hub protocol 3 unavailable (HTTP ${code})`);
            expect.equal((r.out.match(/Hub protocol 3 unavailable/g) || []).length, 1, 'logged once, not per iteration');
        });
    }

    it('WORKER_PROTOCOL=v2only → no /v3 call at all (next nor beacon)', async () => {
        const r = await scenario({ v3: 'task', v2Tasks: [{ ...V2_TASK }], env: { WORKER_PROTOCOL: 'v2only' }, settleMs: 1000 }, (st) => st.v2results.length >= 1);
        expect.ok(r.ok, r.out.slice(-1500));
        expect.equal(r.st.hub.filter((h) => h.startsWith('GET /v3') || h.startsWith('POST /v3')).length, 0, r.st.hub.join(', '));
        expect.equal(r.st.puts.length, 0);
        expect.include(r.out, 'Protocol mode: v2only');
    });

    it('mode both: v3 beacon runs next to the v2 beacon', async () => {
        const r = await scenario({ v3: 'none', settleMs: 1000 }, (st) => st.v3beacons.length >= 2);
        expect.ok(r.ok, r.out.slice(-1500));
        expect.ok(r.st.hub.includes('POST /beacon'), 'v2 heartbeat still sent');
        expect.equal(r.st.v3beacons[0].current_job_id, null);
    });
});
