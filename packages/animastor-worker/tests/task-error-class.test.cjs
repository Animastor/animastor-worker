// ======================================================
// animastor-worker — task/error structured class (bundle 2.2.3, 2026-11-02)
// ======================================================
// GPU retry policy (owner decision 2026-11-02): POST /task/error carries the
// optional `error_class` (deterministic | transient | oom) and `comfy_status`
// next to the unchanged `reason` text. Spawns worker.cjs against a fake
// ComfyUI and a fake hub:
//   · ComfyUI accepts the prompt, history reports execution_error
//     «CUDA out of memory» → error_class "oom", reported right away (no
//     polling until the result timeout);
//   · another execution error → no error_class (backend: unknown, no retry);
//   · ComfyUI /prompt answers 500 → error_class "transient", comfy_status 500.

const { spawn } = require('child_process');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const { describe, it, expect } = require('./harness.cjs');

const BUNDLE_DIR = path.join(__dirname, '..', 'worker');

function copyBundle() {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'worker-errclass-'));
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
        let b = '';
        req.on('data', (c) => { b += c; });
        req.on('end', () => resolve(b));
    });
}

async function fakeComfy(mode) {
    return listen(async (req, res) => {
        if (req.url === '/system_stats') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end('{"system":{},"devices":[]}');
        }
        if (req.url === '/prompt' && req.method === 'POST') {
            await readBody(req);
            if (mode.promptStatus) {
                res.writeHead(mode.promptStatus, { 'Content-Type': 'text/html' });
                return res.end('<html>upstream error</html>');
            }
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end('{"prompt_id":"p-1","number":1,"node_errors":{}}');
        }
        if (req.url === '/history/p-1') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ 'p-1': {
                outputs: {},
                status: { status_str: 'error', completed: false, messages: [
                    ['execution_start', { prompt_id: 'p-1' }],
                    ['execution_error', { prompt_id: 'p-1', node_type: 'KSampler',
                        exception_type: mode.exceptionType, exception_message: mode.exceptionMessage }],
                ] },
            } }));
        }
        res.writeHead(404); res.end();
    });
}

async function fakeHub(state) {
    return listen(async (req, res) => {
        const body = await readBody(req);
        if (req.url.startsWith('/task/next')) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            const task = state.tasks.shift();
            return res.end(JSON.stringify(task ? { task } : { task: null }));
        }
        if (req.url === '/task/error') {
            state.errors.push(JSON.parse(body || '{}'));
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end('{"ok":true}');
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end('{"ok":true}');
    });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function until(fn, ms) {
    const end = Date.now() + ms;
    while (Date.now() < end) { if (fn()) return true; await sleep(100); }
    return fn();
}

async function runOnce(mode) {
    const comfy = await fakeComfy(mode);
    const hubState = {
        errors: [],
        tasks: [{ job_id: 'b_ch-1_sc-1:scene_image', build_id: 'build_b', dispatch_id: 'dispatch-1', protocol_version: 2,
            params: { '9': { class_type: 'SaveImage', inputs: {} } } }],
    };
    const hub = await fakeHub(hubState);
    const dir = copyBundle();
    const out = { text: '' };
    const child = spawn(process.execPath, ['worker.cjs'], {
        cwd: dir,
        env: {
            PATH: process.env.PATH, HOME: dir,
            ANIMASTOR_WORKER_TOKEN: 'wrk.errclass.test',
            WORKER_TYPE: 'image', WORKER_ID: 'errclass-worker',
            ANIMASTOR_API_URL: 'http://127.0.0.1:1/api/v1',
            TASK_SLEEP_MS: '200', WORKER_JOURNAL_DIR: path.join(dir, 'journal'),
            COMFY_INPUT_DIR: path.join(dir, 'ComfyUI', 'input'),
            COMFY_OUTPUT_DIR: path.join(dir, 'ComfyUI', 'output'),
            COMFY_URL: `http://127.0.0.1:${comfy.address().port}`,
            HUB_URL: `http://127.0.0.1:${hub.address().port}`,
            // A result-timeout fallback would show up as "Timeout waiting result".
            RESULT_TIMEOUT_MS: '60000',
        },
    });
    child.stdout.on('data', (d) => { out.text += d; });
    child.stderr.on('data', (d) => { out.text += d; });
    try {
        const ok = await until(() => hubState.errors.length >= 1, 15000);
        return { ok, err: hubState.errors[0], out: out.text };
    } finally {
        child.kill('SIGKILL');
        comfy.close(); hub.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
}

describe('worker — task/error error_class (2.2.3)', () => {
    it('CUDA OOM execution error → error_class oom, reported without waiting for the result timeout', async () => {
        const t0 = Date.now();
        const r = await runOnce({ exceptionType: 'torch.cuda.OutOfMemoryError', exceptionMessage: 'CUDA out of memory. Tried to allocate 2.00 GiB' });
        expect.ok(r.ok, `no task/error: ${r.out.slice(-800)}`);
        expect.equal(r.err.error_class, 'oom');
        expect.include(r.err.reason, 'ComfyUI execution error');
        expect.include(r.err.reason, 'CUDA out of memory');
        expect.equal(r.err.comfy_status, undefined);
        expect.ok(Date.now() - t0 < 15000, 'reported before the 60 s result timeout');
    });

    it('other execution error → no error_class (backend: unknown → no retry)', async () => {
        const r = await runOnce({ exceptionType: 'RuntimeError', exceptionMessage: 'shape mismatch' });
        expect.ok(r.ok, `no task/error: ${r.out.slice(-800)}`);
        expect.equal(r.err.error_class, undefined);
        expect.include(r.err.reason, 'RuntimeError: KSampler: shape mismatch');
    });

    it('ComfyUI /prompt HTTP 500 → error_class transient, comfy_status 500', async () => {
        const r = await runOnce({ promptStatus: 500 });
        expect.ok(r.ok, `no task/error: ${r.out.slice(-800)}`);
        expect.equal(r.err.error_class, 'transient');
        expect.equal(r.err.comfy_status, 500);
        expect.include(r.err.reason, 'Invalid JSON from ComfyUI');
    });
});
