// ======================================================
// animastor-worker — hub 409 body in the log (bundle 2.3.0, 2026-11-04)
// ======================================================
// reports/2026-11-04/regenerate-sweep-fix.md: the worker logged only
// «Hub rejected result: HTTP 409» — no_running_record and dispatch_mismatch
// were indistinguishable. Hub v0.2.9 answers 409 with a JSON body
// (error, detail, job_id, dispatch_id); 2.3.0 logs it (capped) and carries it
// in the task/error reason. Spawns worker.cjs against a fake ComfyUI that
// finishes the prompt with a SaveImage output (file on disk) and a fake hub
// that rejects /task/result (and /task/error) with 409 + body.

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


async function fakeComfy() {
    return listen(async (req, res) => {
        if (req.url === '/system_stats') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end('{"system":{},"devices":[]}');
        }
        if (req.url === '/prompt' && req.method === 'POST') {
            await readBody(req);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end('{"prompt_id":"p-1","number":1,"node_errors":{}}');
        }
        if (req.url === '/history/p-1') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ 'p-1': {
                outputs: { '9': { images: [{ filename: 'out_00001_.png', subfolder: '', type: 'output' }] } },
                status: { status_str: 'success', completed: true, messages: [] },
            } }));
        }
        res.writeHead(404); res.end();
    });
}

const HUB_409 = (job, disp, detail) => JSON.stringify({ error: 'stale_or_unknown_dispatch', detail, job_id: job, dispatch_id: disp });

async function fakeHub(state) {
    return listen(async (req, res) => {
        const body = await readBody(req);
        if (req.url.startsWith('/task/next')) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            const task = state.tasks.shift();
            return res.end(JSON.stringify(task ? { task } : { task: null }));
        }
        if (req.url === '/task/result' || req.url === '/task/error') {
            const b = JSON.parse(body || '{}');
            (req.url === '/task/result' ? state.results : state.errors).push(b);
            res.writeHead(409, { 'Content-Type': 'application/json' });
            return res.end(HUB_409(b.job_id, b.dispatch_id, state.detail));
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

async function runOnce(detail) {
    const comfy = await fakeComfy();
    const hubState = {
        detail, results: [], errors: [],
        tasks: [{ job_id: 'b_ch-1_sc-7_iu-677669ab:iu_image', build_id: 'build_b', dispatch_id: 'dispatch-1', protocol_version: 2,
            params: { '9': { class_type: 'SaveImage', inputs: { filename_prefix: 'out' } } } }],
    };
    const hub = await fakeHub(hubState);
    const dir = copyBundle();
    const outDir = path.join(dir, 'ComfyUI', 'output');
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(path.join(outDir, 'out_00001_.png'), Buffer.from('89504e470d0a1a0a', 'hex'));
    const out = { text: '' };
    const child = spawn(process.execPath, ['worker.cjs'], {
        cwd: dir,
        env: {
            PATH: process.env.PATH, HOME: dir,
            ANIMASTOR_WORKER_TOKEN: 'wrk.hub409.test',
            WORKER_TYPE: 'image', WORKER_ID: 'hub409-worker',
            ANIMASTOR_API_URL: 'http://127.0.0.1:1/api/v1',
            TASK_SLEEP_MS: '200', WORKER_JOURNAL_DIR: path.join(dir, 'journal'),
            COMFY_INPUT_DIR: path.join(dir, 'ComfyUI', 'input'),
            COMFY_OUTPUT_DIR: outDir,
            COMFY_URL: `http://127.0.0.1:${comfy.address().port}`,
            HUB_URL: `http://127.0.0.1:${hub.address().port}`,
            RESULT_TIMEOUT_MS: '60000',
        },
    });
    child.stdout.on('data', (d) => { out.text += d; });
    child.stderr.on('data', (d) => { out.text += d; });
    try {
        const ok = await until(() => hubState.errors.length >= 1 && /Failed to send error to hub/.test(out.text), 15000);
        return { ok, hub: hubState, out: out.text };
    } finally {
        child.kill('SIGKILL');
        comfy.close(); hub.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
}

describe('worker — hub 409 body is logged (2.3.0)', () => {
    it('result 409 no_running_record → the log shows the hub body (detail, ids), task/error reason carries it', async () => {
        const r = await runOnce('no_running_record');
        expect.ok(r.ok, `no task/error: ${r.out.slice(-1200)}`);
        expect.equal(r.hub.results.length, 1);
        expect.include(r.out, 'Hub rejected result: HTTP 409');
        expect.include(r.out, '"detail":"no_running_record"');
        expect.include(r.out, '"dispatch_id":"dispatch-1"');
        expect.include(r.hub.errors[0].reason, '"detail":"no_running_record"');
        // the follow-up task/error rejection is logged with its body too
        expect.include(r.out, 'Hub rejected task error: HTTP 409');
    });

    it('dispatch_mismatch is distinguishable in the log', async () => {
        const r = await runOnce('dispatch_mismatch');
        expect.ok(r.ok, `no task/error: ${r.out.slice(-1200)}`);
        expect.include(r.out, '"detail":"dispatch_mismatch"');
    });
});
