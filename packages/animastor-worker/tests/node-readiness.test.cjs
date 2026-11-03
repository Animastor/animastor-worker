// ======================================================
// animastor-worker — node-class readiness (bundle 2.2.2, GPU E2E 2026-11-02)
// ======================================================
// Spawns worker.cjs against a fake ComfyUI (system_stats + object_info +
// prompt) and a fake hub (beacon / task/next / task/error). Backend verify
// points at a closed port (non-fatal). Checks:
//   · classes missing → no /task/next, ONE "Not taking tasks" line naming
//     the missing classes, re-check with backoff;
//   · classes appear  → "Node classes ready", /task/next starts;
//   · no class list   → old behaviour (ComfyUI API only);
//   · ComfyUI rejects a prompt → the error carries ComfyUI's reason and the
//     gate runs again before the next /task/next.

const { spawn } = require('child_process');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const { describe, it, expect } = require('./harness.cjs');

const BUNDLE_DIR = path.join(__dirname, '..', 'worker');

function copyBundle(extraFiles = {}) {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'worker-readiness-'));
    for (const f of fs.readdirSync(BUNDLE_DIR)) {
        if (f === 'node_modules' || f === '.env') continue;
        fs.copyFileSync(path.join(BUNDLE_DIR, f), path.join(tmp, f));
    }
    for (const [name, body] of Object.entries(extraFiles)) fs.writeFileSync(path.join(tmp, name), body);
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

async function fakeComfy(state) {
    return listen(async (req, res) => {
        if (req.url === '/system_stats') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end('{"system":{},"devices":[]}');
        }
        if (req.url === '/object_info') {
            state.objectInfoCalls++;
            const info = {};
            for (const c of state.classes) info[c] = { input: {} };
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify(info));
        }
        if (req.url === '/prompt' && req.method === 'POST') {
            await readBody(req);
            state.promptCalls++;
            res.writeHead(400, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ error: { type: 'invalid_prompt', message: 'Cannot execute because node FA_Qwen3TTSVoiceDesign does not exist.' }, node_errors: {} }));
        }
        res.writeHead(404); res.end();
    });
}

async function fakeHub(state) {
    return listen(async (req, res) => {
        const body = await readBody(req);
        if (req.url.startsWith('/task/next')) {
            state.nextCalls++;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            const task = state.tasks.shift();
            return res.end(JSON.stringify(task ? { task } : { task: null }));
        }
        if (req.url === '/task/error') {
            state.errors.push(JSON.parse(body || '{}'));
            if (state.onError) state.onError();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end('{"ok":true}');
        }
        if (req.url === '/beacon') {
            state.beacons++;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end('{"ok":true}');
        }
        res.writeHead(404); res.end();
    });
}

function startWorker(dir, env) {
    const out = { text: '' };
    const child = spawn(process.execPath, ['worker.cjs'], {
        cwd: dir,
        env: {
            PATH: process.env.PATH, HOME: dir,
            ANIMASTOR_WORKER_TOKEN: 'wrk.readiness.test',
            WORKER_TYPE: 'audio', WORKER_ID: 'readiness-worker',
            ANIMASTOR_API_URL: 'http://127.0.0.1:1/api/v1',
            NODE_CLASS_RECHECK_MIN_MS: '300', NODE_CLASS_RECHECK_MAX_MS: '600',
            TASK_SLEEP_MS: '200', WORKER_JOURNAL_DIR: path.join(dir, 'journal'),
            COMFY_INPUT_DIR: path.join(dir, 'ComfyUI', 'input'),
            ...env,
        },
    });
    child.stdout.on('data', (d) => { out.text += d; });
    child.stderr.on('data', (d) => { out.text += d; });
    return { child, out };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function until(fn, ms) {
    const end = Date.now() + ms;
    while (Date.now() < end) { if (fn()) return true; await sleep(100); }
    return fn();
}

describe('worker — node-class readiness (2.2.2)', () => {
    it('missing classes → no task polling, one reason line; classes appear → ready and polling starts', async () => {
        const comfyState = { classes: ['SaveAudioMP3'], objectInfoCalls: 0, promptCalls: 0 };
        const hubState = { nextCalls: 0, beacons: 0, errors: [], tasks: [] };
        const comfy = await fakeComfy(comfyState);
        const hub = await fakeHub(hubState);
        const dir = copyBundle({
            'node-classes.json': JSON.stringify({ profiles: {
                'audio/qwen-tts': ['FA_Qwen3TTSVoiceDesign', 'SaveAudioMP3'],
                'image/flux': ['NotForAudio'],
            } }),
        });
        const { child, out } = startWorker(dir, {
            COMFY_URL: `http://127.0.0.1:${comfy.address().port}`,
            HUB_URL: `http://127.0.0.1:${hub.address().port}`,
        });
        try {
            expect.ok(await until(() => out.text.includes('Not taking tasks'), 12000), `no reason line: ${out.text.slice(-800)}`);
            expect.ok(await until(() => comfyState.objectInfoCalls >= 4, 6000), 're-checked with backoff');
            expect.equal(hubState.nextCalls, 0, 'no /task/next while classes are missing');
            expect.equal(hubState.beacons, 0, 'no heartbeat while not ready');
            const reasonLines = out.text.split('\n').filter((l) => l.includes('Not taking tasks'));
            expect.lengthOf(reasonLines, 1, 'one line per distinct reason, not per attempt');
            expect.include(reasonLines[0], 'FA_Qwen3TTSVoiceDesign');
            expect.include(reasonLines[0], 'audio/qwen-tts');
            expect.notInclude(reasonLines[0], 'NotForAudio', 'profiles of other worker types are ignored');

            comfyState.classes.push('FA_Qwen3TTSVoiceDesign');
            expect.ok(await until(() => out.text.includes('Node classes ready: 2/2'), 6000), 'ready after the classes appear');
            expect.ok(await until(() => hubState.nextCalls > 0, 6000), 'task polling starts once ready');
        } finally {
            child.kill('SIGKILL');
            comfy.close(); hub.close();
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('REQUIRED_NODE_CLASSES env wins; no list at all keeps ComfyUI-only readiness', async () => {
        const comfyState = { classes: [], objectInfoCalls: 0, promptCalls: 0 };
        const hubState = { nextCalls: 0, beacons: 0, errors: [], tasks: [] };
        const comfy = await fakeComfy(comfyState);
        const hub = await fakeHub(hubState);
        const dir = copyBundle();
        const { child, out } = startWorker(dir, {
            COMFY_URL: `http://127.0.0.1:${comfy.address().port}`,
            HUB_URL: `http://127.0.0.1:${hub.address().port}`,
        });
        try {
            expect.ok(await until(() => hubState.nextCalls > 0, 12000), `polls without a list: ${out.text.slice(-600)}`);
            const noList = out.text.split('\n').filter((l) => l.includes('readiness = ComfyUI API only'));
            expect.lengthOf(noList, 1, 'one line when node-classes.json is absent');
            expect.match(noList[0], /warn/i, 'the missing list is a warning');
            expect.equal(comfyState.objectInfoCalls, 0);
        } finally {
            child.kill('SIGKILL');
        }
        const run2 = startWorker(dir, {
            COMFY_URL: `http://127.0.0.1:${comfy.address().port}`,
            HUB_URL: `http://127.0.0.1:${hub.address().port}`,
            REQUIRED_NODE_CLASSES: 'OnlyFromEnv',
        });
        try {
            hubState.nextCalls = 0;
            expect.ok(await until(() => run2.out.text.includes('Not taking tasks'), 12000));
            expect.include(run2.out.text, 'OnlyFromEnv');
            expect.include(run2.out.text, 'REQUIRED_NODE_CLASSES');
            expect.equal(hubState.nextCalls, 0);
        } finally {
            run2.child.kill('SIGKILL');
            comfy.close(); hub.close();
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('a rejected prompt reports ComfyUI\'s reason and re-runs the gate before the next task', async () => {
        const comfyState = { classes: ['SaveAudioMP3'], objectInfoCalls: 0, promptCalls: 0 };
        const hubState = {
            nextCalls: 0, beacons: 0, errors: [],
            tasks: [{ job_id: 'b_ch-1_sc-1_0001:audio', build_id: 'build_b', dispatch_id: 'dispatch-1', protocol_version: 2, params: { '1': { class_type: 'SaveAudioMP3', inputs: {} } } }],
        };
        const comfy = await fakeComfy(comfyState);
        const hub = await fakeHub(hubState);
        let callsAtError = null;
        hubState.onError = () => { callsAtError = comfyState.objectInfoCalls; };
        const dir = copyBundle();
        const { child, out } = startWorker(dir, {
            COMFY_URL: `http://127.0.0.1:${comfy.address().port}`,
            HUB_URL: `http://127.0.0.1:${hub.address().port}`,
            REQUIRED_NODE_CLASSES: 'SaveAudioMP3',
        });
        try {
            expect.ok(await until(() => hubState.errors.length === 1, 12000), `task error not sent: ${out.text.slice(-800)}`);
            expect.include(hubState.errors[0].reason || hubState.errors[0].error || JSON.stringify(hubState.errors[0]), 'No prompt_id: invalid_prompt');
            // 2.2.3: structured fields for the backend retry policy.
            expect.equal(hubState.errors[0].error_class, 'deterministic');
            expect.equal(hubState.errors[0].comfy_status, 400);
            expect.equal(comfyState.promptCalls, 1);
            expect.ok(await until(() => comfyState.objectInfoCalls > callsAtError, 6000), 'gate re-run after the rejected prompt');
        } finally {
            child.kill('SIGKILL');
            comfy.close(); hub.close();
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });
});
