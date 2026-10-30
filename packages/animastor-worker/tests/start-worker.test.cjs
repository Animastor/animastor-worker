// ======================================================
// animastor-worker — start-worker.sh behaviour (package-owned)
// ======================================================
// Runs the real start-worker.sh against a throwaway install layout
// (<tmp>/start-worker.sh + <tmp>/worker/<bundle>) and a fake ComfyUI HTTP
// server on 127.0.0.1. Checks: worker type comes from .env (argument only
// overrides), the plain URL is preferred over the notebook prefix, the
// prefix is used only when ComfyUI answers there, and READY is printed only
// after the worker logged "ComfyUI ready". The hub/backend point at a closed
// port — no network egress.

const { spawn, execFileSync } = require('child_process');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const { describe, it, expect } = require('./harness.cjs');

const PKG_DIR = path.join(__dirname, '..');
const BUNDLE_DIR = path.join(PKG_DIR, 'worker');
const HAS_BASH = (() => { try { execFileSync('bash', ['-c', 'command -v curl pgrep setsid >/dev/null']); return true; } catch (_) { return false; } })();

function layout(envLines) {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'start-worker-'));
    fs.copyFileSync(path.join(PKG_DIR, 'start-worker.sh'), path.join(tmp, 'start-worker.sh'));
    fs.chmodSync(path.join(tmp, 'start-worker.sh'), 0o755);
    fs.mkdirSync(path.join(tmp, 'worker'));
    for (const f of fs.readdirSync(BUNDLE_DIR)) {
        if (f === 'node_modules' || f === '.env') continue;
        fs.copyFileSync(path.join(BUNDLE_DIR, f), path.join(tmp, 'worker', f));
    }
    fs.writeFileSync(path.join(tmp, 'worker', '.env'), envLines.join('\n') + '\n', { mode: 0o600 });
    return tmp;
}

/** Fake ComfyUI: answers GET <prefix>/system_stats with 200, else 404. */
function fakeComfy(prefix) {
    return new Promise((resolve) => {
        const srv = http.createServer((req, res) => {
            if (req.url === `${prefix}/system_stats`) {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ system: { os: 'posix' }, devices: [] }));
            } else {
                res.writeHead(404); res.end();
            }
        });
        srv.listen(0, '127.0.0.1', () => resolve(srv));
    });
}

function run(dir, args = [], extraEnv = {}) {
    return new Promise((resolve) => {
        const child = spawn('bash', [path.join(dir, 'start-worker.sh'), ...args], {
            cwd: dir,
            env: {
                PATH: `${path.dirname(process.execPath)}:${process.env.PATH}`,
                HOME: dir,
                COMFY_WAIT_SECS: '6',
                READY_WAIT_SECS: '20',
                ...extraEnv,
            },
        });
        let out = '';
        child.stdout.on('data', (d) => { out += d; });
        child.stderr.on('data', (d) => { out += d; });
        child.on('close', (code) => resolve({ code, out }));
    });
}

function stopWorker(dir) {
    try {
        const pid = Number(fs.readFileSync(path.join(dir, 'logs', 'worker.pid'), 'utf8').trim());
        if (pid) process.kill(-pid, 'SIGKILL');
    } catch (_) { /* not started / already gone */ }
}

function baseEnv(port, extra = []) {
    return [
        'ANIMASTOR_WORKER_TOKEN=wrk.startworker.test',
        'HUB_URL=http://127.0.0.1:1/gpu',
        'ANIMASTOR_API_URL=http://127.0.0.1:1/api/v1',
        `COMFY_PORT=${port}`,
        `NODE_BIN=${process.execPath}`,
        ...extra,
    ];
}

describe('start-worker.sh', () => {
    if (!HAS_BASH) {
        it('skipped — bash/curl/pgrep/setsid not available', async () => {});
        return;
    }

    it('takes the worker type from .env and reports READY only after "ComfyUI ready"', async () => {
        const srv = await fakeComfy('');
        const dir = layout(baseEnv(srv.address().port, ['WORKER_TYPE=audio']));
        try {
            const r = await run(dir);
            expect.equal(r.code, 0, r.out);
            expect.include(r.out, 'Worker type: audio');
            expect.notInclude(r.out, 'command-line override');
            expect.include(r.out, 'READY: audio worker');
            const log = fs.readFileSync(path.join(dir, 'logs', 'worker.log'), 'utf8');
            expect.include(log, 'ComfyUI ready');
            expect.ok(r.out.indexOf('READY') > -1);
        } finally {
            stopWorker(dir); srv.close(); fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('the command-line argument overrides the .env type', async () => {
        const srv = await fakeComfy('');
        const dir = layout(baseEnv(srv.address().port, ['WORKER_TYPE=audio']));
        try {
            const r = await run(dir, ['video']);
            expect.equal(r.code, 0, r.out);
            expect.include(r.out, 'Worker type: video (command-line override)');
        } finally {
            stopWorker(dir); srv.close(); fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('fails with a clear cause when WORKER_TYPE is neither in .env nor passed', async () => {
        const dir = layout(baseEnv(1));
        try {
            const r = await run(dir);
            expect.equal(r.code, 1, r.out);
            expect.include(r.out, 'WORKER_TYPE is not set');
            expect.notInclude(r.out, 'READY');
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('prefers the plain URL when ComfyUI answers without the notebook prefix', async () => {
        const srv = await fakeComfy('');
        const port = srv.address().port;
        const dir = layout(baseEnv(port, ['WORKER_TYPE=audio', 'NOTEBOOK_PATH=/notebook/user/x']));
        try {
            const r = await run(dir);
            expect.equal(r.code, 0, r.out);
            expect.include(r.out, `ComfyUI: http://127.0.0.1:${port}\n`);
        } finally {
            stopWorker(dir); srv.close(); fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('uses the notebook prefix only when ComfyUI answers there', async () => {
        const srv = await fakeComfy('/notebook/user/x');
        const port = srv.address().port;
        const dir = layout(baseEnv(port, ['WORKER_TYPE=audio', 'NOTEBOOK_PATH=/notebook/user/x']));
        try {
            const r = await run(dir);
            expect.equal(r.code, 0, r.out);
            expect.include(r.out, `ComfyUI: http://127.0.0.1:${port}/notebook/user/x`);
            expect.include(r.out, 'READY: audio worker');
        } finally {
            stopWorker(dir); srv.close(); fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('times out with the URLs tried and no READY when ComfyUI never answers', async () => {
        const srv = await fakeComfy('/elsewhere');
        const dir = layout(baseEnv(srv.address().port, ['WORKER_TYPE=audio']));
        try {
            const r = await run(dir, [], { COMFY_WAIT_SECS: '3' });
            expect.equal(r.code, 1, r.out);
            expect.include(r.out, 'ComfyUI API did not answer');
            expect.include(r.out, 'Fix:');
            expect.notInclude(r.out, 'READY');
        } finally {
            srv.close(); fs.rmSync(dir, { recursive: true, force: true });
        }
    });
});
