// ======================================================
// animastor-worker — fresh logs (bundle 2.4.1, ORCHESTRATION_V2 §8.5)
// ======================================================
// Covers the three pieces of worker-logs.cjs that the feature promises:
//   - redaction: one test per rule (Bearer, Authorization, upload_token,
//     key=/token=/secret=/password= + 32+ chars, .env content, a registered
//     concrete secret);
//   - the bounded ring: never more than 2000 lines / 256 KiB, drop-oldest;
//   - the shipper: only new lines per tick, gzip JSON ≤ 64 KiB/body, silent
//     on failure with the lines kept for the next attempt, a one-shot full
//     flush (send_log_tail), no overlap, and a fire-and-forget call site that
//     cannot block the task loop;
//   - ComfyUI stderr capture (when the worker itself started it).

const { spawn } = require('child_process');
const zlib = require('zlib');
const path = require('path');
const fs = require('fs');
const { describe, it, expect } = require('./harness.cjs');

const logs = require(path.join(__dirname, '..', 'worker', 'worker-logs.cjs'));
const { redact, createLogBuffer, createLogShipper, captureStream } = logs;

const HUB = 'https://hub.test/gpu';
const TOKEN = 'wrk.abcdefgh.secretvalue';

function deferred() {
    let resolve, reject;
    const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
    return { promise, resolve, reject };
}

describe('redaction — one rule per test', () => {
    it('Bearer values are replaced', () => {
        const out = redact('Authorization header: Bearer abc.def.ghi-123');
        expect.notInclude(out, 'abc.def.ghi-123');
        expect.include(out, '[redacted]');
    });

    it('Authorization: <value> header dumps are replaced', () => {
        const out = redact('Authorization: v1.SUPERSECRETVALUE123');
        expect.notInclude(out, 'SUPERSECRETVALUE123');
        expect.include(out, 'Authorization: [redacted]');
    });

    it('upload_token values are replaced (JSON and KV forms)', () => {
        const a = redact('{"upload_token":"v1.1999999999.deadbeef"}');
        expect.notInclude(a, 'v1.1999999999.deadbeef');
        expect.include(a, 'upload_token');
        const b = redact('upload_token=v1.1999999999.cafebabe');
        expect.notInclude(b, 'v1.1999999999.cafebabe');
    });

    it('key/token/secret/password = [A-Za-z0-9_-]{32,} is replaced', () => {
        for (const name of ['key', 'token', 'secret', 'password']) {
            const secret = 'A'.repeat(40);
            const out = redact(`${name}=${secret}`);
            expect.notInclude(out, secret, `rule must redact ${name}=`);
            expect.include(out, `${name}=[redacted]`);
        }
        const json = redact('{"secret":"' + 'B'.repeat(48) + '"}');
        expect.notInclude(json, 'B'.repeat(48));
    });

    it('.env content (NAME=VALUE lines) is replaced', () => {
        const env = [
            'ANIMASTOR_WORKER_TOKEN=wrk.c29tZWlk.abcdefgh',
            'WORKER_TYPE=audio',
            'HUB_URL=https://animastor.org/gpu',
        ].join('\n');
        const out = redact(`dumping .env:\n${env}`);
        expect.notInclude(out, 'wrk.c29tZWlk.abcdefgh');
        expect.notInclude(out, 'WORKER_TYPE=audio');
        expect.notInclude(out, 'https://animastor.org/gpu');
        expect.include(out, 'ANIMASTOR_WORKER_TOKEN=[redacted]');
        expect.include(out, 'WORKER_TYPE=[redacted]');
    });

    it('a registered concrete secret value is scrubbed anywhere', () => {
        const out = redact(`pre ${TOKEN} post`, [TOKEN]);
        expect.notInclude(out, TOKEN);
        expect.include(out, '[redacted]');
    });

    it('leaves ordinary diagnostics untouched', () => {
        const line = 'ComfyUI ready at http://127.0.0.1:8188 (attempt 3)';
        expect.equal(redact(line), line);
    });
});

describe('ring buffer — bounded, drop-oldest', () => {
    it('never exceeds the line cap (2000)', () => {
        const b = createLogBuffer();
        for (let i = 0; i < 2500; i++) b.append('info', `line ${i}`);
        expect.equal(b.count(), logs.MAX_LINES);
        expect.equal(b.all()[0].msg, 'line 500');
        expect.equal(b.all()[b.count() - 1].msg, 'line 2499');
    });

    it('never exceeds the byte cap (256 KiB), independent of the line cap', () => {
        const b = createLogBuffer();
        for (let i = 0; i < 4000; i++) b.append('info', 'x'.repeat(1024));
        expect.lessThan(b.sizeBytes(), logs.MAX_BYTES + 1);
        expect.lessThan(b.count(), logs.MAX_LINES + 1);
    });

    it('respects a tiny injected byte cap', () => {
        const b = createLogBuffer({ maxLines: 1000, maxBytes: 200 });
        for (let i = 0; i < 50; i++) b.append('info', 'y'.repeat(50));
        expect.lessThan(b.sizeBytes(), 201);
    });

    it('since(seq) returns only newer lines, with monotonic seq', () => {
        const b = createLogBuffer();
        b.append('info', 'a');
        b.append('warn', 'b');
        const upTo = b.lastSeq();
        b.append('error', 'c');
        const fresh = b.since(upTo);
        expect.equal(fresh.length, 1);
        expect.equal(fresh[0].msg, 'c');
        expect.ok(fresh[0].seq > upTo);
    });
});

describe('shipper — incremental, gzip, ≤ 64 KiB, silent on failure', () => {
    function fakeHub({ fail = false } = {}) {
        const posts = [];
        const impl = async (url, options) => {
            posts.push({ url, options });
            if (fail) return { ok: false, status: 500 };
            return { ok: true, status: 200 };
        };
        return { posts, impl };
    }

    it('sends only new lines, gzip JSON {worker_id, seq_from, seq_to, lines}', async () => {
        const b = createLogBuffer();
        b.append('info', 'first');
        const hub = fakeHub();
        const s = createLogShipper({ buffer: b, hubUrl: HUB, token: TOKEN, workerId: 'w-1', fetchImpl: hub.impl });

        const r1 = await s.shipOnce();
        expect.equal(r1.sent, 1);
        b.append('warn', 'second');
        const r2 = await s.shipOnce();
        expect.equal(r2.sent, 1);

        expect.equal(hub.posts.length, 2);
        const first = JSON.parse(zlib.gunzipSync(hub.posts[0].options.body).toString('utf8'));
        expect.equal(first.worker_id, 'w-1');
        expect.equal(first.seq_from, 1);
        expect.equal(first.seq_to, 1);
        expect.equal(first.lines.length, 1);
        expect.equal(first.lines[0].msg, 'first');
        const second = JSON.parse(zlib.gunzipSync(hub.posts[1].options.body).toString('utf8'));
        expect.equal(second.seq_from, 2);
        expect.equal(second.lines[0].msg, 'second');
        expect.equal(hub.posts[0].url, `${HUB}/v3/logs`);
        expect.equal(hub.posts[0].options.headers.Authorization, `Bearer ${TOKEN}`);
        expect.equal(hub.posts[0].options.headers['Content-Encoding'], 'gzip');
    });

    it('does not POST when there are no new lines', async () => {
        const b = createLogBuffer();
        const hub = fakeHub();
        const s = createLogShipper({ buffer: b, hubUrl: HUB, token: TOKEN, workerId: 'w', fetchImpl: hub.impl });
        const r = await s.shipOnce();
        expect.equal(r.sent, 0);
        expect.equal(hub.posts.length, 0);
        b.append('info', 'x');
        await s.shipOnce();
        await s.shipOnce();   // nothing new
        expect.equal(hub.posts.length, 1);
    });

    it('a failed POST is silent and keeps the lines for the next attempt', async () => {
        const b = createLogBuffer();
        b.append('info', 'keep me');
        let fail = true;
        const posts = [];
        const impl = async (url, options) => { posts.push(options.body); return fail ? { ok: false } : { ok: true }; };
        const s = createLogShipper({ buffer: b, hubUrl: HUB, token: TOKEN, workerId: 'w', fetchImpl: impl });

        const r1 = await s.shipOnce();
        expect.notOk(r1.ok);
        expect.equal(s.lastAckedSeq(), 0);
        fail = false;
        const r2 = await s.shipOnce();
        expect.equal(r2.sent, 1);
        expect.equal(posts.length, 2);
        const body = JSON.parse(zlib.gunzipSync(posts[1]).toString('utf8'));
        expect.equal(body.lines[0].msg, 'keep me');
    });

    it('a network error / timeout is silent too', async () => {
        const b = createLogBuffer();
        b.append('info', 'x');
        const s = createLogShipper({
            buffer: b, hubUrl: HUB, token: TOKEN, workerId: 'w', timeoutMs: 20,
            fetchImpl: () => new Promise((_, reject) => setTimeout(() => reject(new Error('aborted')), 10)),
        });
        const r = await s.shipOnce();
        expect.notOk(r.ok);
        expect.equal(s.lastAckedSeq(), 0);
    });

    it('every POST body stays ≤ maxBytes after gzip; the rest goes next time', async () => {
        const b = createLogBuffer();
        for (let i = 0; i < 20; i++) b.append('info', `line-${i}-` + 'z'.repeat(200));
        const bodies = [];
        const s = createLogShipper({
            buffer: b, hubUrl: HUB, token: TOKEN, workerId: 'w', maxBytes: 1200,
            fetchImpl: async (url, options) => { bodies.push(options.body); return { ok: true }; },
        });
        const r = await s.shipOnce();
        expect.ok(r.sent > 0 && r.sent < 20, 'one batch, not everything');
        expect.ok(bodies[0].length <= 1200, `gzip ${bodies[0].length} > 1200`);
        expect.equal(b.since(s.lastAckedSeq()).length, 20 - r.sent);
    });

    it('shipAll flushes the whole buffer in ≤ maxBytes chunks (send_log_tail)', async () => {
        const b = createLogBuffer();
        for (let i = 0; i < 20; i++) b.append('info', `line-${i}-` + 'q'.repeat(200));
        const bodies = [];
        const s = createLogShipper({
            buffer: b, hubUrl: HUB, token: TOKEN, workerId: 'w', maxBytes: 1200,
            fetchImpl: async (url, options) => { bodies.push(options.body); return { ok: true }; },
        });
        const r = await s.shipAll();
        expect.equal(r.sent, 20);
        expect.ok(bodies.length > 1, 'flushed in several chunks');
        let total = 0;
        for (const body of bodies) {
            expect.ok(body.length <= 1200);
            total += JSON.parse(zlib.gunzipSync(body).toString('utf8')).lines.length;
        }
        expect.equal(total, 20);
    });

    it('redacts secrets before the body leaves the host', async () => {
        const b = createLogBuffer();
        b.append('info', `token ${TOKEN} and upload_token=v1.SECRET.upload`);
        const bodies = [];
        const s = createLogShipper({
            buffer: b, hubUrl: HUB, token: TOKEN, workerId: 'w', secrets: [TOKEN],
            fetchImpl: async (url, options) => { bodies.push(options.body); return { ok: true }; },
        });
        await s.shipOnce();
        const text = zlib.gunzipSync(bodies[0]).toString('utf8');
        expect.notInclude(text, TOKEN);
        expect.notInclude(text, 'v1.SECRET.upload');
        expect.include(text, '[redacted]');
    });

    it('overlapping ticks coalesce; the call site is not awaited (task loop never blocks)', async () => {
        const b = createLogBuffer();
        b.append('info', 'x');
        const gate = deferred();
        let posts = 0;
        const s = createLogShipper({
            buffer: b, hubUrl: HUB, token: TOKEN, workerId: 'w',
            fetchImpl: async () => { posts += 1; await gate.promise; return { ok: true }; },
        });
        const p1 = s.shipOnce();
        let reached = false;
        // The task loop would run its next iteration here; nothing is awaited.
        reached = true;
        const p2 = s.shipOnce();
        expect.ok(reached);
        expect.equal(posts, 1, 'one in-flight POST, not two');
        gate.resolve();
        const [r1, r2] = await Promise.all([p1, p2]);
        expect.equal(r1.sent, 1);
        expect.equal(r2.sent, 1);
        expect.equal(posts, 1);
    });
});

describe('ComfyUI stderr capture (worker-started ComfyUI)', () => {
    it('lines written to a child stderr land in the buffer', async () => {
        const b = createLogBuffer();
        const child = spawn(process.execPath, ['-e', 'process.stderr.write("comfy-stderr-line\\n"); process.exit(0)']);
        captureStream(child.stderr, b, { level: 'comfy' });
        await new Promise((resolve) => child.on('exit', resolve));
        await new Promise((resolve) => setTimeout(resolve, 30));
        const got = b.all().filter((e) => e.level === 'comfy');
        expect.ok(got.some((e) => e.msg === 'comfy-stderr-line'), `buffer: ${JSON.stringify(b.all())}`);
    });
});

describe('worker.cjs wiring (bundle 2.4.1)', () => {
    const src = fs.readFileSync(path.join(__dirname, '..', 'worker', 'worker.cjs'), 'utf8');
    it('honours WORKER_LOG_SHIP and handles send_log_tail', () => {
        expect.include(src, 'WORKER_LOG_SHIP');
        expect.include(src, 'send_log_tail');
        expect.include(src, 'startLogShipping()');
        expect.include(src, 'maybeStartComfyChild()');
    });
    it('logs into the ring buffer', () => {
        expect.include(src, 'logBuffer.append(level, msg)');
        expect.include(src, 'createLogBuffer()');
    });
    it('the bundle version is 2.4.1', () => {
        const pkg = require(path.join(__dirname, '..', 'worker', 'package.json'));
        expect.equal(pkg.version, '2.4.1');
    });
});