<p align="center">
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT" />
  <img src="https://img.shields.io/badge/Node.js-%3E%3D18-339933?logo=node.js&logoColor=white" alt="Node.js" />
</p>

<h1 align="center">Animastor Worker</h1>

<p align="center">
  <strong>GPU worker bundle for the Animastor platform</strong><br/>
  Private / shared / system compute agents that run image, audio, and video generation via ComfyUI.
</p>

---

## About

This repository is the **worker** component of Animastor. Workers poll the GPU hub for
jobs and execute generation pipelines (TTS, Stable Diffusion, LTX video) through ComfyUI.
The distributable artifact is the zero-dependency Node.js bundle in
`packages/animastor-worker/worker/` (`animastor-worker@2.1.1`).

## Repository layout

```
animastor-worker/
├── packages/
│   └── animastor-worker/   # Worker runtime bundle + tests + tools
│       ├── worker/         # canonical worker bundle (worker.cjs, job-protocol-v2.cjs, …)
│       ├── tests/          # test suite
│       └── tools/          # sync-protocol.cjs (Job Protocol v2)
├── docker/worker/          # Dockerfile + entrypoint for the worker image
├── docs/architecture/      # Worker docs (JOB_PROTOCOL_V2.md, Linux installer, phase audits)
└── LICENSE
```

## Development

```bash
cd packages/animastor-worker
npm ci
npm test                     # run the worker test suite
node tools/sync-protocol.cjs --check
```

Building the worker image (from the repository root):

```bash
docker build -t animastor-worker -f docker/worker/Dockerfile docker/worker
```

## Related repositories

Animastor is split into separate repositories:

- [animastor-backend](https://github.com/Animastor/animastor-backend) — API server + orchestration
- [animastor-web](https://github.com/Animastor/animastor-web) — responsive web client
- [animastor-android](https://github.com/Animastor/animastor-android) — native Android client
- [animastor-gpu-hub](https://github.com/Animastor/animastor-gpu-hub) — GPU task dispatcher

The worker communicates with the hub and shares the canonical Job Protocol v2 (see
[`docs/architecture/JOB_PROTOCOL_V2.md`](docs/architecture/JOB_PROTOCOL_V2.md)).

## License

This project is licensed under the [MIT License](LICENSE).
