#!/usr/bin/env bash

# ======================================================
# Animastor GPU Worker Startup Script
# ======================================================
#
#   ./start-worker.sh            # worker type from worker/.env (WORKER_TYPE)
#   ./start-worker.sh audio      # explicit override of the .env type
#
# Prints "READY: <type> worker" ONLY after the worker logged "ComfyUI ready"
# (i.e. it really reached ComfyUI). Any failure exits non-zero with one line
# of cause and the exact log path.
#
# Tunables (env or worker/.env): COMFY_WAIT_SECS (default 180) — how long to
# wait for the ComfyUI API; READY_WAIT_SECS (default 90) — how long to wait
# for the worker's "ComfyUI ready" log line.

echo "===================================="
echo "Animastor Worker Startup"
echo "===================================="

# ======================================================
# 0. RUN AS THE INSTALLATION OWNER
# ======================================================
# The worker must never run as root: the .env credential is chmod 600 and
# owned by the installing user, and worker state/logs must stay owned by
# that user too. If launched as root, re-exec as the script's owner.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Owner of the installation directory — the script file itself may have been
# copied by root or another account, the install root is always owned by
# the user the installer ran for.
SCRIPT_OWNER="$(stat -c %U "$SCRIPT_DIR" 2>/dev/null || echo root)"

if [ "$(id -u)" = "0" ] && [ "$SCRIPT_OWNER" != "root" ]; then
  OWNER_HOME="$(getent passwd "$SCRIPT_OWNER" | cut -d: -f6)"
  echo "Re-executing as $SCRIPT_OWNER (worker must not run as root) ..."
  exec env HOME="$OWNER_HOME" runuser -u "$SCRIPT_OWNER" -- "$SCRIPT_DIR/start-worker.sh" "$@"
fi

fail() {
  echo "❌ $1"
  [ -n "${2:-}" ] && echo "   Fix: $2"
  exit 1
}

# ======================================================
# 1. PATHS
# ======================================================

# Anchor to the directory this script lives in (…/animastor) rather than
# $HOME, so the correct worker/.env is loaded no matter who launches it.
BASE_DIR="$SCRIPT_DIR"

mkdir -p "$BASE_DIR/logs"
mkdir -p "$BASE_DIR/worker"

cd "$BASE_DIR/worker" || exit 1

# ======================================================
# 2. LOAD .env (worker-local configuration)
# ======================================================
# The Private Worker credential (ANIMASTOR_WORKER_TOKEN), WORKER_TYPE and
# optional overrides (HUB_URL, WORKER_ID, COMFY_URL, COMFY_PORT, NODE_BIN,
# ...) are read from ./.env. Format: KEY=VALUE lines, '#' comments. Values
# are NOT shell-evaluated.

if [ -f .env ]; then
  echo "Loading .env from $(pwd)/.env"
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      ''|\#*) continue ;;
      *=*) ;;
      *) continue ;;
    esac
    ENV_KEY="${line%%=*}"
    ENV_VAL="${line#*=}"
    # strip optional surrounding quotes
    ENV_VAL="${ENV_VAL%\"}"; ENV_VAL="${ENV_VAL#\"}"
    ENV_VAL="${ENV_VAL%\'}"; ENV_VAL="${ENV_VAL#\'}"
    export "$ENV_KEY=$ENV_VAL"
  done < .env
fi

# ======================================================
# 3. WORKER TYPE (.env is the source; the argument only overrides)
# ======================================================

WORKER_TYPE="${1:-${WORKER_TYPE:-}}"

if [ -z "$WORKER_TYPE" ]; then
  fail "WORKER_TYPE is not set in $(pwd)/.env" "add WORKER_TYPE=image|audio|video to .env (the type chosen in Animastor), or run: $0 audio"
fi
if [[ "$WORKER_TYPE" != "image" && "$WORKER_TYPE" != "audio" && "$WORKER_TYPE" != "video" ]]; then
  fail "Invalid worker type: $WORKER_TYPE" "use one of: image | audio | video"
fi

echo "Worker type: $WORKER_TYPE${1:+ (command-line override)}"

# ======================================================
# 4. GPU CHECK
# ======================================================
# A missing NVIDIA GPU is not fatal: the worker can serve a ComfyUI running
# in CPU mode (--cpu; TTS/audio profile test scenario on CPU-only VPS).

if ! command -v nvidia-smi >/dev/null 2>&1; then
  echo "⚠️ NVIDIA GPU not detected — continuing in CPU mode"
  echo "   (performance will be significantly lower; suitable for the TTS/audio profile)"
else
  echo "GPU:"
  nvidia-smi --query-gpu=name,memory.total --format=csv,noheader
fi

# ======================================================
# 5. NODE.JS >= 20 (the same binary the installer verified)
# ======================================================
# NODE_BIN from .env wins (written by the installer). Otherwise the first
# Node.js >= 20 among node/node22/node20 and the installer-provisioned runtime
# (~/.animastor/node-runtime). Nothing is installed system-wide from here.

node_major() { "$1" -e 'process.stdout.write(process.versions.node.split(".")[0])' 2>/dev/null || echo 0; }

pick_node() {
  local c
  for c in "${NODE_BIN:-}" node node22 node20 "$HOME"/.animastor/node-runtime/*/node-*/bin/node; do
    [ -n "$c" ] || continue
    if [ -x "$c" ] || command -v "$c" >/dev/null 2>&1; then
      if [ "$(node_major "$c")" -ge 20 ]; then
        command -v "$c" 2>/dev/null || echo "$c"
        return 0
      fi
    fi
  done
  return 1
}

if [ -n "${NODE_BIN:-}" ] && [ "$(node_major "$NODE_BIN")" -lt 20 ]; then
  echo "⚠️ NODE_BIN=$NODE_BIN from .env is missing or older than Node.js 20 — looking for another one"
fi
NODE_BIN="$(pick_node)" || fail "Node.js >= 20 not found (node -v: $(node -v 2>/dev/null || echo none))" \
  "re-run the Animastor installer (it provisions Node.js), or install Node.js 20+ and set NODE_BIN=/path/to/node in $(pwd)/.env"

echo "Node: $("$NODE_BIN" -v) ($NODE_BIN)"

# ======================================================
# 6. COMFYUI ENDPOINT (wait for the API, then pick the prefix)
# ======================================================
# COMFY_URL from .env wins (the installer writes the URL it probed). Without
# it: port = COMFY_PORT, else --port of the user's running ComfyUI, else 8188.
# The plain http://127.0.0.1:<port> is tried FIRST; the notebook prefix
# (--baseurl / NOTEBOOK_PATH) only when the plain URL does not answer.

COMFY_PID=$(pgrep -u "$(id -u)" -f "main.py" | head -1)
COMFY_CMDLINE=""
[ -n "$COMFY_PID" ] && COMFY_CMDLINE=$(tr '\0' ' ' < "/proc/$COMFY_PID/cmdline" 2>/dev/null)

if [ -z "${COMFY_PORT:-}" ] && [ -n "$COMFY_CMDLINE" ]; then
  COMFY_PORT=$(echo "$COMFY_CMDLINE" | grep -oP '(?<=--port )[^ ]+' | head -1)
fi
COMFY_PORT="${COMFY_PORT:-8188}"

if [ -z "${NOTEBOOK_PATH:-}" ] && [ -n "$COMFY_CMDLINE" ]; then
  NOTEBOOK_PATH=$(echo "$COMFY_CMDLINE" | grep -oP '(?<=--baseurl )[^ ]+' | head -1)
fi
NOTEBOOK_PATH="${NOTEBOOK_PATH%/}"

CANDIDATES=()
if [ -n "${COMFY_URL:-}" ]; then
  CANDIDATES+=("${COMFY_URL%/}")
else
  CANDIDATES+=("http://127.0.0.1:$COMFY_PORT")
  [ -n "$NOTEBOOK_PATH" ] && CANDIDATES+=("http://127.0.0.1:$COMFY_PORT$NOTEBOOK_PATH")
fi

COMFY_WAIT_SECS="${COMFY_WAIT_SECS:-180}"
echo "Waiting for ComfyUI (up to ${COMFY_WAIT_SECS}s): ${CANDIDATES[*]}"
SELECTED=""
DEADLINE=$(( $(date +%s) + COMFY_WAIT_SECS ))
while [ -z "$SELECTED" ]; do
  for u in "${CANDIDATES[@]}"; do
    if curl -fsS -m 5 -o /dev/null "$u/system_stats" 2>/dev/null; then SELECTED="$u"; break; fi
  done
  [ -n "$SELECTED" ] && break
  [ "$(date +%s)" -ge "$DEADLINE" ] && break
  sleep 3
done

if [ -z "$SELECTED" ]; then
  fail "ComfyUI API did not answer within ${COMFY_WAIT_SECS}s at: ${CANDIDATES[*]} (GET /system_stats)" \
    "start ComfyUI (e.g. cd ~/ComfyUI && python main.py --listen 127.0.0.1 --port $COMFY_PORT) or set COMFY_URL=http://127.0.0.1:<port> in $(pwd)/.env"
fi
echo "ComfyUI: $SELECTED"

# ======================================================
# 7. ENV
# ======================================================
# Values already present in the environment or loaded from .env WIN over
# the defaults below (so a private worker token is never overwritten).

export HUB_URL="${HUB_URL:-https://animastor.org/gpu}"
export COMFY_URL="$SELECTED"
export COMFY_PORT
export WORKER_TYPE
export COMFY_INPUT_DIR="${COMFY_INPUT_DIR:-$HOME/ComfyUI/input}"
export WORKER_ID="${WORKER_ID:-gpu-$(hostname)}"

if [ -z "${ANIMASTOR_WORKER_TOKEN:-}" ]; then
  echo "❌ Worker authentication failed — check ANIMASTOR_WORKER_TOKEN"
  echo ""
  echo "   ANIMASTOR_WORKER_TOKEN is not set. The worker cannot start:"
  echo "   a missing credential must never silently turn this GPU into"
  echo "   shared/system capacity (fail-closed model)."
  echo ""
  echo "   1. Open Animastor → Settings → Workers and create a worker"
  echo "      (Private = only your workspace; Share = volunteer to the community)."
  echo "   2. Copy the one-time credential (wrk.…)."
  echo "   3. Set ANIMASTOR_WORKER_TOKEN=wrk.… in ./.env (or the environment)."
  exit 1
else
  echo "ANIMASTOR_WORKER_TOKEN: set (credential will be verified at startup)"
fi

# ======================================================
# 8. STOP OLD WORKERS
# ======================================================

# Only workers of THIS installation (cwd = this worker dir) — another
# installation on the same host keeps running.
OLD_PIDS=""
for pid in $(pgrep -u "$(id -u)" -f "worker\.cjs"); do
  [ "$(readlink "/proc/$pid/cwd" 2>/dev/null)" = "$(pwd -P)" ] && OLD_PIDS="$OLD_PIDS $pid"
done
if [ -n "$OLD_PIDS" ]; then
  echo "Stopping old workers:$OLD_PIDS"
  kill $OLD_PIDS 2>/dev/null
  sleep 2
fi

# ======================================================
# 9. CHECK WORKER FILE
# ======================================================

[ -f worker.cjs ] || fail "worker.cjs not found in $(pwd)" "re-run the Animastor installer to deploy the worker bundle"

# ======================================================
# 10. START WORKER
# ======================================================

LOG_FILE="$BASE_DIR/logs/worker.log"
touch "$LOG_FILE"
LOG_OFFSET=$(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)

echo "Starting $WORKER_TYPE worker..."

setsid "$NODE_BIN" worker.cjs >> "$LOG_FILE" 2>&1 &

WORKER_PID=$!
echo "$WORKER_PID" > "$BASE_DIR/logs/worker.pid"

# ======================================================
# 11. READY = the worker itself logged "ComfyUI ready"
# ======================================================

READY_WAIT_SECS="${READY_WAIT_SECS:-90}"
DEADLINE=$(( $(date +%s) + READY_WAIT_SECS ))
READY=""
while :; do
  if tail -c +"$((LOG_OFFSET + 1))" "$LOG_FILE" 2>/dev/null | grep -q "ComfyUI ready"; then READY=1; break; fi
  if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "------ last logs ------"; tail -n 20 "$LOG_FILE"; echo "-----------------------"
    fail "worker exited during startup — see $LOG_FILE" "fix the error shown above, then re-run $0"
  fi
  [ "$(date +%s)" -ge "$DEADLINE" ] && break
  sleep 2
done

if [ -z "$READY" ]; then
  echo "------ last logs ------"; tail -n 20 "$LOG_FILE"; echo "-----------------------"
  fail "worker (PID $WORKER_PID) did not log \"ComfyUI ready\" within ${READY_WAIT_SECS}s — see $LOG_FILE" \
    "check that $SELECTED/system_stats answers for the worker's user, then re-run $0"
fi

echo "✅ Worker started (PID: $WORKER_PID), ComfyUI ready"
echo "Log: $LOG_FILE"
echo "===================================="
echo "READY: $WORKER_TYPE worker"
echo "===================================="
