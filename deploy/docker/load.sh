#!/bin/sh
# Multi-process load test: real gateway + N workers + reconciler in containers, provider nodes replaced by cmd/loadstub.
#   CHAOS_KILL=1 additionally SIGKILLs a random worker every CHAOS_EVERY seconds (default 4). A send in flight when its worker dies is
#   legitimately UNKNOWN (never resent), so this mode uses UNKNOWN_BARRIER_TIMEOUT=5s and tolerates UNKNOWN.
#   WORKERS=4 INSTANCES=40 MESSAGES=4000 STUB_SEND_LATENCY=50ms sh deploy/docker/load.sh
# Secrets are generated per run into .env.load (git-ignored) and the stack is removed afterwards (KEEP=1 keeps it).
set -eu
cd "$(dirname "$0")/../.."
WORKERS=${WORKERS:-3}
INSTANCES=${INSTANCES:-20}
MESSAGES=${MESSAGES:-2000}
ENVF=.env.load
rnd() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
if [ ! -f "$ENVF" ]; then
  cat > "$ENVF" <<ENV
POSTGRES_PASSWORD=$(rnd)
ADMIN_API_KEY=$(rnd)
WEBHOOK_SECRET=$(rnd)
EVOLUTION_NODE_01_API_KEY=$(rnd)
EVOLUTION_NODE_02_API_KEY=$(rnd)
BLOB_ACCESS_KEY=relayplane
BLOB_SECRET_KEY=$(rnd)
COMPOSE_PROFILES=rustfs
BLOB_STORE_ENDPOINT=rustfs:9000
BLOB_PORT=19000
GATEWAY_PORT=18080
NODE_CAPACITY=500
RATE_MIN_INTERVAL=0s
RATE_MAX_PER_MINUTE=0
RATE_MAX_CONCURRENT=0
RATE_BURST=0
RECONCILER_INTERVAL=2s
UNKNOWN_BARRIER_TIMEOUT=$([ "${CHAOS_KILL:-0}" = 1 ] && echo 5s || echo 0)
STUB_SEND_LATENCY=${STUB_SEND_LATENCY:-50ms}
ENV
fi
DC="docker compose --env-file $ENVF -f docker-compose.yml -f deploy/docker/compose.load.yml"
cleanup() { [ "${KEEP:-0}" = 1 ] || $DC --profile '*' down -v >/dev/null 2>&1; [ "${KEEP:-0}" = 1 ] || rm -f "$ENVF"; }
trap cleanup EXIT
$DC up -d --build --scale worker="$WORKERS"
echo "waiting for the gateway..."
i=0
until curl -fsS http://127.0.0.1:18080/health/ready >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -gt 90 ] && { $DC logs --tail 40 gateway; echo "gateway never became ready" >&2; exit 1; }
  sleep 2
done
. "./$ENVF"
if [ "${CHAOS_KILL:-0}" = 1 ]; then
  # SIGKILL a random worker every few seconds while the load runs, then start it again
  ( while :; do
      sleep "${CHAOS_EVERY:-4}"
      c=$($DC ps -q worker | shuf -n 1 2>/dev/null || true)
      # (docker kill does not trigger the restart policy: bring the worker back like an orchestrator would)
      if [ -n "$c" ] && docker kill "$c" >/dev/null 2>&1; then
        echo "chaos: killed worker $(echo "$c" | cut -c1-12)"
        sleep 2; docker start "$c" >/dev/null 2>&1 || true
      fi
    done ) &
  CHAOS_PID=$!
  trap 'kill $CHAOS_PID 2>/dev/null; cleanup' EXIT
fi
set +e
go run ./cmd/loadgen -webhook-listen "0.0.0.0:${WEBHOOK_SINK_PORT:-18090}" -webhook-url "http://host.docker.internal:${WEBHOOK_SINK_PORT:-18090}/hook" -webhook-fail-rate "${WEBHOOK_FAIL_RATE:-0.1}" -gateway http://127.0.0.1:18080 -admin-key "$ADMIN_API_KEY" -instances "$INSTANCES" -messages "$MESSAGES" -timeout "${LOAD_TIMEOUT:-5m}" \
  -allow-unknown="$([ "${CHAOS_KILL:-0}" = 1 ] && echo true || echo false)" \
  -stubs "http://127.0.0.1:18081=$EVOLUTION_NODE_01_API_KEY,http://127.0.0.1:18082=$EVOLUTION_NODE_02_API_KEY"
status=$?
[ -z "${CHAOS_PID:-}" ] || kill "$CHAOS_PID" 2>/dev/null
$DC logs --no-log-prefix worker reconciler gateway 2>&1 | grep -ci '"level":"error"\|level=error' | sed 's/^/error log lines: /' || true
exit $status
