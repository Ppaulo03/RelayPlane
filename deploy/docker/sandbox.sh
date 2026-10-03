#!/bin/sh
# RelayPlane sandbox: the real stack with a drivable provider simulator instead of WhatsApp.
#
#   sh deploy/docker/sandbox.sh up        # start (secrets are generated into .env.sandbox, git-ignored)
#   sh deploy/docker/sandbox.sh example   # run examples/sandbox/quickstart.py against it
#   sh deploy/docker/sandbox.sh test      # up + example + down (CI)
#   sh deploy/docker/sandbox.sh down      # stop and remove everything
#
# Gateway: http://127.0.0.1:18080   Simulator control: http://127.0.0.1:18081/_sim/... (header: apikey)
set -eu
cd "$(dirname "$0")/../.."
ENVF=.env.sandbox
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
NODE_CAPACITY=100
RATE_MIN_INTERVAL=0s
RATE_MAX_PER_MINUTE=0
RATE_MAX_CONCURRENT=0
RATE_BURST=0
RECONCILER_INTERVAL=2s
ENV
fi
DC="docker compose --env-file $ENVF -f docker-compose.yml -f deploy/docker/compose.sandbox.yml"

up() {
  $DC up -d --build
  echo "waiting for the gateway..."
  i=0
  until curl -fsS http://127.0.0.1:18080/health/ready >/dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -gt 90 ] && { $DC logs --tail 40 gateway; echo "gateway never became ready" >&2; exit 1; }
    sleep 2
  done
  . "./$ENVF"
  echo "sandbox ready"
  echo "  gateway          http://127.0.0.1:18080   (admin key: see $ENVF -> ADMIN_API_KEY)"
  echo "  simulator node-1 http://127.0.0.1:18081   (apikey: EVOLUTION_NODE_01_API_KEY in $ENVF)"
}
down() { $DC --profile '*' down -v >/dev/null 2>&1 || true; rm -f "$ENVF"; }
example() {
  . "./$ENVF"
  GATEWAY=http://127.0.0.1:18080 ADMIN_API_KEY="$ADMIN_API_KEY" \
  SIM_NODES="http://127.0.0.1:18081=$EVOLUTION_NODE_01_API_KEY,http://127.0.0.1:18082=$EVOLUTION_NODE_02_API_KEY" \
  PYTHONPATH=sdk/python "${PYTHON:-python}" examples/sandbox/quickstart.py
}

case "${1:-}" in
  up) up ;;
  example) example ;;
  down) down ;;
  test)
    trap down EXIT
    up
    example
    ;;
  *) echo "usage: $0 up|example|test|down" >&2; exit 2 ;;
esac
