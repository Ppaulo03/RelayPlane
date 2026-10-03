#!/bin/sh
# Real-number spike: the real stack with a REAL Evolution node and a tap that records the raw webhooks.
# See docs/REAL-NUMBER-SPIKE.md. Use a DISPOSABLE WhatsApp number (the Evolution/Baileys API is unofficial: numbers can be banned).
#
#   sh deploy/docker/spike.sh up                      # build + start (secrets in .env.spike, git-ignored)
#   sh deploy/docker/spike.sh pair                    # create the instance, save/open the QR, wait for CONNECTED
#   sh deploy/docker/spike.sh send 5562999999999 "oi" # send a message from the paired number (prints the provider id)
#   sh deploy/docker/spike.sh summary                 # check the captures against our assumptions (safe to share)
#   sh deploy/docker/spike.sh sanitize                # produce shareable fixtures from the raw captures
#   sh deploy/docker/spike.sh logs [service]
#   sh deploy/docker/spike.sh restart-node            # recreate the node container KEEPING its database (session must come back without a QR)
#   sh deploy/docker/spike.sh down                    # stop; add -v to also delete the volumes (sessions!) and the captures state
set -eu
cd "$(dirname "$0")/../.."
ENVF=.env.spike
rnd() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
if [ ! -f "$ENVF" ]; then
  cat > "$ENVF" <<ENV
APP_ENV=development
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
NODE_CAPACITY=5
# conservative pacing: this is a real number
RATE_MIN_INTERVAL=3s
RATE_MAX_PER_MINUTE=10
RATE_MAX_CONCURRENT=1
RATE_BURST=1
RECONCILER_INTERVAL=5s
ENV
fi
DC="docker compose --env-file $ENVF -f docker-compose.yml -f deploy/docker/compose.spike.yml"
SERVICES="postgres redis rustfs evolution-node-01 gateway worker reconciler webhooktap"
PY="${PYTHON:-python}"
mkdir -p captures
export CAPTURES=captures

py() {
  . "./$ENVF"
  GATEWAY=http://127.0.0.1:18080 ADMIN_API_KEY="$ADMIN_API_KEY" PYTHONPATH=sdk/python:tools/spike "$PY" "$@"
}

case "${1:-}" in
  up)
    $DC up -d --build $SERVICES
    echo "waiting for the gateway..."
    i=0
    until curl -fsS http://127.0.0.1:18080/health/ready >/dev/null 2>&1; do
      i=$((i + 1)); [ "$i" -gt 120 ] && { $DC logs --tail 60 gateway evolution-node-01; echo "gateway never became ready" >&2; exit 1; }
      sleep 2
    done
    echo "stack ready. Next: sh deploy/docker/spike.sh pair"
    ;;
  pair) py tools/spike/pair.py ;;
  send) shift; py tools/spike/send.py "$@" ;;
  summary) py tools/spike/summary.py ;;
  sanitize) py tools/spike/sanitize.py ;;
  logs) shift; $DC logs --tail 100 -f "$@" ;;
  restart-node)
    # the node is a singleton whose state lives in its database: a NEW container on the same database must resume the session
    $DC stop evolution-node-01
    $DC rm -f evolution-node-01
    $DC up -d evolution-node-01
    echo "node recreated. Watch it reconnect without a QR:  sh deploy/docker/spike.sh logs evolution-node-01"
    ;;
  down)
    shift
    $DC --profile '*' down "$@"
    ;;
  *) echo "usage: $0 up|pair|send|summary|sanitize|logs|restart-node|down" >&2; exit 2 ;;
esac
