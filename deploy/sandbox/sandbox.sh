#!/bin/sh
# RelayPlane sandbox WITHOUT the repository: starts the published images with a provider simulator (compose.yml, next to this script).
#
#   [GATEWAY_PORT=.. SIM_NODE_01_PORT=.. SIM_NODE_02_PORT=..] sh sandbox.sh up [images.env]   # (ports: when the defaults are taken) secrets are generated into .env.sandbox; images.env comes from a release (default: ./images.env)
#   sh sandbox.sh info              # URLs and keys of the running sandbox
#   sh sandbox.sh down              # stop and remove everything, including the data and .env.sandbox
#
# Needs only Docker (with compose v2) and curl. The release publishes this file, compose.yml and images.env together.
set -eu
cd "$(dirname "$0")"
ENVF=.env.sandbox
IMAGES_ENV=${IMAGES_ENV:-images.env}
rnd() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

init() {
  [ -f "$ENVF" ] && return 0
  cat > "$ENVF" <<ENV
POSTGRES_PASSWORD=$(rnd)
ADMIN_API_KEY=$(rnd)
WEBHOOK_SECRET=$(rnd)
EVOLUTION_NODE_01_API_KEY=$(rnd)
EVOLUTION_NODE_02_API_KEY=$(rnd)
BLOB_SECRET_KEY=$(rnd)
GATEWAY_PORT=${GATEWAY_PORT:-18080}
SIM_NODE_01_PORT=${SIM_NODE_01_PORT:-18081}
SIM_NODE_02_PORT=${SIM_NODE_02_PORT:-18082}
ENV
  echo "wrote $ENVF"
}

dc() { docker compose --env-file "$ENVF" --env-file "$IMAGES_ENV" -f compose.yml "$@"; }

info() {
  . "./$ENVF"
  echo "gateway          http://127.0.0.1:${GATEWAY_PORT:-18080}      admin key: $ADMIN_API_KEY"
  echo "simulator node-1 http://127.0.0.1:${SIM_NODE_01_PORT:-18081}  apikey:    $EVOLUTION_NODE_01_API_KEY"
  echo "simulator node-2 http://127.0.0.1:${SIM_NODE_02_PORT:-18082}  apikey:    $EVOLUTION_NODE_02_API_KEY"
  echo "a consumer running on this machine receives webhooks at http://host.docker.internal:<port>/<path>"
}

case "${1:-}" in
  up)
    [ -z "${2:-}" ] || IMAGES_ENV=$2
    [ -f "$IMAGES_ENV" ] || { echo "no $IMAGES_ENV: take it from a release (it lists GATEWAY_IMAGE, WORKER_IMAGE, RECONCILER_IMAGE and SIMULATOR_IMAGE)" >&2; exit 1; }
    init
    dc up -d --no-build
    . "./$ENVF"
    echo "waiting for the gateway..."
    i=0
    until curl -fsS "http://127.0.0.1:${GATEWAY_PORT:-18080}/health/ready" >/dev/null 2>&1; do
      i=$((i + 1)); [ "$i" -gt 90 ] && { dc logs --tail 40 gateway; echo "gateway never became ready" >&2; exit 1; }
      sleep 2
    done
    echo "sandbox ready"
    info
    ;;
  info) [ -f "$ENVF" ] || { echo "not started: sh sandbox.sh up" >&2; exit 1; }; info ;;
  down)
    # by project name: it needs neither the images file nor the generated secrets
    docker compose -p relayplane-sandbox down -v >/dev/null 2>&1 || true
    rm -f "$ENVF"
    echo "sandbox removed"
    ;;
  *) echo "usage: $0 up [images.env] | info | down" >&2; exit 2 ;;
esac
