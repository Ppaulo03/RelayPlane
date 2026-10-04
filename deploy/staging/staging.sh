#!/bin/sh
# Staging: the production configuration, from the images a release built.
#
#   sh deploy/staging/staging.sh init                   # secrets into .env.staging (git-ignored), APP_ENV=production
#   sh deploy/staging/staging.sh build-local            # CI/PR: build the images locally and write images.env for them
#   sh deploy/staging/staging.sh up [images.env]        # start from the images listed in images.env (a release's, or build-local's)
#   sh deploy/staging/staging.sh smoke                  # what must be true of a freshly started staging (no WhatsApp number needed)
#   sh deploy/staging/staging.sh images                 # which image digest each container really runs
#   sh deploy/staging/staging.sh logs [service]         # FOLLOW=1 to keep following
#   sh deploy/staging/staging.sh down [-v]
set -eu
cd "$(dirname "$0")/../.."
ENVF=.env.staging
IMAGES_ENV=${IMAGES_ENV:-deploy/staging/images.env}
rnd() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
PY="${PYTHON:-python}"

init() {
  [ -f "$ENVF" ] && return 0
  cat > "$ENVF" <<ENV
APP_ENV=production
POSTGRES_PASSWORD=$(rnd)
ADMIN_API_KEY=$(rnd)
WEBHOOK_SECRET=$(rnd)
SUBSCRIPTION_SECRET=$(rnd)
EVOLUTION_NODE_01_API_KEY=$(rnd)
EVOLUTION_NODE_02_API_KEY=$(rnd)
BLOB_ACCESS_KEY=relayplane
BLOB_SECRET_KEY=$(rnd)
COMPOSE_PROFILES=rustfs
BLOB_STORE_ENDPOINT=rustfs:9000
GATEWAY_PORT=18080
BLOB_PORT=19000
ENV
  echo "wrote $ENVF (git-ignored)"
}

dc() {
  [ -f "$IMAGES_ENV" ] || { echo "no $IMAGES_ENV: take it from a release (docs/RELEASE.md) or run: sh deploy/staging/staging.sh build-local" >&2; exit 1; }
  docker compose --env-file "$ENVF" --env-file "$IMAGES_ENV" -f docker-compose.yml -f deploy/staging/compose.staging.yml "$@"
}

case "${1:-}" in
  init) init ;;
  build-local)
    init
    # the same Dockerfiles a release builds, tagged locally; the compose file of the repository knows how to build them
    docker compose --env-file "$ENVF" -f docker-compose.yml build gateway worker reconciler evolution-node-01
    V=${RELAYPLANE_VERSION:-dev}
    cat > "$IMAGES_ENV" <<ENV
GATEWAY_IMAGE=relayplane/gateway:$V
WORKER_IMAGE=relayplane/worker:$V
RECONCILER_IMAGE=relayplane/reconciler:$V
EVOLUTION_IMAGE=relayplane/evolution:2.3.7-baileys-rc13
LOCAL_IMAGES=1
ENV
    echo "wrote $IMAGES_ENV (local images)"
    ;;
  up)
    init
    [ -z "${2:-}" ] || IMAGES_ENV=$2
    [ -f "$IMAGES_ENV" ] || { echo "no $IMAGES_ENV" >&2; exit 1; }
    # a staging started from anything but immutable references is not a rehearsal of production: refuse, unless the images
    # were just built locally (build-local says so)
    if ! grep -q '^LOCAL_IMAGES=1' "$IMAGES_ENV"; then
      for v in GATEWAY_IMAGE WORKER_IMAGE RECONCILER_IMAGE EVOLUTION_IMAGE; do
        ref=$(sed -n "s/^$v=//p" "$IMAGES_ENV")
        case "$ref" in *@sha256:????????????????????????????????????????????????????????????????) ;; *) echo "$v must be pinned by digest (repository@sha256:...), got '$ref'" >&2; exit 1 ;; esac
      done
    fi
    dc up -d --no-build
    echo "waiting for the gateway..."
    i=0
    until curl -fsS "http://127.0.0.1:$(. "./$ENVF"; echo "${GATEWAY_PORT:-18080}")/health/ready" >/dev/null 2>&1; do
      i=$((i + 1)); [ "$i" -gt 120 ] && { dc logs --tail 60 gateway; echo "gateway never became ready" >&2; exit 1; }
      sleep 2
    done
    echo "staging is up. Next: sh deploy/staging/staging.sh smoke"
    ;;
  smoke)
    . "./$ENVF"
    GATEWAY="http://127.0.0.1:${GATEWAY_PORT:-18080}" ADMIN_API_KEY="$ADMIN_API_KEY" PYTHONPATH=sdk/python PYTHONUNBUFFERED=1 "$PY" deploy/staging/smoke.py
    ;;
  images)
    for s in gateway worker reconciler evolution-node-01 evolution-node-02; do
      for c in $(dc ps -q "$s"); do
        printf '%s: ' "$s"; docker inspect --format '{{.Config.Image}} -> {{.Image}}' "$c"
      done
    done
    ;;
  logs) shift; dc logs --tail 200 ${FOLLOW:+-f} "$@" ;;   # FOLLOW=1 to keep following
  down) shift; dc --profile '*' down "$@" ;;
  *) echo "usage: $0 init|build-local|up [images.env]|smoke|images|logs|down" >&2; exit 2 ;;
esac
