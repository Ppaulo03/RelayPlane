GO      ?= go
PYTHON  ?= python
WORKERS ?= 3
INSTANCES ?= 20
MESSAGES ?= 2000
CHAOS_KILL ?= 0
GODIGEST = golang:1.26.5@sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678
INFRA    = deploy/docker/compose.infra.yml

.PHONY: sandbox-up sandbox-down sandbox-example test-sandbox test-load-stack test-load test-chaos test-integration-s3 fmt fmt-check vet build test test-integration test-race sdk-test infra-up infra-down up down evolution-image

# fmt rewrites files; fmt-check only reports (tests must never modify the working tree)
fmt:
	gofmt -l -w cmd internal migrations

fmt-check:
	@out="$$(gofmt -l cmd internal migrations)"; if [ -n "$$out" ]; then echo "needs gofmt:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...
	$(GO) vet -tags integration ./...

build:
	$(GO) build ./...

# unit + contract suites + in-memory system tests (no infrastructure needed)
test: fmt-check vet
	$(GO) test -count=1 ./...

infra-up:
	docker compose -f $(INFRA) up -d --wait

infra-down:
	docker compose -f $(INFRA) down -v

# real PostgreSQL / Redis Streams / S3 adapters + the whole system suite on real infrastructure
test-integration: infra-up
	$(GO) test -tags integration -count=1 ./internal/adapters/...
	RELAYPLANE_SYSTEMTEST_BACKEND=real $(GO) test -tags integration -count=1 ./internal/systemtest

# the object-store adapter and the whole system suite against EVERY supported S3-compatible backend
# MinIO is opt-in because its official image can no longer be pulled anywhere:
#   make test-integration-s3 COMPOSE_PROFILES=minio S3_BACKENDS="minio=127.0.0.1:59010"
S3_BACKENDS ?= rustfs=127.0.0.1:59011 seaweedfs=127.0.0.1:59012
test-integration-s3: infra-up
	@set -e; for b in $(S3_BACKENDS); do name=$${b%%=*}; ep=$${b#*=}; echo "=== object store: $$name ($$ep)"; \
	  RELAYPLANE_TEST_S3_ENDPOINT=$$ep $(GO) test -tags integration -count=1 ./internal/adapters/blob/...; \
	  RELAYPLANE_TEST_S3_ENDPOINT=$$ep RELAYPLANE_SYSTEMTEST_BACKEND=real $(GO) test -tags integration -count=1 ./internal/systemtest; done

# fault injection on real infrastructure (restart/pause redis and postgres, kill workers) while traffic flows
test-chaos: infra-up
	RELAYPLANE_SYSTEMTEST_BACKEND=real $(GO) test -tags "integration chaos" -count=1 -v -run "Chaos|Jitter" -timeout 10m ./internal/systemtest

# sandbox: the real stack with a drivable provider simulator (no WhatsApp number); see deploy/docker/sandbox.sh
sandbox-up:
	sh deploy/docker/sandbox.sh up
sandbox-example:
	sh deploy/docker/sandbox.sh example
sandbox-down:
	sh deploy/docker/sandbox.sh down
test-sandbox:
	sh deploy/docker/sandbox.sh test

# multi-process load: real gateway + N workers + reconciler containers, provider nodes replaced by cmd/loadstub.
# e.g. make test-load-stack WORKERS=6 INSTANCES=60 MESSAGES=6000 CHAOS_KILL=1   (see deploy/docker/load.sh)
test-load-stack:
	WORKERS=$(WORKERS) INSTANCES=$(INSTANCES) MESSAGES=$(MESSAGES) CHAOS_KILL=$(CHAOS_KILL) sh deploy/docker/load.sh

# the race detector needs cgo; run it in the pinned golang image
test-race:
	docker run --rm -v "$(CURDIR):/src" -w /src -v relayplane-gomod:/go/pkg/mod -v relayplane-gobuild:/root/.cache/go-build $(GODIGEST) go test -race -count=1 ./internal/...

sdk-test:
	cd sdk/python && $(PYTHON) -m pip install -q -e ".[dev]" && $(PYTHON) -m pytest -q

# Release flow of the Evolution image (build -> verify Baileys -> scan -> push -> capture digest).
# Pushing needs REGISTRY=registry.example.com/relayplane; without it only the local build runs.
EVO_TAG ?= 2.3.7-baileys-rc13
# SCANNER=scout uses Docker Scout (needs `docker login`); trivy is used automatically when installed
evolution-image:
	docker build -t relayplane/evolution:$(EVO_TAG) deploy/docker/evolution
	@echo "baileys in image: $$(docker run --rm --entrypoint node relayplane/evolution:$(EVO_TAG) -p "require('/evolution/node_modules/baileys/package.json').version")"
	@if command -v trivy >/dev/null 2>&1; then trivy image --exit-code 1 --severity CRITICAL relayplane/evolution:$(EVO_TAG); \
	elif [ "$(SCANNER)" = "scout" ]; then docker scout cves --exit-code --only-severity critical relayplane/evolution:$(EVO_TAG); \
	else echo "WARNING: no image scanner found (install trivy or docker scout): the image was NOT scanned"; fi
	@if [ -n "$(REGISTRY)" ]; then \
	  docker tag relayplane/evolution:$(EVO_TAG) $(REGISTRY)/evolution:$(EVO_TAG) && docker push $(REGISTRY)/evolution:$(EVO_TAG) && \
	  echo "deploy with: $$(docker image inspect $(REGISTRY)/evolution:$(EVO_TAG) --format '{{index .RepoDigests 0}}')"; \
	else echo "REGISTRY not set: not pushed. Set it to publish and capture the immutable digest to deploy."; fi

up:
	docker compose up -d --build

down:
	docker compose down

# throughput/latency baseline on real infrastructure (LOAD_MESSAGES, LOAD_INSTANCES tune the size)
test-load: infra-up
	RELAYPLANE_SYSTEMTEST_BACKEND=real $(GO) test -tags "integration chaos" -count=1 -v -run Load -timeout 15m ./internal/systemtest
