GO      ?= go
PYTHON  ?= python
GODIGEST = golang:1.26.5@sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678
INFRA    = deploy/docker/compose.infra.yml

.PHONY: fmt fmt-check vet build test test-integration test-race sdk-test infra-up infra-down up down evolution-image

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

# real PostgreSQL / Redis Streams / MinIO adapters + the whole system suite on real infrastructure
test-integration: infra-up
	$(GO) test -tags integration -count=1 ./internal/adapters/...
	RELAYPLANE_SYSTEMTEST_BACKEND=real $(GO) test -tags integration -count=1 ./internal/systemtest

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
