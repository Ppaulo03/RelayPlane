GO      ?= go
PYTHON  ?= python
GODIGEST = golang:1.26.5@sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678
INFRA    = deploy/docker/compose.infra.yml

.PHONY: fmt fmt-check vet build test test-integration test-race sdk-test infra-up infra-down up down

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

up:
	docker compose up -d --build

down:
	docker compose down
