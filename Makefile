# Makefile for github.com/mopo3ula/dedup
GO      := go
PKGS    := $(shell $(GO) list ./...)
REDIS_IMAGE     := redis:7
REDIS_CONTAINER := dedup-redis
REDIS_ADDR      := 127.0.0.1:6379
.PHONY: all test bench vet fmt tidy deps lint \
        docker-redis stop-redis test-redis
# ── default ────────────────────────────────────────────────────────────────────
all: vet test
# ── code quality ───────────────────────────────────────────────────────────────
vet:
	@echo ">> go vet"
	$(GO) vet ./...
fmt:
	@echo ">> gofmt -w ."
	gofmt -w .
lint:
	@echo ">> golangci-lint run"
	golangci-lint run || true
# ── tests ──────────────────────────────────────────────────────────────────────
test:
	@echo ">> go test -race ./..."
	$(GO) test -race -count=1 ./...
# Run only unit tests (no Redis required)
test-unit:
	@echo ">> go test -race (unit only)"
	$(GO) test -race -count=1 -run 'Test|Benchmark' \
	    . ./coordinator/singleflight/... ./store/inmemory/... ./key/...
# Run integration tests that require Redis (set REDIS_ADDR if non-default)
test-redis:
	@echo ">> go test -race -tags integration ./..."
	REDIS_ADDR=$(REDIS_ADDR) $(GO) test -race -count=1 -tags integration ./...
bench:
	@echo ">> go test -bench=. -benchmem ."
	$(GO) test -bench=. -benchmem -run '^$$' .
bench-cpu:
	@echo ">> go test -bench=. -cpuprofile cpu.prof ."
	$(GO) test -bench=. -run '^$$' -cpuprofile cpu.prof .
	$(GO) tool pprof cpu.prof
# ── dependencies ───────────────────────────────────────────────────────────────
tidy:
	@echo ">> go mod tidy"
	$(GO) mod tidy
deps:
	@echo ">> go list -m all"
	$(GO) list -m all
# ── Redis helpers ──────────────────────────────────────────────────────────────
docker-redis:
	@echo ">> starting Redis container $(REDIS_CONTAINER)"
	docker run -d -p 6379:6379 --name $(REDIS_CONTAINER) $(REDIS_IMAGE)
	@echo "Redis available at $(REDIS_ADDR)"
stop-redis:
	@echo ">> stopping $(REDIS_CONTAINER)"
	-docker stop $(REDIS_CONTAINER)
	-docker rm   $(REDIS_CONTAINER)
