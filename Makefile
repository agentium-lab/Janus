.PHONY: all build vet staticcheck test coverage verify proto python-compile clean

# All Go modules managed by go.work. Commands must reference module paths
# explicitly because the repo root is not itself a Go module.
GO_MODULES := ./core/... ./server/... ./cli/... ./sdk/go/... ./demo/... ./proto/...

# Internal packages that do not require external infrastructure (PostgreSQL,
# NATS, Redis). These run on every change for fast feedback.
GO_INTERNAL_PKGS := \
	./core/... \
	./server/internal/handler/... \
	./server/internal/service/... \
	./server/internal/grpc/... \
	./server/internal/gateway/... \
	./server/internal/auth/... \
	./server/internal/expiry/... \
	./server/internal/heartbeat/... \
	./server/internal/outbox/... \
	./server/internal/retry/... \
	./server/internal/metrics/... \
	./server/internal/config/... \
	./server/internal/bootstrap/... \
	./server/internal/lease/... \
	./server/tests/simulation/... \
	./sdk/go/... \
	./cli/... \
	./demo/...

# Coverage gate packages: Core unit-testable production packages only.
# Demo and simulation are excluded (not Core production code per the GA plan).
COVERAGE_PKGS := \
	./core/... \
	./server/internal/handler/... \
	./server/internal/service/... \
	./server/internal/grpc/... \
	./server/internal/gateway/... \
	./server/internal/auth/... \
	./server/internal/expiry/... \
	./server/internal/heartbeat/... \
	./server/internal/outbox/... \
	./server/internal/retry/... \
	./server/internal/bootstrap/... \
	./server/internal/lease/... \
	./server/internal/metrics/... \
	./server/internal/config/... \
	./sdk/go/... \
	./cli/...

# Coverage gate threshold (percent). The hard 90% floor is a Milestone 1
# (Core Reliability Alpha) exit criterion; the Phase 0 default is 0
# (report-only). Override at the command line: make coverage COVERAGE_THRESHOLD=90
COVERAGE_THRESHOLD ?= 0

# Proto generation uses a pinned buf + plugin toolchain.
PROTO_PLUGINS := protoc-gen-go protoc-gen-go-grpc protoc-gen-grpc-gateway

# Default target: build everything.
all: build

## build: Compile all Go modules.
build:
	go build $(GO_MODULES)

## vet: Run go vet on all Go modules.
vet:
	go vet $(GO_MODULES)

## staticcheck: Run staticcheck on all Go modules (install if missing).
# Excluded checks (all housekeeping/style, not correctness gates for Phase 0):
#   U1000      - unused code (legacy test helpers exist)
#   ST1000-... - documentation style (package comments, comment forms)
# Tighten these exclusions as the corresponding cleanup lands in later
# milestones. Correctness checks (SA*, ST1001+, etc.) remain enforced.
staticcheck:
	@command -v staticcheck >/dev/null 2>&1 || { \
		echo "installing staticcheck..."; \
		GOPROXY=$${GOPROXY:-https://goproxy.cn,direct} go install honnef.co/go/tools/cmd/staticcheck@latest; \
	}
	$$(go env GOPATH)/bin/staticcheck -checks 'all,-U1000,-ST1000,-ST1020,-ST1021' $(GO_MODULES)

## test: Run unit tests that do not need external infrastructure.
test:
	go test -count=1 -timeout=120s -race $(GO_INTERNAL_PKGS)

## coverage: Run unit tests with coverage and enforce a floor.
## coverage: Run unit tests with coverage and report. The hard 90% floor is a
# Core Reliability Alpha (Milestone 1) exit criterion, not a Phase 0 gate, so
# the default threshold is 0 (report-only). Override via COVERAGE_THRESHOLD=90.
coverage:
	@mkdir -p .cover
	go test -count=1 -timeout=120s -race -coverprofile=.cover/core.out $(COVERAGE_PKGS) >/dev/null 2>&1 || true
	@cov=$$(go tool cover -func=.cover/core.out | awk '/^total:/ {print $$3}' | tr -d '%'); \
	cov_int=$${cov%.*}; \
	thresh_int=$${COVERAGE_THRESHOLD:-0}; \
	thresh_int=$${thresh_int%.*}; \
	echo "Coverage: $$cov% (threshold: $${COVERAGE_THRESHOLD:-0}%)"; \
	if [ "$$cov_int" -lt "$$thresh_int" ]; then \
		echo "FAIL: coverage $$cov% < $${COVERAGE_THRESHOLD:-0}%"; \
		rm -f .cover/core.out; \
		exit 1; \
	fi; \
	echo "OK: coverage $$cov% >= $${COVERAGE_THRESHOLD:-0}%"

## python-compile: Syntax-check the Python SDK.
python-compile:
	@command -v python3 >/dev/null 2>&1 || { echo "python3 not found"; exit 0; }
	python3 -m py_compile $$(find sdk/python/janus_sdk -name '*.py')

## proto: Regenerate protobuf + grpc-gateway artifacts from proto/.
proto: proto-toolchain
	cd proto && PATH=$$(go env GOPATH)/bin:$$PATH buf generate

proto-toolchain:
	@command -v buf >/dev/null 2>&1 || { \
		echo "installing buf..."; \
		GOPROXY=$${GOPROXY:-https://goproxy.cn,direct} go install github.com/bufbuild/buf/cmd/buf@latest; \
	}
	@for plugin in $(PROTO_PLUGINS); do \
		command -v $$plugin >/dev/null 2>&1 || { \
			echo "missing $$plugin; installing..."; \
			case $$plugin in \
				protoc-gen-go) GOPROXY=$${GOPROXY:-https://goproxy.cn,direct} go install google.golang.org/protobuf/cmd/protoc-gen-go@latest;; \
				protoc-gen-go-grpc) GOPROXY=$${GOPROXY:-https://goproxy.cn,direct} go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest;; \
				protoc-gen-grpc-gateway) GOPROXY=$${GOPROXY:-https://goproxy.cn,direct} go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest;; \
			esac; \
		}; \
	done

## verify: Local merge baseline = vet + staticcheck + unit tests + python + coverage gate.
verify: vet staticcheck test python-compile coverage
	@echo "verify: all gates passed"

## clean: Remove generated coverage artifacts.
clean:
	rm -rf .cover
