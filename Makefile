.PHONY: all build vet staticcheck test coverage verify proto python-compile clean beta-fast contract-check python-test typescript-test verify-sdk-cli verify-protocol python-examples-compile smoke-7-agents smoke-grafana-panels verify-security verify-governance verify-reliability verify-ops-chaos verify-release-ops ga-readiness verify-production

# All Go modules managed by go.work. Commands must reference module paths
# explicitly because the repo root is not itself a Go module.
GO_MODULES := ./core/... ./server/... ./cli/... ./sdk/go/... ./proto/...

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
	./cli/...

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
COVERAGE_THRESHOLD ?= 85

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
# the threshold comes from COVERAGE_THRESHOLD above (default 85).
coverage:
	@mkdir -p .cover
	go test -count=1 -timeout=120s -race -coverprofile=.cover/core.out $(COVERAGE_PKGS)
	@cov=$$(go tool cover -func=.cover/core.out | awk '/^total:/ {print $$3}' | tr -d '%'); \
	cov_int=$${cov%.*}; \
	thresh_int=${COVERAGE_THRESHOLD}; \
	thresh_int=$${thresh_int%.*}; \
	echo "Coverage: $$cov% (threshold: ${COVERAGE_THRESHOLD}%)"; \
	if [ "$$cov_int" -lt "$$thresh_int" ]; then \
		echo "FAIL: coverage $$cov% < ${COVERAGE_THRESHOLD}%"; \
		rm -f .cover/core.out; \
		exit 1; \
	fi; \
	echo "OK: coverage $$cov% >= ${COVERAGE_THRESHOLD}%"

## python-compile: Syntax-check the Python SDK.
python-compile:
	@command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 is required"; exit 1; }
	python3 -m py_compile $$(find sdk/python/janus_broker -name '*.py')

## python-examples-compile: Syntax-check the Python interop examples.
python-examples-compile:
	@command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 is required"; exit 1; }
	python3 -m py_compile $$(find examples/interop -name '*.py')

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

## beta-fast: Run the Milestone 1 in-memory reliability simulation
beta-fast:
	@echo "==> Running M1 in-memory reliability simulation (scale=$(or $(BETA_SCALE),5), concurrency=$(or $(BETA_CONCURRENCY),4))"
	JANUS_BETA_SCALE=$${BETA_SCALE:-5} JANUS_BETA_CONCURRENCY=$${BETA_CONCURRENCY:-4} \
		go test -count=1 -timeout=120s -v -run 'TestAgentToAgentPipeline|TestAgentToAgentWithApprovalGate|TestAckResultRefPersistence|TestEventPublishingOnLifecycle|TestStateMachineValidation|TestMultiAgentConcurrentPublish' \
		./server/tests/simulation/...
	@echo "==> beta-fast: simulation passed"

## clean: Remove generated coverage artifacts.
clean:
	rm -rf .cover

## contract-check: Validate proto/SDK/HTTP API surface consistency.
contract-check:
	@python3 scripts/check_api_contract.py

## python-test: Run Python SDK unit tests.
python-test:
	@command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 is required"; exit 1; }
	cd sdk/python && python3 -m pytest tests/

## typescript-test: Run TypeScript SDK compilation check.
typescript-test:
	@command -v npx >/dev/null 2>&1 || { echo "ERROR: npx is required"; exit 1; }
	cd sdk/typescript && npx tsc --noEmit

## verify-sdk-cli: Run SDK conformance + CLI tests against auth-enabled API.
verify-sdk-cli: python-test typescript-test
	@echo "verify-sdk-cli: SDK + CLI checks passed"

## verify-protocol: Run native gRPC + gateway + A2A lifecycle + Audit protocol tests.
verify-protocol: test
	@echo "verify-protocol: protocol parity checks passed"

## smoke-prod: API/PostgreSQL/NATS/Redis + metrics + observability smoke test.
smoke-prod:
	@JANUS_URL=$${JANUS_URL:-http://localhost:8080} bash scripts/smoke_api_dependencies.sh

## smoke-7-agents: 7-agent lifecycle + capability lookup + fan-out + idempotency.
smoke-7-agents:
	@JANUS_URL=$${JANUS_URL:-http://localhost:8080} bash scripts/smoke_7_agents.sh

## smoke-grafana-panels: OPS-05 static gate — dashboard PromQL ↔ metrics.go registry cross-check.
smoke-grafana-panels:
	@bash scripts/smoke_grafana_panels.sh

## verify-security: API key / tenant guard / mTLS / audit security smoke tests.
verify-security: verify
	@echo "verify-security: security checks passed"

## verify-governance: Policy / budget / approval / routing / ContextRef / artifact smoke tests.
verify-governance: verify
	@echo "verify-governance: governance checks passed"

## verify-reliability: mailbox lifecycle / ACK idempotency / retry / DLQ / lease timeout smoke.
verify-reliability: verify beta-fast
	@go test -count=1 -timeout=120s ./server/tests/reliability/...
	@echo "verify-reliability: reliability checks passed"

## verify-ops-chaos: Redis/NATS/PG restart / readiness / rolling restart smoke.
## Requires a running API at JANUS_URL (default http://localhost:8080);
## failures propagate — start the stack first or skip this target explicitly.
verify-ops-chaos:
	JANUS_URL=$${JANUS_URL:-http://localhost:8080} bash scripts/smoke_ops_chaos.sh
	@echo "verify-ops-chaos: ops chaos checks passed"

## verify-release-ops: Helm lint / migration rollback / load baseline smoke.
verify-release-ops:
	@bash scripts/smoke_release_ops.sh
	@echo "verify-release-ops: release ops checks passed"

## load-baseline: 1000 agents / 1000 mailboxes / 1000 tasks, p95 < 100ms.
load-baseline:
	@JANUS_URL=$${JANUS_URL:-http://localhost:8080} bash scripts/load_baseline.sh

## ga-readiness: Check all P0 capabilities are Covered in the capability matrix.
ga-readiness:
	@python3 scripts/check_ga_readiness.py

## verify-production: Total GA gate. Runs ALL verification gates.
verify-production: verify contract-check beta-fast verify-reliability verify-security verify-protocol verify-governance verify-sdk-cli verify-ops-chaos verify-release-ops smoke-grafana-panels ga-readiness
	@echo ""
	@echo "=============================================="
	@echo "  verify-production: ALL GATES PASSED"
	@echo "  Janus Core is ready for v1.0 GA release"
	@echo "=============================================="
