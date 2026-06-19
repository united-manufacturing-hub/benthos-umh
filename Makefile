# Copyright 2023 UMH Systems GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include ./Makefile.Common

GINKGO_CMD=go run github.com/onsi/ginkgo/v2/ginkgo
GINKGO_FLAGS=-r --output-interceptor-mode=none -trace -p --randomize-all --cover --coverprofile=cover.profile
GINKGO_SERIAL_FLAGS=$(GINKGO_FLAGS) --procs=1

GOTESTSUM_FLAGS=--format pkgname -- -race -coverprofile=cover.profile

BENTHOS_BIN := tmp/bin/benthos
LOG_LEVEL ?= INFO
CONFIG ?= ./config/opcua-hex-test.yaml

# uns_beta pre-build key filter (ENG-5105): a perf-only patch to the official
# redpanda Connect ordered franz reader that omits records a downstream key
# filter would drop, before their message is built. We do not fork connect; the
# diff lives in patches/ and is applied at build time onto a throwaway copy of
# the pinned module. The replace is written to a SIDE MODFILE (go.patched.mod),
# NOT the committed go.mod, so the committed go.mod NEVER changes and no commit
# can ever capture the local replace. Patched build/test runs set
# GOFLAGS=-modfile=go.patched.mod. `git apply --check` is the drift gate: a
# Renovate bump that moves the code under the patch fails the build loudly. The
# copy is git-baselined so the patch can always be regenerated with `git diff`.
CONNECT_PKG := github.com/redpanda-data/connect/v4
CONNECT_PATCH := patches/connect-key-filter.patch
CONNECT_VENDOR := .connect-patched
PATCHED_MODFILE := go.patched.mod
PATCHED_SUMFILE := go.patched.sum

.PHONY: all
all: clean build

.PHONY: install
install: install-tools

.PHONY: clean
clean:
	rm -rf tmp/bin tmp/benthos-*.zip .tools/*

.PHONY: run
run:
	go run cmd/benthos/main.go run --log.level $(LOG_LEVEL) $(CONFIG)

# build depends on patch-connect: the shipped binary registers uns_beta, whose
# template wires key_pattern — a field that only exists on the patched connect
# reader (ENG-5105). An unpatched binary would fail at runtime when a user
# configures uns_beta, so every build is patched. The side modfile keeps the
# committed go.mod clean; the patch-connect drift gate fails the build loudly on
# a connect bump that moves the patched code.
.PHONY: build
build: patch-connect
	@mkdir -p $(dir $(BENTHOS_BIN))
	@GOFLAGS=-modfile=$(PATCHED_MODFILE) go build \
       -ldflags "-s -w \
       -X github.com/redpanda-data/benthos/v4/internal/cli.Version=temp \
       -X github.com/redpanda-data/benthos/v4/internal/cli.DateBuilt=$$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
       -o $(BENTHOS_BIN) \
       cmd/benthos/main.go

.PHONY: patch-connect
patch-connect:
	@CONNECT_SRC=$$(go list -m -f '{{.Dir}}' $(CONNECT_PKG)) && \
	rm -rf $(CONNECT_VENDOR) && mkdir -p $(CONNECT_VENDOR) && \
	cp -R "$$CONNECT_SRC"/. $(CONNECT_VENDOR)/ && \
	chmod -R u+w $(CONNECT_VENDOR) && \
	( cd $(CONNECT_VENDOR) && git init -q && git add -A && \
	  git -c user.email=patch@umh -c user.name=patch commit -qm base && \
	  git apply --check -p1 ../$(CONNECT_PATCH) && \
	  git apply -p1 ../$(CONNECT_PATCH) ) && \
	cp go.mod $(PATCHED_MODFILE) && cp go.sum $(PATCHED_SUMFILE) && \
	go mod edit -replace $(CONNECT_PKG)=./$(CONNECT_VENDOR) $(PATCHED_MODFILE) && \
	echo "patched $(CONNECT_PKG) -> ./$(CONNECT_VENDOR); committed go.mod untouched." && \
	echo "build/test patched with: GOFLAGS=-modfile=$(PATCHED_MODFILE) go test -tags connect_patched ./uns_plugin/..."

# Drift gate only: verify the diff still applies to the pinned module without
# touching go.mod. CI runs this so a Renovate bump that moves connect fails loud.
.PHONY: check-connect-patch
check-connect-patch:
	@CONNECT_SRC=$$(go list -m -f '{{.Dir}}' $(CONNECT_PKG)) && \
	rm -rf $(CONNECT_VENDOR) && mkdir -p $(CONNECT_VENDOR) && \
	cp -R "$$CONNECT_SRC"/. $(CONNECT_VENDOR)/ && \
	chmod -R u+w $(CONNECT_VENDOR) && \
	( cd $(CONNECT_VENDOR) && git init -q && git apply --check -p1 ../$(CONNECT_PATCH) ) && \
	echo "connect patch applies cleanly to $$(go list -m -f '{{.Version}}' $(CONNECT_PKG))"

.PHONY: unpatch-connect
unpatch-connect:
	@rm -f $(PATCHED_MODFILE) $(PATCHED_SUMFILE) && \
	rm -rf $(CONNECT_VENDOR) && \
	echo "removed $(PATCHED_MODFILE)/$(PATCHED_SUMFILE) and $(CONNECT_VENDOR); committed go.mod was never touched"

# Run the uns_beta suite against the patched build (side modfile keeps the
# committed go.mod clean; connect_patched tag pulls in the alloc-budget gate).
.PHONY: test-uns-patched
test-uns-patched: patch-connect
	@GOFLAGS=-modfile=$(PATCHED_MODFILE) go test -tags connect_patched -count=1 -timeout=900s ./uns_plugin/... -args -ginkgo.label-filter='uns_beta'

.PHONY: lint
lint: $(LINT)
	$(LINT) run

.PHONY: lint-fix
lint-fix: $(LINT)
	$(LINT) run --fix

.PHONY: fmt
fmt: $(GOFUMPT) $(GCI)
	$(GOFUMPT) -w .
	$(GCI) write --skip-generated -s standard -s default -s 'prefix(github.com/united-manufacturing-hub/benthos-umh)' .

.PHONY: license-fix
license-fix:
	@$(LICENSE_EYE) header fix

.PHONY: license-check
license-check:
	@$(LICENSE_EYE) header check

.PHONY: setup-test-deps
setup-test-deps:
	@go get -t ./...
	@go mod tidy

.PHONY: update-benthos
update-benthos:
	@go get github.com/redpanda-data/connect/public/bundle/free/v4@latest && \
  go get github.com/redpanda-data/connect/v4@latest && \
  go get github.com/redpanda-data/benthos/v4@latest && \
  go mod tidy

.PHONY: test
test: setup-test-deps
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./...

.PHONY: test-schema-export
test-schema-export: $(GOTESTSUM)
	@$(GOTESTSUM) $(GOTESTSUM_FLAGS) ./cmd/schema-export/...

.PHONY: test-eip
test-eip:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./eip_plugin/...

.PHONY: test-modbus
test-modbus:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./modbus_plugin/...

.PHONY: test-noderedjs
test-noderedjs:
	@TEST_NODERED_JS=true \
		$(GINKGO_CMD) $(GINKGO_FLAGS) ./nodered_js_plugin/...

.PHONY: test-unit-opc
test-unit-opc:
	@TEST_OPCUA_UNIT=true \
		$(GINKGO_CMD) $(GINKGO_FLAGS) ./opcua_plugin/...

.PHONY: test-integration-opc
test-integration-opc:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./opcua_plugin/...

# The uns_beta specs render the template, which wires key_pattern — a field that
# only exists on the patched connect reader. They therefore run in the patched
# lane (test-uns-patched); excluding them here keeps the fast unpatched lane for
# the legacy uns input green.
.PHONY: test-uns
test-uns:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) --label-filter='!redpanda && !uns_beta' ./uns_plugin/...

.PHONY: test-uns-redpanda
test-uns-redpanda:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) --label-filter='!uns_beta' ./uns_plugin/...

.PHONY: test-s7comm
test-s7comm:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./s7comm_plugin/...

.PHONY: test-sensorconnect
test-sensorconnect:
	@$(GINKGO_CMD) $(GINKGO_SERIAL_FLAGS) ./sensorconnect_plugin/...

.PHONY: test-stream-processor
test-stream-processor:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) --race ./stream_processor_plugin/...

.PHONY: fuzz-stream-processor
fuzz-stream-processor:
	@echo "Running fuzz tests for stream processor (press Ctrl+C to stop)..."
	@cd stream_processor_plugin && go test -tags=fuzz -fuzz=FuzzStreamProcessor

.PHONY: test-tag-processor
test-tag-processor:
	@TEST_TAG_PROCESSOR=true \
		$(GINKGO_CMD) $(GINKGO_FLAGS) ./tag_processor_plugin/...

.PHONY: test-snowflake-put
test-snowflake-put: $(GOTESTSUM)
	@$(GOTESTSUM) $(GOTESTSUM_FLAGS) ./snowflake_put_plugin/...

.PHONY: test-sparkplug
test-sparkplug:
	$(GINKGO_CMD) $(GINKGO_FLAGS) ./sparkplug_plugin/...

.PHONY: test-classic-to-core
test-classic-to-core:
	@TEST_CLASSIC_TO_CORE=1 \
		$(GINKGO_CMD) $(GINKGO_FLAGS) ./classic_to_core_plugin/...

.PHONY: test-downsampler
test-downsampler:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./downsampler_plugin/...

.PHONY: test-pkg-umh-topic
test-pkg-umh-topic:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./pkg/umh/topic/...

.PHONY: bench-pkg-umh-topic
bench-pkg-umh-topic:
	go test -bench=. -benchmem ./pkg/umh/topic/...

.PHONY: bench-stream-processor
bench-stream-processor:
	go test -bench=. -benchmem -benchtime=10s ./stream_processor_plugin/...

.PHONY: test-topic-browser
test-topic-browser:
	@TEST_TOPIC_BROWSER=1 \
		$(GINKGO_CMD) $(GINKGO_FLAGS) ./topic_browser_plugin/...

# Generate Go files from protobuf for topic browser
.PHONY: proto
proto:
	rm pkg/umh/topic/proto/topic_browser_data.pb.go 2>/dev/null || true
	$(PROTOC) \
		-I=pkg/umh/topic/proto \
		--go_out=pkg/umh/topic/proto \
		pkg/umh/topic/proto/topic_browser_data.proto
	@echo "Successfully generated topic_browser_data.pb.go"

# Generate UMH plugin list and schema files for ManagementConsole
.PHONY: generate-schema
generate-schema:
ifndef VERSION
	$(error VERSION is required. Usage: make generate-schema VERSION=0.11.7)
endif
	go run ./cmd/tools/generate_plugins .
	go build -o ./tmp/schema-export ./cmd/schema-export
	./tmp/schema-export -version $(VERSION) -format benthos
	./tmp/schema-export -version $(VERSION) -format json-schema
	./tmp/schema-export -version $(VERSION) -format mapping

.PHONY: changelog
changelog: $(CHANGIE)
	$(CHANGIE) new

.PHONY: changelog-merge
changelog-merge: $(CHANGIE)
ifndef VERSION
	$(error VERSION is required. Usage: make changelog-merge VERSION=v0.13.0)
endif
	$(CHANGIE) batch $(VERSION) --force
	$(CHANGIE) merge

.PHONY: changelog-diff
changelog-diff: $(CHANGIE)
	@$(CHANGIE) diff 1 | tail -n +2

.PHONY: serve-pprof
serve-pprof:
	@export PATH="$(TOOLS_BIN_DIR)/graphviz:$$PATH" && \
		if ! which dot > /dev/null; then \
			echo "error: dot not installed (run make install)" >&2; \
			exit 1; \
		fi && \
		go tool pprof -http=:8080 "localhost:4195/debug/pprof/profile?seconds=20"
