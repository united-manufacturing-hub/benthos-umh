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

BENTHOS_BIN := tmp/bin/benthos
LOG_LEVEL ?= INFO
CONFIG ?= ./config/opcua-hex-test.yaml

.PHONY: all
all: clean build

.PHONY: install
install: install-tools

.PHONY: clean
clean:
	@rm -rf tmp/bin tmp/benthos-*.zip

.PHONY: run
run:
	go run cmd/benthos/main.go run --log.level $(LOG_LEVEL) $(CONFIG)

.PHONY: build
build:
	@mkdir -p $(dir $(BENTHOS_BIN))
	@go build \
       -ldflags "-s -w \
       -X github.com/redpanda-data/benthos/v4/internal/cli.Version=temp \
       -X github.com/redpanda-data/benthos/v4/internal/cli.DateBuilt=$$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
       -o $(BENTHOS_BIN) \
       cmd/benthos/main.go

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

.PHONY: test-uns
test-uns:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) --label-filter='!redpanda' ./uns_plugin/...

.PHONY: test-uns-redpanda
test-uns-redpanda:
	@$(GINKGO_CMD) $(GINKGO_FLAGS)  ./uns_plugin/...

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

.PHONY: serve-pprof
serve-pprof:
	@export PATH="$(TOOLS_BIN_DIR)/graphviz:$$PATH" && \
		if ! which dot > /dev/null; then \
			echo "error: dot not installed (run make install)" >&2; \
			exit 1; \
		fi && \
		go tool pprof -http=:8080 "localhost:4195/debug/pprof/profile?seconds=20"
