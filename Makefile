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

GINKGO_CMD=ginkgo
GINKGO_FLAGS=-r --output-interceptor-mode=none --github-output -vv -trace -p --randomize-all --cover --coverprofile=cover.profile --repeat=2
GINKGO_SERIAL_FLAGS=$(GINKGO_FLAGS) --procs=1

BENTHOS_BIN := tmp/bin/benthos

.PHONY: all
all: clean target

.PHONY: clean
clean:
	@rm -rf target tmp/bin tmp/benthos-*.zip

.PHONY: target
target: build-protobuf
	@mkdir -p $(dir $(BENTHOS_BIN))
	@go build \
       -ldflags "-s -w \
       -X github.com/redpanda-data/benthos/v4/internal/cli.Version=temp \
       -X github.com/redpanda-data/benthos/v4/internal/cli.DateBuilt=$$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
       -o $(BENTHOS_BIN) \
       cmd/benthos/main.go

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

.PHONY: test-opc
test-opc:
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

.PHONY: test-tag-processor
test-tag-processor:
	@TEST_TAG_PROCESSOR=true \
		$(GINKGO_CMD) $(GINKGO_FLAGS) ./tag_processor_plugin/...

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
  
.PHONY: test-topic-browser
test-topic-browser:
	@TEST_TOPIC_BROWSER=1 \
		$(GINKGO_CMD) $(GINKGO_FLAGS) ./topic_browser_plugin/...

###### TESTS WITH RUNNING BENTHOS-UMH #####
# Test the tag processor with a local OPC UA server
.PHONY: test-benthos-tag-processor
test-benthos-tag-processor: target
	@$(BENTHOS_BIN) -c ./config/tag-processor-test.yaml

# USAGE:
# make test-benthos-sensorconnect TEST_DEBUG_IFM_ENDPOINT=(IP of sensor interface)
.PHONY: test-benthos-sensorconnect
test-benthos-sensorconnect: target
	@$(BENTHOS_BIN) -c ./config/sensorconnect-test.yaml

.PHONY: test-benthos-downsampler
test-benthos-downsampler: target
	@$(BENTHOS_BIN) -c ./config/downsampler_example.yaml

.PHONY: test-benthos-downsampler-example-one
test-benthos-downsampler-example-one: target
	@$(BENTHOS_BIN) -c ./config/downsampler_example_one.yaml

.PHONY: test-benthos-downsampler-example-one-timeout
test-benthos-downsampler-example-one-timeout: target
	@$(BENTHOS_BIN) -c ./config/downsampler_example_one_timeout.yaml

.PHONY: test-benthos-topic-browser
test-benthos-topic-browser: target
	@$(BENTHOS_BIN) -c ./config/topic-browser-test.yaml

## Generate go files from protobuf for topic browser
build-protobuf:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	rm pkg/umh/topic/proto/topic_browser_data.pb.go || true
	protoc \
		-I=pkg/umh/topic/proto \
		--go_out=pkg/umh/topic/proto \
		pkg/umh/topic/proto/topic_browser_data.proto
	@echo '// Copyright 2025 UMH Systems GmbH\n//\n// Licensed under the Apache License, Version 2.0 (the "License");\n// you may not use this file except in compliance with the License.\n// You may obtain a copy of the License at\n//\n//     http://www.apache.org/licenses/LICENSE-2.0\n//\n// Unless required by applicable law or agreed to in writing, software\n// distributed under the License is distributed on an "AS IS" BASIS,\n// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n// See the License for the specific language governing permissions and\n// limitations under the License.\n\n' | cat - pkg/umh/topic/proto/topic_browser_data.pb.go > temp && mv temp pkg/umh/topic/proto/topic_browser_data.pb.go
