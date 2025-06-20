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
target:
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
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./uns_plugin/...

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

.PHONY: test-downsampler
test-downsampler:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./downsampler_plugin/...


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