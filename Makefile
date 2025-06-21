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

.PHONY: test-sparkplug
test-sparkplug:
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./sparkplug_plugin/...

.PHONY: test-sparkplug-unit
test-sparkplug-unit:
	@echo "Running Sparkplug unit tests..."
	@$(GINKGO_CMD) $(GINKGO_FLAGS) ./sparkplug_plugin/...

.PHONY: test-sparkplug-b-integration
test-sparkplug-b-integration:
	@echo "Running Sparkplug B integration tests (requires running Mosquitto broker)..."
	@echo "If Mosquitto is not running, start it with: make start-mosquitto"
	@TEST_SPARKPLUG_B=1 \
		$(GINKGO_CMD) $(GINKGO_FLAGS) ./sparkplug_plugin/...

.PHONY: start-mosquitto
start-mosquitto:
	@echo "Starting Mosquitto MQTT broker for integration tests..."
	@if ! docker ps | grep -q test-mosquitto; then \
		echo "Creating mosquitto.conf..."; \
		echo "listener 1883" > /tmp/mosquitto.conf; \
		echo "allow_anonymous true" >> /tmp/mosquitto.conf; \
		echo "Starting Mosquitto container..."; \
		docker run -d --name test-mosquitto -p 1883:1883 \
			-v /tmp/mosquitto.conf:/mosquitto/config/mosquitto.conf \
			eclipse-mosquitto:2.0; \
		echo "Waiting for Mosquitto to start..."; \
		sleep 2; \
		echo "Mosquitto is ready at localhost:1883"; \
	else \
		echo "Mosquitto is already running"; \
	fi

.PHONY: stop-mosquitto
stop-mosquitto:
	@echo "Stopping Mosquitto MQTT broker..."
	@docker stop test-mosquitto 2>/dev/null || true
	@docker rm test-mosquitto 2>/dev/null || true
	@echo "Mosquitto stopped"

.PHONY: test-sparkplug-b-full
test-sparkplug-b-full: start-mosquitto test-sparkplug-b-integration
	@echo "Sparkplug B integration tests completed"

.PHONY: test-sparkplug-bidirectional
test-sparkplug-bidirectional:
	@echo "Running Sparkplug B bidirectional communication tests (requires running Mosquitto broker)..."
	@echo "If Mosquitto is not running, start it with: make start-mosquitto"
	@TEST_SPARKPLUG_B=1 \
		$(GINKGO_CMD) $(GINKGO_FLAGS) --focus="Bidirectional" ./sparkplug_plugin/...

.PHONY: test-sparkplug-bidirectional-full
test-sparkplug-bidirectional-full: start-mosquitto test-sparkplug-bidirectional
	@echo "Sparkplug B bidirectional communication tests completed"
	@echo "🚀 P2.5 Bidirectional Communication Validation - PASSED"


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

# Test Sparkplug device discovery on broker.hivemq.com
.PHONY: test-benthos-sparkplug-device-discovery
test-benthos-sparkplug-device-discovery:
	@$(GINKGO_CMD) -r --output-interceptor-mode=none --github-output -vv -trace --tags=integration --focus="Device Discovery" ./sparkplug_plugin/...

# Test Sparkplug device publishing on broker.hivemq.com
.PHONY: test-benthos-sparkplug-device-publisher
test-benthos-sparkplug-device-publisher:
	@$(GINKGO_CMD) -r --output-interceptor-mode=none --github-output -vv -trace --tags=integration --focus="Device Publisher" ./sparkplug_plugin/...

# Test both Sparkplug discovery and publishing in parallel
.PHONY: test-benthos-sparkplug-parallel
test-benthos-sparkplug-parallel:
	@echo "🚀 Running Sparkplug discovery and publishing tests in parallel..."
	@echo "📡 Discovery test will listen for devices while publisher test publishes data"
	@echo "🔄 This tests real-world Sparkplug B communication between components"
	$(GINKGO_CMD) -r --output-interceptor-mode=none --github-output -v -trace --tags=integration --focus="Sparkplug.*Integration Test" --procs=2 ./sparkplug_plugin/...
