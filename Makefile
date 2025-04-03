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

.PHONY:
clean:
	@rm -rf target tmp/bin tmp/benthos-*.zip

.PHONY:
target:
	@mkdir -p tmp/bin
	@go build \
       -ldflags "-s -w \
       -X github.com/redpanda-data/benthos/v4/internal/cli.Version=temp \
       -X github.com/redpanda-data/benthos/v4/internal/cli.DateBuilt=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
       -o tmp/bin/benthos \
       cmd/benthos/main.go

.PHONY:
setup-test-deps:
	@go get -t ./...
	@go mod tidy

.PHONY:
test: setup-test-deps
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace -p --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./...

.PHONY:
update-benthos:
	@go get github.com/redpanda-data/connect/public/bundle/free/v4@latest && \
  go get github.com/redpanda-data/connect/v4@latest && \
  go get github.com/redpanda-data/benthos/v4@latest && \
  go mod tidy

# provides serial runners, which are needed to restrict data requests on sensor interface
# Note: Serial execution will increase test duration but ensures reliable results
.PHONY:
test-serial:
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --procs 1 --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./...

.PHONY:
test-sensorconnect:
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --procs 1 --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./sensorconnect_plugin/...

.PHONY:
test-noderedjs:
	@TEST_NODERED_JS=true ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./nodered_js_plugin/...

.PHONY:
test-s7comm:
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./s7comm_plugin/...

.PHONY:
test-modbus:
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./modbus_plugin/...

.PHONY:
test-opc:
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./opcua_plugin/...

.PHONY:
test-eip:
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --randomize-all --cover --coverprofile=cover.profile  ./eip_plugin/...

.PHONY:
test-tag-processor:
	@TEST_TAG_PROCESSOR=true ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./tag_processor_plugin/...


##### TESTS WITH RUNNING BENTHOS-UMH
# Test the tag processor with a local OPC UA server
.PHONY:
test-benthos-tag-processor: target
	./tmp/bin/benthos -c ./config/tag-processor-test.yaml

	# usage:
	# make test-sensorconnect TEST_DEBUG_IFM_ENDPOINT=(IP of sensor interface)
.PHONY:
test-benthos-sensorconnect: test-serial
	./tmp/bin/benthos -c ./config/sensorconnect-test.yaml
