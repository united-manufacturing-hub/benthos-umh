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

all: clean target

clean:
	@rm -rf target tmp/bin tmp/benthos-*.zip

target:
	@mkdir -p tmp/bin
	@go build \
       -ldflags "-s -w \
       -X github.com/redpanda-data/benthos/v4/internal/cli.Version=temp \
       -X github.com/redpanda-data/benthos/v4/internal/cli.DateBuilt=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
       -o tmp/bin/benthos \
       cmd/benthos/main.go

test:
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace -p --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./...

update-benthos:
	@go get github.com/redpanda-data/connect/public/bundle/free/v4@latest && \
  go get github.com/redpanda-data/connect/v4@latest && \
  go get github.com/redpanda-data/benthos/v4@latest && \
  go mod tidy

## provides serial runners, which are needed to restrict data requests on sensor interface
test-serial:
	@ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --procs 1 --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./...

# usage:
# make test-sensorconnect TEST_DEBUG_IFM_ENDPOINT=(IP of sensor interface)
test-sensorconnect: test-serial
	./tmp/bin/benthos -c ./config/sensorconnect-test.yaml

# run the tests against the opc-plc simulator
# this will be the default simulator when starting up the devcontainer
test-opc-plc:
	@TEST_OPCUA_SIMULATOR=true ginkgo -r --output-interceptor-mode=none --github-output -vv -trace --randomize-all --cover --coverprofile=cover.profile --repeat=2 ./opcua_plugin/...

# Test the tag processor with a local OPC UA server
test-tag-processor: target
	./tmp/bin/benthos -c ./config/tag-processor-test.yaml

.PHONY: clean target test update-benthos test-opc-plc test-tag-processor
