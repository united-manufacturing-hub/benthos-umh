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

FROM management.umh.app/oci/library/golang:1.24 as build

RUN useradd -u 10001 benthos

WORKDIR /go/src/github.com/united-manufacturing-hub/benthos-umh

ENV GOPROXY=https://golangproxy.umh.app,https://proxy.golang.org,direct

COPY go.mod go.sum ./
RUN go mod download

COPY ./cmd ./cmd
COPY ./opcua_plugin ./opcua_plugin
COPY ./s7comm_plugin ./s7comm_plugin
COPY ./modbus_plugin ./modbus_plugin
COPY ./nodered_js_plugin ./nodered_js_plugin
COPY ./tag_processor_plugin ./tag_processor_plugin
COPY ./sensorconnect_plugin ./sensorconnect_plugin
COPY ./eip_plugin ./eip_plugin

ENV CGO_ENABLED=0
RUN go build \
    -ldflags "-s -w \
    -X github.com/redpanda-data/benthos/v4/internal/cli.Version=${APP_VERSION} \
    -X github.com/redpanda-data/benthos/v4/internal/cli.DateBuilt=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o benthos \
    cmd/benthos/main.go

FROM management.umh.app/oci/library/busybox as app

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/united-manufacturing-hub/benthos-umh/benthos benthos
COPY ./config/default.yaml /benthos.yaml
COPY ./templates /templates
COPY ./proto /proto

ENTRYPOINT ["/benthos"]

CMD ["-c", "/benthos.yaml", "-t", "/templates/*.yaml"]

EXPOSE 4195

USER benthos

LABEL org.opencontainers.image.source=https://github.com/united-manufacturing-hub/benthos-umh

