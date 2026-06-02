# Copyright 2025 UMH Systems GmbH
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

# Integration-test fixture: a second, independent Open Protocol controller.
#
# This builds the third-party Rust simulator Jarrekstar/open-protocol-device-
# simulator (dual MIT/Apache-2.0). It is fetched at build time and NOT vendored
# into this repository. It serves Open Protocol over TCP (:8080) and a REST/HTTP
# control API (:8081). Unlike the Python emulator, it does not auto-push results
# until auto-tightening is started via POST :8081/auto-tightening/start.
#
# Notably this simulator sends MID 0061 with a spec-correct 10-digit tightening
# ID, making it a useful cross-check of the plugin's parser against a second
# independent implementation.
FROM rust:slim

# git: fetch the simulator; build-essential: rusqlite "bundled" compiles SQLite.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG SIM_REPO=https://github.com/Jarrekstar/open-protocol-device-simulator.git
ARG SIM_REF=main

RUN git clone --depth 1 --branch "${SIM_REF}" "${SIM_REPO}" . \
    && cargo build --release --bin open-protocol-device-simulator

EXPOSE 8080 8081

ENTRYPOINT ["/app/target/release/open-protocol-device-simulator"]
CMD ["--bind-address", "0.0.0.0", "--tcp-port", "8080", "--http-port", "8081"]
