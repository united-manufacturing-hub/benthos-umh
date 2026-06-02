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

# Integration-test fixture: a headless Atlas Copco Open Protocol controller.
#
# The actual protocol emulator is the third-party GPL-3.0 project
# art-x/open-protocol-emulator. It is fetched here at build time and is NOT
# vendored into this Apache-2.0 repository. Only `headless_runner.py`
# (UMH-authored, Apache-2.0) lives in this repo.
FROM python:3.13-slim

ARG EMULATOR_REPO=https://github.com/art-x/open-protocol-emulator.git
# Pin to a commit for reproducibility; override at build time if needed.
ARG EMULATOR_REF=main

# git: to fetch the emulator at build time.
# python3-tk: the emulator imports tkinter at module scope (the GUI is never
# launched in headless mode, but the import must succeed).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git python3-tk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Fetch the GPL emulator source at build time (not stored in our repo).
RUN git clone --depth 1 --branch "${EMULATOR_REF}" "${EMULATOR_REPO}" /tmp/op-emu \
    && cp /tmp/op-emu/open_protocol_emulator.py /app/open_protocol_emulator.py \
    && rm -rf /tmp/op-emu

# Our Apache-licensed headless entrypoint.
COPY headless_runner.py /app/headless_runner.py

EXPOSE 4545

ENTRYPOINT ["python", "/app/headless_runner.py"]
CMD ["--port", "4545", "--name", "UMHTestSim"]
