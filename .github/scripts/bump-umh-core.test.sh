#!/usr/bin/env bash
# Copyright 2026 UMH Systems GmbH
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

# Tests for bump-umh-core.sh in throwaway umh-core directories. Their
# benthosmetrics-fixture target writes VERSION as umh-core's does, through
# $(PWD), but records no fixture, so no Docker is needed.
# Runnable locally:
#   .github/scripts/bump-umh-core.test.sh

set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bump-umh-core.sh"
ROOT="$(mktemp -d)"
trap 'rm -rf "$ROOT"' EXIT
FAILED=0

# A umh-core directory pinned to 0.17.0. $1 is the fixture target's recipe;
# the default writes the pinned version the way umh-core's target does.
new_core() {
  local recipe="${1:-echo \$(BENTHOS_UMH_VERSION) > \$(PWD)/pkg/benthosmetrics/testdata/VERSION}"
  CORE="$(mktemp -d "$ROOT/core.XXXXXX")"
  mkdir -p "$CORE/pkg/benthosmetrics/testdata"
  echo 0.17.0 > "$CORE/pkg/benthosmetrics/testdata/VERSION"
  printf 'REDPANDA_VERSION = 24.3.18\nBENTHOS_UMH_VERSION = 0.17.0\nBUF_VERSION = 1.55.1\n\nbenthosmetrics-fixture:\n\t%s\n' \
    "$recipe" > "$CORE/Makefile"
}

# Runs the script for version $1 from another directory; prints the exit code.
# With $2, also prints whether stderr contains $2, so each failure case proves
# which check stopped it.
run() {
  local rc=0 err
  err="$(cd "$ROOT" && VERSION="$1" "$SCRIPT" "$CORE" 2>&1 >/dev/null)" || rc=$?
  if [ -z "${2:-}" ]; then echo "$rc"
  elif [[ "$err" == *"$2"* ]]; then echo "$rc matched"
  else echo "$rc stderr: $err"; fi
}

pin() { sed -n 's/^BENTHOS_UMH_VERSION = //p' "$CORE/Makefile"; }
fixture() { cat "$CORE/pkg/benthosmetrics/testdata/VERSION"; }

check() {
  if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: want '$2', got '$3'"; FAILED=1; fi
}

new_core
check "a bump succeeds" "0" "$(run v0.17.1)"
check "a bump sets the Makefile pin without the v" "0.17.1" "$(pin)"
check "a bump writes the metrics fixture version without the v" "0.17.1" "$(fixture)"
check "a bump leaves the other Makefile lines alone" "REDPANDA_VERSION = 24.3.18 BUF_VERSION = 1.55.1" \
  "$(grep -E '^(REDPANDA|BUF)_VERSION' "$CORE/Makefile" | tr '\n' ' ' | sed 's/ $//')"

new_core
check "a re-run at the pinned version succeeds" "0" "$(run v0.17.0)"

new_core
check "a version without the v prefix fails" "1 matched" "$(run 0.17.1 "VERSION must be vX.Y.Z")"
check "a rejected version leaves the Makefile pin alone" "0.17.0" "$(pin)"
new_core
check "a version that is not X.Y.Z fails" "1 matched" "$(run v0.17 "VERSION must be vX.Y.Z")"
new_core
check "an empty version fails" "1 matched" "$(run "" "VERSION must be vX.Y.Z")"

new_core; CORE=""
check "a run without the umh-core directory fails" "1 matched" "$(run v0.17.1 "usage:")"

new_core; sed -i.bak '/^BENTHOS_UMH_VERSION/d' "$CORE/Makefile"
check "a Makefile without BENTHOS_UMH_VERSION fails" "1 matched" "$(run v0.17.1 "no 'BENTHOS_UMH_VERSION = ' line")"

new_core; rm "$CORE/pkg/benthosmetrics/testdata/VERSION"
check "a missing metrics fixture version fails" "1 matched" "$(run v0.17.1 "did umh-core move the benthosmetrics fixture")"

new_core "exit 1"
check "a failing fixture target fails" "1 matched" "$(run v0.17.1 "make benthosmetrics-fixture failed")"

new_core "true"
check "a fixture target that does not write the version fails" "1 matched" "$(run v0.17.0 "did not write")"

new_core "echo 0.16.2 > \$(PWD)/pkg/benthosmetrics/testdata/VERSION"
check "a fixture target that writes another version fails" "1 matched" "$(run v0.17.1 "is '0.16.2', want '0.17.1'")"

new_core; printf 'BENTHOS_UMH_VERSION = 0.17.0\n' > "$CORE/Makefile"
check "a Makefile without the fixture target fails" "1 matched" "$(run v0.17.1 "make benthosmetrics-fixture failed")"
check "a failed bump leaves the metrics fixture version unwritten" "" \
  "$(cat "$CORE/pkg/benthosmetrics/testdata/VERSION" 2>/dev/null || true)"

exit "$FAILED"
