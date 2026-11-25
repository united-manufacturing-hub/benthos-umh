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

# Bumps benthos-umh in an umh-core checkout to VERSION (vX.Y.Z):
#   1. Sets BENTHOS_UMH_VERSION in umh-core/Makefile.
#   2. Runs umh-core's `make benthosmetrics-fixture`, which re-records
#      pkg/benthosmetrics/testdata/metrics.txt from the released image and
#      writes the version to pkg/benthosmetrics/testdata/VERSION. umh-core's
#      fixture spec fails until that file equals the Makefile pin.
# Usage:
#   VERSION=vX.Y.Z .github/scripts/bump-umh-core.sh <umh-core dir>

set -euo pipefail

fail() { echo "::error::$*" >&2; exit 1; }

dir="${1:-}"
[ -n "$dir" ] || fail "usage: VERSION=vX.Y.Z $0 <umh-core dir>"
[[ "${VERSION:-}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "VERSION must be vX.Y.Z, got '${VERSION:-}'"
version="${VERSION#v}"
fixture_version=pkg/benthosmetrics/testdata/VERSION

# cd, not make -C: the fixture target mounts its config from $(PWD).
cd "$dir"

grep -q '^BENTHOS_UMH_VERSION = ' Makefile || fail "no 'BENTHOS_UMH_VERSION = ' line in $dir/Makefile"
# -i.bak, not -i: BSD sed needs the suffix, so the script also runs on macOS.
sed -i.bak "s/^BENTHOS_UMH_VERSION = .*/BENTHOS_UMH_VERSION = ${version}/" Makefile
rm Makefile.bak

[ -f "$fixture_version" ] || fail "$dir/$fixture_version does not exist; did umh-core move the benthosmetrics fixture?"
# Removed first, so the check below proves the target wrote it in this run.
rm "$fixture_version"
make benthosmetrics-fixture \
  || fail "make benthosmetrics-fixture failed in $dir. The log above shows whether the image did not pull or benthos did not start with pkg/benthosmetrics/testdata/capture.yaml."

[ -f "$fixture_version" ] || fail "make benthosmetrics-fixture did not write $dir/$fixture_version; did umh-core change the target?"
recorded="$(tr -d '[:space:]' < "$fixture_version")"
[ "$recorded" = "$version" ] || fail "$dir/$fixture_version is '$recorded', want '$version'"
