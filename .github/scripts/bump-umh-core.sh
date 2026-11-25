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

# Sets BENTHOS_UMH_VERSION in an umh-core checkout's Makefile to VERSION
# (vX.Y.Z), without the v.
# Usage:
#   VERSION=vX.Y.Z .github/scripts/bump-umh-core.sh <umh-core dir>

set -euo pipefail

fail() { echo "::error::$*" >&2; exit 1; }

dir="${1:-}"
[ -n "$dir" ] || fail "usage: VERSION=vX.Y.Z $0 <umh-core dir>"
version="${VERSION:-}"
version="${version#v}"

cd "$dir"

# -i.bak, not -i: BSD sed needs the suffix, so the script also runs on macOS.
sed -i.bak "s/^BENTHOS_UMH_VERSION = .*/BENTHOS_UMH_VERSION = ${version}/" Makefile
rm Makefile.bak
grep -q "^BENTHOS_UMH_VERSION = ${version}$" Makefile || fail "BENTHOS_UMH_VERSION in $dir/Makefile is not '${version}'"
