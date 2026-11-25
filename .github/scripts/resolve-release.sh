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

# Finds out what a push to staging releases, for release.yml.
#
# The release PR (prepare-release.yml) is one commit on top of the staging
# commit S it was generated from. It moves the fragments into
# .changelog/vX.Y.Z.md and .changelog/vX.Y.Z/ and regenerates CHANGELOG.md. Its
# merge commit M adds the version file, so S is M^2^. The release ships S,
# whose code the changelog describes exactly; PRs merged after S ship with the
# next release.
#
# Env:     BEFORE, AFTER  commit range of the push; needs the full history
# Outputs: release (true/false), merge (M), base (S), version (vX.Y.Z), written
#          to $GITHUB_OUTPUT, or stdout when unset
# Runnable locally:
#   BEFORE=<sha> AFTER=<sha> .github/scripts/resolve-release.sh

set -euo pipefail

OUTPUT="${GITHUB_OUTPUT:-/dev/stdout}"
VERSION_FILE_RE='^\.changelog/v[0-9]+\.[0-9]+\.[0-9]+\.md$'

fail() {
  echo "::error::$1" >&2
  exit 1
}

added_version_files() {
  git diff --no-renames --diff-filter=A --name-only "$1^1" "$1" -- .changelog | grep -E "$VERSION_FILE_RE" || true
}

# The newest release PR merge in the push wins.
M=""
for commit in $(git rev-list --first-parent "${BEFORE:?}..${AFTER:?}"); do
  [ -n "$(added_version_files "$commit")" ] || continue
  # Only a merge commit names S. A squash merge would ship the staging tip,
  # including PRs that are not in the changelog.
  git rev-parse --verify --quiet "${commit}^2" >/dev/null \
    || fail "${commit} adds a version file but is not a merge commit. Merge the release PR with a merge commit."
  M="$commit"
  break
done
if [ -z "$M" ]; then
  echo "release=false" >> "$OUTPUT"
  exit 0
fi

S="$(git rev-parse "${M}^2^")"
git merge-base --is-ancestor "$S" "${M}^1" \
  || fail "The release PR's base ${S} is not on staging. A release PR is one commit on top of staging."
other="$(git diff --name-only "$S" "${M}^2" | grep -vE '^(\.changelog/|CHANGELOG\.md$)' || true)"
[ -z "$other" ] || fail "The release PR changes files outside the changelog: ${other//$'\n'/ }"
added="$(added_version_files "$M")"
[ "$(printf '%s\n' "$added" | wc -l)" -eq 1 ] || fail "The release PR adds more than one version file: ${added//$'\n'/ }"
version="${added#.changelog/}"

{
  echo "release=true"
  echo "merge=${M}"
  echo "base=${S}"
  echo "version=${version%.md}"
} >> "$OUTPUT"
