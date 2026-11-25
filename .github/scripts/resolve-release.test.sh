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

# Tests for resolve-release.sh in throwaway repositories that mimic staging:
# PRs land as merge commits, and a release PR is one commit on top of staging.
# Runnable locally:
#   .github/scripts/resolve-release.test.sh

set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/resolve-release.sh"
# The cases read the script's outputs from stdout, which it uses only when
# GITHUB_OUTPUT is unset. GitHub Actions sets it.
unset GITHUB_OUTPUT
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.com GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.com
ROOT="$(mktemp -d)"
trap 'rm -rf "$ROOT"' EXIT
FAILED=0

new_repo() {
  cd "$(mktemp -d "$ROOT/repo.XXXXXX")"
  git init -q -b staging
  mkdir -p .changelog/unreleased
  touch .changelog/unreleased/.gitkeep CHANGELOG.md .changelog/v0.17.0.md
  git add -A && git commit -qm init
}

# A feature PR on branch feature-$1 that adds code and a fragment.
feature_pr() {
  git checkout -q -b "feature-$1" staging
  echo "$1" > "code-$1.txt"
  echo "body: $1" > ".changelog/unreleased/$1.yaml"
  git add -A && git commit -qm "feature $1"
  git checkout -q staging
}

# A release PR on branch release-$1 on top of staging, as make release-changelog makes it.
release_pr() {
  git checkout -q -b "release-$1" staging
  mkdir ".changelog/$1"
  git mv .changelog/unreleased/*.yaml ".changelog/$1/"
  echo "## [$1]" > ".changelog/$1.md" && echo "## [$1]" >> CHANGELOG.md
  git add -A && git commit -qm "release $1"
  git checkout -q staging
}

merge() { git merge -q --no-ff --no-edit "$1"; }

# Runs the script on the push from $1 to the staging tip; prints the exit code and outputs.
run() {
  local rc=0 out
  out="$(BEFORE="$1" AFTER="$(git rev-parse staging)" "$SCRIPT" 2>/dev/null)" || rc=$?
  echo "$rc $(printf '%s\n' "$out" | grep -E '^(release|version)=' | cut -d= -f2 | tr '\n' ' ')"
}

check() {
  if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: want '$2', got '$3'"; FAILED=1; fi
}

new_repo; feature_pr a; B="$(git rev-parse staging)"; merge feature-a
check "a push without a release PR releases nothing" "0 false " "$(run "$B")"

new_repo; feature_pr a; merge feature-a; release_pr v0.17.1; S="$(git rev-parse staging)"
feature_pr b; merge feature-b; B="$(git rev-parse staging)"; merge release-v0.17.1
check "a release ships its version" "0 true v0.17.1 " "$(run "$B")"
check "a release ships its base, not the staging tip" "$S" \
  "$(BEFORE="$B" AFTER="$(git rev-parse staging)" "$SCRIPT" | sed -n 's/^base=//p')"

new_repo; feature_pr a; merge feature-a; release_pr v0.17.1
git checkout -q release-v0.17.1 && echo x > code-a.txt && git commit -qam "code change" && git checkout -q staging
B="$(git rev-parse staging)"; merge release-v0.17.1
check "a release PR that changes code fails" "1 " "$(run "$B")"

new_repo; feature_pr a; merge feature-a; release_pr v0.17.1
B="$(git rev-parse staging)"; git merge -q --squash release-v0.17.1 >/dev/null; git commit -qm squash
check "a squash-merged release PR fails" "1 " "$(run "$B")"

new_repo; feature_pr a; merge feature-a; release_pr v0.17.1
git checkout -q release-v0.17.1 && touch .changelog/v0.17.2.md && git add -A && git commit -qm two && git checkout -q staging
B="$(git rev-parse staging)"; merge release-v0.17.1
check "a release PR with more than one commit fails" "1 " "$(run "$B")"

exit "$FAILED"
