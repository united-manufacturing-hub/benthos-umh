#!/bin/bash
# ENG-3799 PR Split Strategy - Quick Command Reference
# Based on: docs/plans/2025-10-30-pr-split-strategy.md
#
# DO NOT RUN THIS SCRIPT DIRECTLY - commands are meant to be executed manually
# This is a reference for copy-pasting commands during execution

set -e

echo "==================================================================="
echo "ENG-3799 PR SPLIT STRATEGY - COMMAND REFERENCE"
echo "==================================================================="
echo ""
echo "⚠️  WARNING: DO NOT RUN THIS SCRIPT DIRECTLY"
echo "Execute commands manually, phase by phase, verifying each step."
echo ""

# ==================================================================
# PHASE 1: VERIFY AND PREPARE (10 min)
# ==================================================================

echo "==================================================================="
echo "PHASE 1: VERIFY AND PREPARE"
echo "==================================================================="

echo ""
echo "--- Step 1.1: Verify PR #222 State ---"
echo ""
cat << 'STEP_1_1'
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799
git fetch origin
git log origin/ENG-3799-cpu-optimizations-opcua --oneline -5

# Expected: Only 2 commits from ENG-3799 (5ee99e8, 51d80be)
STEP_1_1

echo ""
echo "--- Step 1.2: Create Fix 3 Worktree ---"
echo ""
cat << 'STEP_1_2'
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799
git worktree add -b ENG-3799-fix3-deadband /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git worktree list

# Verify in new worktree
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git log --oneline -10
STEP_1_2

# ==================================================================
# PHASE 2: RESET ORIGINAL WORKTREE (10 min)
# ==================================================================

echo ""
echo "==================================================================="
echo "PHASE 2: RESET ORIGINAL WORKTREE"
echo "==================================================================="

echo ""
echo "--- Step 2.1: Move Uncommitted Files ---"
echo ""
cat << 'STEP_2_1'
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799

# Copy files to Fix 3 worktree
cp -r config/test-deadband-*.yaml /Users/jeremytheocharis/umh-git/benthos-umh-fix3/config/
cp -r docs/ /Users/jeremytheocharis/umh-git/benthos-umh-fix3/

# Verify files copied
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
ls -la config/test-deadband-*.yaml
ls -la docs/testing/
ls -la docs/plans/

# Commit in Fix 3 worktree
git add config/test-deadband-*.yaml docs/testing/ docs/plans/
git commit -m "docs: Add deadband testing artifacts and plans [ENG-3799]

- Test configurations for baseline, absolute, and percent deadband
- Testing plan and results documentation
- PR split strategy plan

These artifacts document the testing process that revealed the 3 critical
bugs (type checking, capability detection, metrics) that need fixing.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
STEP_2_1

echo ""
echo "--- Step 2.2: Reset Original to Remote ---"
echo ""
cat << 'STEP_2_2'
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799

# Clean untracked files (safely in Fix 3 now)
git clean -fd

# Reset to remote state
git reset --hard origin/ENG-3799-cpu-optimizations-opcua

# Verify
git status
git log --oneline -5
STEP_2_2

# ==================================================================
# PHASE 3: REBASE FIX 3 ONTO STAGING (15 min)
# ==================================================================

echo ""
echo "==================================================================="
echo "PHASE 3: REBASE FIX 3 ONTO STAGING"
echo "==================================================================="

echo ""
echo "--- Step 3.2: Fetch Latest ---"
echo ""
cat << 'STEP_3_2'
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git fetch origin staging
STEP_3_2

echo ""
echo "--- Step 3.3: Interactive Rebase (Method A - PREFERRED) ---"
echo ""
cat << 'STEP_3_3'
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git rebase -i origin/staging

# In editor, change Fix 1+2 commits from 'pick' to 'drop':
# drop 51d80be feat(opcua): Pre-compile sanitize regex...
# drop 5ee99e8 perf(opcua): Replace O(n²) deduplication...
# pick c5517e3 feat(opcua): Add deadband filtering config fields...
# pick 592dddc feat(opcua): Add DataChangeFilter helper function...
# pick ed6a674 feat(opcua): Apply deadband filter to monitored items...

# If conflicts:
git add <resolved-files>
git rebase --continue
STEP_3_3

echo ""
echo "--- Step 3.4: Cherry-pick Fallback (Method B - IF REBASE FAILS) ---"
echo ""
cat << 'STEP_3_4'
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3

# Abort rebase if started
git rebase --abort

# Fresh branch from staging
git checkout -b ENG-3799-fix3-deadband-rebased origin/staging

# Cherry-pick only Fix 3 commits
git cherry-pick c5517e3  # Task 1: Config fields
git cherry-pick 592dddc  # Task 2: Helper function
git cherry-pick ed6a674  # Task 3: Integration

# Replace old branch
git branch -D ENG-3799-fix3-deadband
git branch -m ENG-3799-fix3-deadband
STEP_3_4

echo ""
echo "--- Step 3.5: Verify Rebase ---"
echo ""
cat << 'STEP_3_5'
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3

# Check history (should NOT show Fix 1+2)
git log --oneline -10

# Verify build
go build ./...

# Verify tests
go test ./internal/impl/opcua/...
STEP_3_5

# ==================================================================
# PHASE 4A: MERGE PR #222 (30 min) - PARALLEL
# ==================================================================

echo ""
echo "==================================================================="
echo "PHASE 4A: MERGE PR #222 (can run parallel with Phase 4B)"
echo "==================================================================="

echo ""
cat << 'PHASE_4A'
# Open PR in browser
gh pr view 222 --web

# Trigger CodeRabbit review (comment on PR):
/review

# After approval, merge via GitHub UI

# Cleanup original worktree (optional)
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799
git checkout staging
git pull
git branch -d ENG-3799-cpu-optimizations-opcua
cd /Users/jeremytheocharis/umh-git
git worktree remove benthos-umh-eng-3799
PHASE_4A

# ==================================================================
# PHASE 4B: FIX 3 DEVELOPMENT (125 min) - PARALLEL
# ==================================================================

echo ""
echo "==================================================================="
echo "PHASE 4B: FIX 3 DEVELOPMENT (can run parallel with Phase 4A)"
echo "==================================================================="
echo ""
echo "Implement Fixes 3.1, 3.2, 3.3 using TDD + code review workflow"
echo "See docs/plans/2025-10-30-pr-split-strategy.md for details"
echo ""

# ==================================================================
# PHASE 4C: CREATE PR FOR FIX 3 (10 min)
# ==================================================================

echo ""
echo "==================================================================="
echo "PHASE 4C: CREATE PR FOR FIX 3"
echo "==================================================================="

echo ""
cat << 'PHASE_4C'
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3

# Push to remote
git push -u origin ENG-3799-fix3-deadband

# Create PR
gh pr create \
  --title "feat(opcua): Add deadband filtering with server compatibility [ENG-3799]" \
  --body "$(cat <<'PRBODY'
## Summary

Implements OPC UA deadband filtering (absolute/percent) with server-side compatibility:
- Server-side filtering reduces notification traffic by 30-70%
- Type checking prevents filter rejection on non-numeric nodes
- Server capability detection with graceful fallback (percent → absolute → none)
- Prometheus metrics for filter rejection visibility

## Changes

### Deadband Implementation (Tasks 1-3)
- Add `deadbandType` and `deadbandValue` config fields
- Implement `DataChangeFilter` helper function
- Integrate filter into `MonitorBatched` subscription

### Bug Fixes (Fixes 3.1-3.3)
- **Fix 3.1:** Type checking for numeric nodes only (prevents StatusBadFilterNotAllowed)
- **Fix 3.2:** Server capability detection with fallback (prevents complete failures)
- **Fix 3.3:** Add `opcua_subscription_filter_failures_total` metric (visibility)

## Testing

Validated with opc-plc (1000 nodes):
- ✅ Type checking prevents filter rejection on non-numeric nodes
- ✅ Capability detection prevents complete failure on unsupported filters
- ✅ Metrics expose rejection counts for monitoring
- ✅ Deadband filtering reduces traffic by 30-70% (absolute: 70%, percent: 30%)

## Part of ENG-3799

This PR is part 2 of ENG-3799 CPU optimizations:
- **PR #222:** Fix 1+2 (regex pre-compilation, O(n) deduplication) - merged
- **PR #???:** Fix 3 (deadband filtering with compatibility) - this PR

## References

- Linear: [ENG-3799](https://linear.app/united-manufacturing-hub/issue/ENG-3799)
- Testing Results: `docs/testing/opcua-deadband-test-results.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
PRBODY
)"
PHASE_4C

# ==================================================================
# VERIFICATION COMMANDS
# ==================================================================

echo ""
echo "==================================================================="
echo "VERIFICATION COMMANDS"
echo "==================================================================="

echo ""
echo "--- Verify PR #222 Clean ---"
echo ""
cat << 'VERIFY_1'
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799
git log --oneline origin/ENG-3799-cpu-optimizations-opcua | grep ENG-3799 | wc -l
# Expected: 2
VERIFY_1

echo ""
echo "--- Verify Fix 3 Has All Commits ---"
echo ""
cat << 'VERIFY_2'
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git log --oneline | grep "feat(opcua).*deadband" | wc -l
# Expected: 3
VERIFY_2

echo ""
echo "--- Verify Original Reset ---"
echo ""
cat << 'VERIFY_3'
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799
git status
git log --oneline -5 | grep ENG-3799 | wc -l
# Expected: 2
VERIFY_3

echo ""
echo "--- Verify Fix 3 Rebased ---"
echo ""
cat << 'VERIFY_4'
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git log --oneline origin/staging..HEAD | wc -l
# Expected: 4 (3 Fix 3 commits + 1 docs commit)
git log --oneline origin/staging..HEAD | grep -E "(51d80be|5ee99e8)"
# Expected: No output
VERIFY_4

echo ""
echo "==================================================================="
echo "END OF COMMAND REFERENCE"
echo "==================================================================="
echo ""
echo "For full details, see: docs/plans/2025-10-30-pr-split-strategy.md"
