# ENG-3799 PR Split Strategy

> **For Claude:** This is a strategic plan document, not an implementation plan. Execute steps manually as documented.

**Goal:** Split ENG-3799 work into two PRs: merge Fix 1+2 immediately, continue Fix 3 development with critical bug fixes in isolated worktree.

**Strategy:** Git worktree isolation + branch reset

**Created:** 2025-10-30 14:30

**Last Updated:** 2025-10-30 14:30

---

## Current State Analysis

### Remote (origin/ENG-3799-cpu-optimizations-opcua)

**PR #222:** https://github.com/united-manufacturing-hub/benthos-umh/pull/222

Contains only Fix 1 and Fix 2 (VERIFIED):
- `5ee99e8` - perf(opcua): Replace O(n²) deduplication with O(n) hash map [ENG-3799]
- `51d80be` - feat(opcua): Pre-compile sanitize regex for 75% allocation reduction [ENG-3799]

**Status:** Ready to merge immediately

### Local Only (not pushed)

3 commits ahead of remote (Fix 3 - Tasks 1-3):
- `ed6a674` - feat(opcua): Apply deadband filter to monitored items [ENG-3799]
- `592dddc` - feat(opcua): Add DataChangeFilter helper function [ENG-3799]
- `c5517e3` - feat(opcua): Add deadband filtering config fields [ENG-3799]

**Status:** Contains 3 critical bugs, needs fixes 3.1, 3.2, 3.3 before merging

### Uncommitted (testing artifacts)

Untracked files:
- `config/test-deadband-absolute.yaml`
- `config/test-deadband-baseline.yaml`
- `config/test-deadband-percent.yaml`
- `docs/plans/` (this directory)
- `docs/testing/opcua-deadband-test-results.md`
- `docs/testing/opcua-deadband-testing-plan.md`

**Status:** Need to move to Fix 3 worktree

---

## Strategy: Git Worktree Split with Clean Lineage

### Objective

1. Keep PR #222 (Fix 1+2) clean and ready to merge ASAP
2. Move Fix 3 work to isolated worktree for continued development
3. Ensure Fix 3 builds on master (independent of Fix 1+2)
4. Implement critical bug fixes in isolated environment

### Why Rebase on Master?

Fix 3 has critical bugs that need fixing before merge. Fix 1+2 are production-ready. By rebasing Fix 3 onto master (not on Fix 1+2), we:
- Keep Fix 3 as independent PR (can merge separately)
- Prevent blocking Fix 1+2 merge on Fix 3 completion
- Enable parallel workflows (merge #222 while developing Fix 3)

---

## Implementation Steps

### Phase 1: Verify and Prepare (10 minutes)

#### Step 1.1: Verify PR #222 State (5 min)

**Goal:** Confirm PR #222 contains only Fix 1+2, no Fix 3 commits

**Commands:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799

# Check remote branch
git fetch origin
git log origin/ENG-3799-cpu-optimizations-opcua --oneline

# Expected output:
# 5ee99e8 perf(opcua): Replace O(n²) deduplication with O(n) hash map [ENG-3799]
# 51d80be feat(opcua): Pre-compile sanitize regex for 75% allocation reduction [ENG-3799]
# ac78148 fix(opcua): Fix certificate Key Usage bits per OPC UA Part 6 [ENG-3772] (#221)
```

**Verification:**
- Only 2 commits from ENG-3799 on remote
- Both are Fix 1 (51d80be) and Fix 2 (5ee99e8)
- No Fix 3 commits present

**Action:** If verified, PR #222 is ready for immediate merge

#### Step 1.2: Create Fix 3 Worktree (5 min)

**Goal:** Create isolated workspace with all current work (Fix 3 commits + testing docs)

**Commands:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799

# Create new worktree from current HEAD (includes all local commits)
git worktree add -b ENG-3799-fix3-deadband /Users/jeremytheocharis/umh-git/benthos-umh-fix3

# Verify worktree created
git worktree list
```

**Expected Output:**
```
/Users/jeremytheocharis/umh-git/benthos-umh-eng-3799   ed6a674 [ENG-3799-cpu-optimizations-opcua]
/Users/jeremytheocharis/umh-git/benthos-umh-fix3       ed6a674 [ENG-3799-fix3-deadband]
```

**Verification:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git log --oneline -10

# Should show all commits including:
# ed6a674 feat(opcua): Apply deadband filter to monitored items [ENG-3799]
# 592dddc feat(opcua): Add DataChangeFilter helper function [ENG-3799]
# c5517e3 feat(opcua): Add deadband filtering config fields [ENG-3799]
# 5ee99e8 perf(opcua): Replace O(n²) deduplication with O(n) hash map [ENG-3799]
# 51d80be feat(opcua): Pre-compile sanitize regex for 75% allocation reduction [ENG-3799]
```

---

### Phase 2: Reset Original Worktree (10 minutes)

#### Step 2.1: Move Uncommitted Files to Fix 3 Worktree (5 min)

**Goal:** Preserve testing artifacts in Fix 3 worktree

**Commands:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799

# Copy untracked files to Fix 3 worktree
cp -r config/test-deadband-*.yaml /Users/jeremytheocharis/umh-git/benthos-umh-fix3/config/
cp -r docs/ /Users/jeremytheocharis/umh-git/benthos-umh-fix3/

# Verify files copied
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
ls -la config/test-deadband-*.yaml
ls -la docs/testing/
ls -la docs/plans/
```

**Add and commit in Fix 3 worktree:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3

git add config/test-deadband-*.yaml
git add docs/testing/
git add docs/plans/

git commit -m "docs: Add deadband testing artifacts and plans [ENG-3799]

- Test configurations for baseline, absolute, and percent deadband
- Testing plan and results documentation
- PR split strategy plan

These artifacts document the testing process that revealed the 3 critical
bugs (type checking, capability detection, metrics) that need fixing.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

#### Step 2.2: Reset Original to Remote State (5 min)

**Goal:** Clean original worktree to match PR #222 exactly (Fix 1+2 only)

**Commands:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799

# Clean untracked files (now safely in Fix 3 worktree)
git clean -fd

# Reset to remote state (Fix 1+2 only)
git reset --hard origin/ENG-3799-cpu-optimizations-opcua

# Verify clean state
git status
# Expected: "Your branch is up to date with 'origin/ENG-3799-cpu-optimizations-opcua'"

git log --oneline -5
# Expected: Only shows 5ee99e8 and 51d80be from ENG-3799
```

**Result:** Original worktree now contains ONLY Fix 1+2, matches PR #222 exactly

---

### Phase 3: Rebase Fix 3 onto Master (15 minutes)

#### Step 3.1: Rebase Strategy Decision (2 min)

**Goal:** Determine best approach for rebasing Fix 3 onto master

**Two approaches:**

**A) Interactive Rebase (recommended if no conflicts)**
- Cleaner history
- Preserves commit messages
- Faster if no conflicts

**B) Cherry-pick (safer if conflicts expected)**
- More control over each commit
- Easier conflict resolution
- Can skip unwanted commits

**Decision:** Start with interactive rebase, fall back to cherry-pick if conflicts

#### Step 3.2: Fetch Latest Master (1 min)

**Commands:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3

# Fetch latest from remote
git fetch origin master
git fetch origin staging  # UMH uses staging as base
```

#### Step 3.3: Interactive Rebase (Method A) (10 min)

**Commands:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3

# Rebase Fix 3 commits onto staging (UMH base branch)
git rebase -i origin/staging

# Interactive rebase editor will open
# Keep only Fix 3 commits (c5517e3, 592dddc, ed6a674)
# Drop Fix 1+2 commits (51d80be, 5ee99e8) - they're already on staging via PR #222
```

**Interactive rebase instructions:**
```
# Will show something like:
pick 51d80be feat(opcua): Pre-compile sanitize regex for 75% allocation reduction [ENG-3799]
pick 5ee99e8 perf(opcua): Replace O(n²) deduplication with O(n) hash map [ENG-3799]
pick c5517e3 feat(opcua): Add deadband filtering config fields [ENG-3799]
pick 592dddc feat(opcua): Add DataChangeFilter helper function [ENG-3799]
pick ed6a674 feat(opcua): Apply deadband filter to monitored items [ENG-3799]

# Change to:
drop 51d80be feat(opcua): Pre-compile sanitize regex for 75% allocation reduction [ENG-3799]
drop 5ee99e8 perf(opcua): Replace O(n²) deduplication with O(n) hash map [ENG-3799]
pick c5517e3 feat(opcua): Add deadband filtering config fields [ENG-3799]
pick 592dddc feat(opcua): Add DataChangeFilter helper function [ENG-3799]
pick ed6a674 feat(opcua): Apply deadband filter to monitored items [ENG-3799]
```

**Conflict resolution (if needed):**
- Resolve conflicts in each commit
- `git add <resolved-files>`
- `git rebase --continue`

#### Step 3.4: Cherry-pick Fallback (Method B) (12 min)

**If rebase has conflicts or is too complex:**

```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3

# Abort rebase if started
git rebase --abort

# Create fresh branch from staging
git checkout -b ENG-3799-fix3-deadband-rebased origin/staging

# Cherry-pick only Fix 3 commits
git cherry-pick c5517e3  # Task 1: Config fields
git cherry-pick 592dddc  # Task 2: Helper function
git cherry-pick ed6a674  # Task 3: Integration

# If cherry-pick succeeded, replace old branch
git branch -D ENG-3799-fix3-deadband
git branch -m ENG-3799-fix3-deadband
```

#### Step 3.5: Verify Rebase Success (2 min)

**Commands:**
```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3

# Check history
git log --oneline -10

# Expected: Fix 3 commits on top of staging, NO Fix 1+2 commits
# c5517e3 feat(opcua): Add deadband filtering config fields [ENG-3799]
# 592dddc feat(opcua): Add DataChangeFilter helper function [ENG-3799]
# ed6a674 feat(opcua): Apply deadband filter to monitored items [ENG-3799]
# [docs commit]
# [staging base commit]

# Verify builds
go build ./...

# Verify tests pass
go test ./internal/impl/opcua/...
```

**Result:** Fix 3 branch now builds on staging, independent of Fix 1+2

---

### Phase 4: Parallel Workflow Execution

#### Workflow A: PR #222 (Fix 1+2) - Merge ASAP

**Timeline:** Immediate (can happen during Fix 3 development)

**Location:** GitHub web UI or `gh` CLI

**Steps:**

1. **Review PR #222**
   ```bash
   # Open in browser
   gh pr view 222 --web
   ```

2. **Trigger CodeRabbit Review**
   - Comment on PR: `/review`
   - Wait for automated review

3. **Address Feedback** (if any)
   - Make changes in `/Users/jeremytheocharis/umh-git/benthos-umh-eng-3799`
   - Push to `origin/ENG-3799-cpu-optimizations-opcua`

4. **Merge to Master**
   - Click "Merge pull request" on GitHub
   - Select "Squash and merge" or "Create merge commit" per repo policy
   - Delete branch after merge

5. **Cleanup**
   ```bash
   cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799
   git checkout staging
   git pull
   git branch -d ENG-3799-cpu-optimizations-opcua
   
   # Optionally remove worktree
   cd /Users/jeremytheocharis/umh-git
   git worktree remove benthos-umh-eng-3799
   ```

**Outcome:** Fix 1+2 in production, 75% allocation reduction + 173x speedup delivered

#### Workflow B: Fix 3 Worktree - Continue Development

**Timeline:** 125 minutes + testing

**Location:** `/Users/jeremytheocharis/umh-git/benthos-umh-fix3`

**Steps:**

1. **Implement Fix 3.1: Type Checking (30 min)**
   - Add type validation for numeric nodes
   - Prevent filter application on non-numeric types
   - Follow TDD workflow (test → implement → verify)

2. **Code Review Fix 3.1 (10 min)**
   - Dispatch code-reviewer subagent
   - Address feedback
   - Commit

3. **Implement Fix 3.2: Capability Detection (40 min)**
   - Query server capabilities
   - Graceful fallback: percent → absolute → none
   - Prevent complete failure on unsupported filters

4. **Code Review Fix 3.2 (10 min)**
   - Dispatch code-reviewer subagent
   - Address feedback
   - Commit

5. **Implement Fix 3.3: Failure Metrics (25 min)**
   - Add `opcua_subscription_filter_failures_total` metric
   - Track filter rejection visibility
   - Update documentation

6. **Code Review Fix 3.3 (10 min)**
   - Dispatch code-reviewer subagent
   - Address feedback
   - Commit

7. **Retest with opc-plc (30 min)**
   - Run all test configurations
   - Validate bug fixes work
   - Document results

8. **Create PR for Fix 3 (10 min)**
   ```bash
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
   ```

9. **Review and Merge (30 min)**
   - Trigger CodeRabbit review
   - Address feedback
   - Merge to staging

**Outcome:** Fix 3 production-ready with proper server compatibility, deadband filtering complete

---

## Timeline Summary

| Phase | Duration | Worktree | Can Parallelize |
|-------|----------|----------|-----------------|
| **Phase 1: Verify and Prepare** | 10 min | Both | No |
| 1.1 Verify PR #222 state | 5 min | Original | No |
| 1.2 Create Fix 3 worktree | 5 min | N/A | No |
| **Phase 2: Reset Original** | 10 min | Both | No |
| 2.1 Move uncommitted files | 5 min | Both | No |
| 2.2 Reset original to remote | 5 min | Original | No |
| **Phase 3: Rebase Fix 3** | 15 min | Fix 3 | No |
| 3.1 Rebase strategy | 2 min | Fix 3 | No |
| 3.2 Fetch latest | 1 min | Fix 3 | No |
| 3.3 Interactive rebase | 10 min | Fix 3 | No |
| 3.5 Verify rebase | 2 min | Fix 3 | No |
| **SETUP COMPLETE** | **35 min** | | |
| | | | |
| **Phase 4A: PR #222 Merge** | 30 min | Original | **YES** |
| Review and merge PR #222 | 30 min | GitHub | **Parallel with 4B** |
| | | | |
| **Phase 4B: Fix 3 Development** | 125 min | Fix 3 | **YES** |
| Fix 3.1 + review | 40 min | Fix 3 | **Parallel with 4A** |
| Fix 3.2 + review | 50 min | Fix 3 | **Parallel with 4A** |
| Fix 3.3 + review | 35 min | Fix 3 | After 4A |
| | | | |
| **Phase 4C: Fix 3 Finalize** | 40 min | Fix 3 | No |
| Retest with opc-plc | 30 min | Fix 3 | No |
| Create PR | 10 min | Fix 3 | No |
| | | | |
| **TOTAL (Sequential)** | **240 min** | | |
| **TOTAL (Parallel)** | **200 min** | | |

**Critical Path:** Setup (35 min) → Fix 3 Development (125 min) → Fix 3 Finalize (40 min) = **200 minutes**

**PR #222 merge can happen anytime during Fix 3 development (parallel)**

---

## Acceptance Criteria

### PR #222 (Fix 1+2)

- ✅ Contains exactly 2 commits: 51d80be (Fix 1) and 5ee99e8 (Fix 2)
- ✅ No Fix 3 commits included
- ✅ Ready to merge immediately
- ✅ CodeRabbit review clean
- ✅ All tests pass
- ✅ Can merge while Fix 3 is in development

### Fix 3 Worktree

- ✅ Contains all Fix 3 commits (c5517e3, 592dddc, ed6a674)
- ✅ Contains testing artifacts and documentation
- ✅ Rebased on staging (independent of Fix 1+2)
- ✅ Build succeeds
- ✅ Tests pass (baseline before bug fixes)
- ✅ Ready for bug fix implementation

### Documentation

- ✅ Testing results in Fix 3 worktree (`docs/testing/`)
- ✅ PR split strategy in Fix 3 worktree (`docs/plans/2025-10-30-pr-split-strategy.md`)
- ✅ Original worktree clean (no uncommitted files)
- ✅ Git history clean (Fix 3 independent of Fix 1+2)

### Bug Fixes (Post-Phase 4B)

- ✅ Fix 3.1: Type checking implemented and tested
- ✅ Fix 3.2: Capability detection implemented and tested
- ✅ Fix 3.3: Metrics implemented and tested
- ✅ All fixes validated with opc-plc
- ✅ Testing documentation updated

---

## Risk Mitigation

### Risk 1: Rebase Conflicts

**Probability:** Medium (Fix 3 touches same files as Fix 1+2 might have touched)

**Impact:** High (blocks independent PR strategy)

**Mitigation:**
- **Approach A:** Interactive rebase with careful conflict resolution
- **Approach B (Fallback):** Cherry-pick approach (cleaner, easier conflicts)
- **Testing:** Verify builds and tests pass after rebase

**Contingency:** If conflicts too complex, keep Fix 3 as continuation of Fix 1+2 (not ideal, but workable)

### Risk 2: Fix 1+2 Merged Before Rebase Complete

**Probability:** Low (user controls merge timing)

**Impact:** Low (actually beneficial - validates rebase strategy)

**Mitigation:**
- Complete Phase 1-3 (setup + rebase) before merging PR #222
- If PR #222 merged during rebase, fetch latest staging and rebase again

**Contingency:** Rebase Fix 3 onto staging after PR #222 merge completes

### Risk 3: Uncommitted Files Lost

**Probability:** Very Low (using copy, not move)

**Impact:** High (lose testing artifacts and documentation)

**Mitigation:**
- Use `cp -r` (copy, not move) to duplicate files to Fix 3 worktree
- Verify files exist in Fix 3 worktree before cleaning original
- Commit files in Fix 3 worktree before running `git clean -fd` in original

**Contingency:** Files still in original worktree until after verification

### Risk 4: Fix 3 Tests Fail After Rebase

**Probability:** Medium (rebase may introduce issues)

**Impact:** Medium (delays Fix 3 development, but expected)

**Mitigation:**
- Run `go build ./...` after rebase (catches build errors)
- Run `go test ./internal/impl/opcua/...` after rebase (baseline test)
- Document any new failures as potential conflicts to resolve

**Contingency:** Fix any rebase-introduced issues before proceeding to bug fixes

### Risk 5: Wrong Worktree Access

**Probability:** Medium (two worktrees with similar names)

**Impact:** High (changes in wrong worktree, confusion)

**Mitigation:**
- Always verify current directory before git commands
- Use full absolute paths in all commands
- Name worktrees distinctly: `benthos-umh-eng-3799` vs `benthos-umh-fix3`

**Contingency:** Use `git worktree list` to identify correct worktree if confused

---

## Success Criteria

### Immediate (Phase 1-3: Setup - 35 min)

- ✅ PR #222 verified as Fix 1+2 only
- ✅ Fix 3 worktree created with all commits
- ✅ Testing artifacts moved to Fix 3 worktree
- ✅ Original worktree reset to remote state (Fix 1+2 only)
- ✅ Fix 3 rebased on staging (independent lineage)

### Short-term (Phase 4A: PR #222 merge - 30 min)

- ✅ PR #222 reviewed and approved
- ✅ PR #222 merged to staging/master
- ✅ Fix 1+2 in production
- ✅ Original worktree cleaned up (branch deleted)

### Medium-term (Phase 4B: Fix 3 development - 125 min)

- ✅ Fix 3.1 (type checking) implemented and reviewed
- ✅ Fix 3.2 (capability detection) implemented and reviewed
- ✅ Fix 3.3 (metrics) implemented and reviewed
- ✅ All bug fixes committed to Fix 3 branch

### Long-term (Phase 4C: Fix 3 finalize - 40 min)

- ✅ Testing with opc-plc validates all fixes work
- ✅ PR created for Fix 3 (deadband filtering + bug fixes)
- ✅ PR reviewed and approved
- ✅ Fix 3 merged to staging/master
- ✅ Complete ENG-3799 implementation in production

---

## Verification Commands

### Verify PR #222 Clean

```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799
git fetch origin
git log --oneline origin/ENG-3799-cpu-optimizations-opcua | grep ENG-3799 | wc -l
# Expected: 2 (only Fix 1 and Fix 2)
```

### Verify Fix 3 Worktree Has All Commits

```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git log --oneline | grep "feat(opcua).*deadband" | wc -l
# Expected: 3 (all Fix 3 tasks)
```

### Verify Original Reset to Remote

```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-eng-3799
git status
# Expected: "Your branch is up to date with 'origin/ENG-3799-cpu-optimizations-opcua'"
git log --oneline -5 | grep ENG-3799 | wc -l
# Expected: 2 (only Fix 1 and Fix 2)
```

### Verify Fix 3 Rebased on Staging

```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
git log --oneline origin/staging..HEAD | wc -l
# Expected: 4 (3 Fix 3 commits + 1 docs commit)
git log --oneline origin/staging..HEAD | grep -E "(51d80be|5ee99e8)"
# Expected: No output (Fix 1+2 not in history)
```

### Verify Files Moved Correctly

```bash
cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3
ls config/test-deadband-*.yaml | wc -l
# Expected: 3
ls docs/testing/*.md | wc -l
# Expected: 2
ls docs/plans/*.md | wc -l
# Expected: 1 (this plan)
```

---

## Changelog

### 2025-10-30 14:30 - Plan created

Initial PR split strategy created to separate Fix 1+2 (ready to merge) from Fix 3 (needs bug fixes). Strategy uses git worktree isolation + branch reset to enable parallel workflows: immediate merge of PR #222 while continuing Fix 3 development in isolated worktree.

Key decisions:
- Rebase Fix 3 onto staging (independent of Fix 1+2) to enable parallel merge
- Move all testing artifacts to Fix 3 worktree for continuity
- Reset original worktree to match PR #222 exactly (prevents accidental contamination)
- Use git worktree for isolation (not branches) to enable simultaneous work

