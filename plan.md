# Implementation Plan: PR #217 Review Feedback

## Context
Implementing review feedback from led0nk and daniel-umh on PR #217 (ENG-3720).

## Prerequisites
- Working directory: `/Users/jeremytheocharis/umh-git/benthos-umh-eng-3720`
- Branch: `eng-3720-sparkplugb-sequence-sanitized-tests`
- Already pulled latest code

---

## Task 1: Optimize `processDataMessage` mutex handling
**File**: `sparkplug_plugin/sparkplug_b_input.go` (lines 599-661)

**Objective**: Implement daniel-umh's optimization with single lock/unlock cycle

**Changes**:
1. Remove complex lock/unlock/re-lock pattern
2. Update all state (lastSeq, lastSeen, isOnline) before releasing lock
3. Use elegant `state.isOnline = isValidSequence` one-liner
4. Call MQTT operations (sendRebirthRequest, requestBirthIfNeeded) after unlock
5. Keep single exit path for existing nodes

**Acceptance Criteria**:
- [ ] Single lock acquisition, single unlock
- [ ] No re-acquisition after MQTT I/O
- [ ] All state updated atomically before unlock
- [ ] Tests pass (especially lock management tests)

**Status**: [ ] Not Started

---

## Task 2: Implement bdSeq validation in `processDeathMessage`
**File**: `sparkplug_plugin/sparkplug_b_input.go` (lines 663-673)

**Objective**: Use the `payload` parameter for Sparkplug B spec-compliant bdSeq validation

**Changes**:
1. Extract bdSeq from NDEATH payload metrics
2. Compare with stored bdSeq from last NBIRTH
3. Reject stale NDEATH messages (wrong bdSeq = old session)
4. Handle missing bdSeq gracefully (backwards compatibility)
5. No bdSeq validation for DDEATH (correct per spec)

**Acceptance Criteria**:
- [ ] bdSeq extracted from NDEATH payload
- [ ] Stale NDEATH (mismatched bdSeq) ignored with warning
- [ ] Missing bdSeq handled gracefully
- [ ] DDEATH processed without bdSeq validation
- [ ] Payload parameter is now used (no lint warning)

**Status**: [ ] Not Started

---

## Task 3: Add test helper methods for death message testing
**File**: `sparkplug_plugin/sparkplug_b_input_test_helper.go`

**Objective**: Enable testing of new `processDeathMessage` behavior

**Changes**:
1. Add `ProcessDeathMessage()` wrapper method
2. Add `SetNodeBdSeq()` to simulate NBIRTH bdSeq
3. Add `GetNodeBdSeq()` to verify bdSeq state

**Acceptance Criteria**:
- [ ] All three methods added
- [ ] Methods compile without errors
- [ ] Can be used in tests

**Status**: [ ] Not Started

---

## Task 4: Add bdSeq validation tests
**File**: `sparkplug_plugin/sparkplug_b_death_validation_test.go` (NEW)

**Objective**: Verify bdSeq validation behavior with comprehensive tests

**Test Cases**:
1. Accept NDEATH with matching bdSeq
2. Reject stale NDEATH with mismatched bdSeq
3. Handle NDEATH without bdSeq metric gracefully
4. Process DDEATH without bdSeq validation

**Acceptance Criteria**:
- [ ] All 4 test cases implemented
- [ ] Tests pass
- [ ] Uses Ginkgo/Gomega framework

**Status**: [ ] Not Started

---

## Task 5: Run full test suite and verify
**Objective**: Ensure all changes work together

**Commands**:
```bash
cd sparkplug_plugin && go test -v ./...
```

**Acceptance Criteria**:
- [ ] All existing tests pass
- [ ] New bdSeq validation tests pass
- [ ] Lock management tests pass
- [ ] No regressions

**Status**: [ ] Not Started

---

## Task 6: Commit changes
**Objective**: Commit with proper message referencing the review

**Commit Message**:
```
fix(sparkplug): apply review feedback from led0nk and daniel-umh

- Optimize processDataMessage mutex handling (daniel-umh's suggestion)
- Single lock/unlock cycle, no re-acquisition after MQTT I/O
- Implement bdSeq validation in processDeathMessage
- Reject stale NDEATH messages per Sparkplug B spec
- Add test helpers and tests for bdSeq validation

Addresses review comments on PR #217
```

**Status**: [ ] Not Started

---

## Completion Checklist
- [ ] Task 1: processDataMessage optimization
- [ ] Task 2: processDeathMessage bdSeq validation
- [ ] Task 3: Test helper methods
- [ ] Task 4: bdSeq validation tests
- [ ] Task 5: Full test suite passes
- [ ] Task 6: Changes committed
