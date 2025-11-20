# processDataMessage Refactoring Implementation Plan

**Goal:** Reduce cognitive complexity from 8-10 to 4-5 (50% reduction) while maintaining all 223 tests passing

**Created:** 2025-11-20
**Status:** In Progress

---

## Pre-Refactoring Preparation

### Task 1: Create Safety Branch and Baseline
- Create refactoring branch
- Run baseline test suite (verify 223 tests pass)
- Document current complexity metrics
- Create rollback point

**Status:** [ ] Not Started

---

## Phase 1: TDD - Create Tests for New Functions

### Task 2: Test stateAction struct
- Write failing tests for stateAction
- Implement minimal struct
- Verify tests pass
- Commit

**Status:** [ ] Not Started

### Task 3: Test updateNodeState pure function
- Write failing tests for all paths (new node, valid seq, gap, wraparound)
- Implement minimal updateNodeState function
- Verify tests pass
- Commit

**Status:** [ ] Not Started

### Task 4: Test logSequenceError helper
- Write failing tests for logging
- Implement minimal logSequenceError
- Verify tests pass
- Commit

**Status:** [ ] Not Started

---

## Phase 2: Incremental Refactoring

### Task 5: Extract updateNodeState into processDataMessage
- Refactor processDataMessage to use updateNodeState
- Run test suite (223 tests must pass)
- Verify complexity reduction
- Commit

**Status:** [ ] Not Started

### Task 6: Optimize log call location
- Move logging after lock release
- Run test suite
- Commit

**Status:** [ ] Not Started

---

## Phase 3: Integration Testing

### Task 7: Run comprehensive test suite
- Run full Sparkplug test suite
- Run full benthos test suite
- Document test results
- Commit

**Status:** [ ] Not Started

---

## Phase 4: Performance Verification

### Task 8: Measure complexity reduction
- Manual complexity assessment
- Document final metrics
- Commit metrics

**Status:** [ ] Not Started

---

## Phase 5: Documentation Updates

### Task 9: Update function documentation
- Add comprehensive function docs
- Follow Go doc conventions
- Run tests
- Commit

**Status:** [ ] Not Started

### Task 10: Create refactoring summary
- Create summary document
- Document results and benefits
- Commit

**Status:** [ ] Not Started

---

## Phase 6: Final Review

### Task 11: Request code review
- Verify all changes committed
- Review commit history
- Request code review

**Status:** [ ] Not Started

---

## Acceptance Criteria

- [ ] Cognitive complexity reduced from 8-10 to 4-5 (50% target)
- [ ] Function size reduced (target: ~30 lines or less)
- [ ] All 223 existing tests passing
- [ ] New tests added for extracted functions (minimum 5 test cases)
- [ ] Single lock/unlock pattern maintained
- [ ] No behavior changes (verified by existing test suite)
- [ ] Documentation updated for all modified functions

## Target Design

```go
// Main function becomes simple orchestration (~25 lines)
func (s *sparkplugInput) processDataMessage(deviceKey, msgType string, payload *sparkplugb.Payload) {
    s.stateMu.Lock()
    currentSeq := GetSequenceNumber(payload)
    action := updateNodeState(s.nodeStates, deviceKey, currentSeq)
    s.resolveAliases(deviceKey, payload.Metrics)
    s.stateMu.Unlock()

    if action.isNewNode {
        s.requestBirthIfNeeded(deviceKey)
    } else if action.needsRebirth {
        s.sendRebirthRequest(deviceKey)
    }
}

// Pure function - deterministic, no side effects
func updateNodeState(nodeStates map[string]*nodeState, deviceKey string, currentSeq uint8) stateAction {
    // State transition logic here
}

// Centralized logging
func logSequenceError(logger *zap.SugaredLogger, metric metrics.ExternalCounter, deviceKey string, lastSeq, currentSeq uint8) {
    // Logging logic here
}
```

## Risk Mitigation

**Rollback Strategy:**
- Baseline commit created before refactoring
- Incremental commits after each phase
- Test verification after every change
- Can revert to any commit if issues arise

**Red Flags - Stop if:**
- Any test fails during refactoring
- Complexity increases instead of decreases
- Lock strategy changes
- New bugs introduced
