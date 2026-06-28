# VSDD Status: ENG-5240 nodered_js array fan-out

Intensity: Full

- [x] Phase 1: Spec — CONVERGED (da-sweep round 10). Linear doc on ENG-5240.
- [x] Phase 2: TDD — LADDER COMPLETE (capstone green, real env). See below.
- [ ] Phase 3: Roast (full-diff adversarial)
- [ ] Phase 4: Feedback
- [ ] Phase 5: Hardening (10k load test + mutation)
- [ ] Phase 6: Convergence

## Phase 2 exit checkpoint

Ladder (8 commits on eng-5240-allow-nodered_js-to-return-multiple-messages, base 258fc7a1):
- R1 4f1c6af1 — nodered_js array fan-out core (signature → []*service.Message, case []interface{})
- R2 05732dba — nil-skip (amended: removed spec-divergent per-nil messagesDropped bump)
- R3 a0682528 — non-map element error ("array elements must be message objects")
- R4 da01d04b — empty/all-nil array → messagesDropped once (whole-input drop)
- R5 3c69113a — tag_processor messagesDropped wired (len==0 guard, was dead counter)
- R6 VACUOUS — covered by R5 (condition-then drop routes through same processMessageBatchWithProgram); no commit
- R7 e4016842 — tag_processor messagesErrored on condition + defaults JS errors
- R7b fd4cb83b — tag_processor messagesErrored on advanced JS error (amended: batchSize-before-call fix for nil-batch bug)
- R8 c725fb08 — CAPSTONE: ERP array fan-out end-to-end (test-only, composed R1-R4)

Tests: nodered_js 98 specs, tag_processor 44/45 (1 pre-existing skip). All green under TEST_NODERED_JS=true / TEST_TAG_PROCESSOR=true.

Conductor catches (self-report is INPUT, never a substitute):
1. R2 per-nil messagesDropped bump — spec divergence (nil-skip in partial array must not bump; tag_processor mirror doesn't). Amended.
2. R7b nil-batch bug — Incr(len(batch)) where batch is nil after processMessageBatchWithProgram returns (nil,err). Per-commit review caught it; confirmed via REAL-env run (TEST_TAG_PROCESSOR=true) which showed 3 failures the workflow's vacuous-skipped "green" hid. Amended with batchSize-before-call.
3. CRITICAL: R5/R7/R7b were VACUOUS GREEN — tag_processor (and nodered_js) suites Skip() unless TEST_*_JS=true. tdd-commit's bare `go test` skipped everything → false greenConfirmed. Re-verified all rungs under real env.

Deferred to Phase 3-end / PR-boundary:
- changie fragments (spec lists 2: `new` fan-out + `improvements` metrics)
- docs/processing/node-red-javascript-processor.md update (array return undocumented)
- refactor: batchFatalErr(stage,err) helper (structural bump, can't regress), messageFromReturnValue nil-on-error, double-logging on condition errors
- test-strengthening: Equal(1) not BeNumerically(">=",1); R6 double-count + pass-through guard tests (test-only regression pin)
