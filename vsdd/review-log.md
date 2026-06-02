# VSDD Review Log — Open Protocol Input Plugin

## Phase 1: Spec Crystallization

### remove-ai-slop pass (Section 25 focus) — 2026-06-02

Spec is clean. Phase/verification vocabulary is native to a design spec (§25.1/25.3
scope carve-out). One real tell:

- §25.12 personification: "asset-identifying fields … **ride** as metadata" → "are carried as".

Em-dashes are dense but used as appositive punctuation in a technical spec (standard
style; skill body itself uses them). Out of the Section-25 primary scope — not stripped.
No copula-avoidance, rule-of-three padding, vacuous closers, or wrong-layer narration found.

Disposition: apply the one personification fix in the batched Phase-1 edit.

### Adversarial spec review #1 (fresh context) — 2026-06-02

Spec: `docs/superpowers/specs/2026-06-02-open-protocol-input-design.md`
Verdict: **3 blockers, 9 majors, 8 minors.** Almost all legitimate.

| # | Sev | Finding | Resolution |
|---|-----|---------|------------|
| 1 | BLOCKER | 18-msg fan-out vs single 0062 ack timing under-specified | New **Delivery & Acknowledgement** section: 18 msgs = 1 `MessageBatch` = 1 `AckFunc` = 1 MID 0062, fired only after downstream acks all 18 |
| 12 | BLOCKER | "block don't drop" contradicts draft; keep-alive-during-backpressure unstated | Same section: block (no drop); keep-alives continue independently as the only back-pressure signal. Added Divergences section |
| 2 | MAJOR | line-50 "forward then ack" vs AckFunc model | Reworded "What We're Shipping" to the AckFunc model |
| 4 | MAJOR | No edge case for `AckFunc(err≠nil)` | Edge #21: no 0062, result left un-acked, warn |
| 5 | MAJOR | Reconnect orphans in-flight AckFuncs | Edge #22 + Delivery section: ack bound to connection generation; stale = no-op |
| 6 | MAJOR | No read-idle/dead-link timeout (half-open TCP) | Added `read_timeout` config + Edge #23 |
| 9 | MAJOR | 23 PIDs → 18 tags mapping implicit | Reconciliation note under output table |
| 10 | MAJOR | revision default 1 (spec) vs 0 (draft); on-wire value | Reconciled to 1; documented MID 0001 value; in Divergences |
| 14 | MAJOR | Property #8 needs enumerated interleavings | Expanded into success / downstream-fail / reconnect-before-ack |
| 18 | MAJOR | gremlins ≥95% unachievable as stated | Scoped: exclusion list + required exact-length test for widthRest |
| 3 | MINOR | Revision guard normalized vs raw; blank-rev 0061 | Guard tests normalized revision; blank/000 ⇒ rev 1 ⇒ decoded |
| 7 | MINOR | Reassembly has no size/part bound vs Memory NFR | Added max-parts + max-bytes ceiling |
| 8 | MINOR | param 21 disposition + DST | param 21 → `open_protocol_pset_change_timestamp` metadata; DST via `ParseInLocation` |
| 11 | MINOR | `op_*`→`open_protocol_*` not flagged | Added to Divergences |
| 13 | MINOR | Edge #2 "treated as failed" vs reconnect | Reworded: unrecoverable → close + reconnect |
| 15 | MINOR | golden proves samples, not spec | Reworded #4/#5: goldens encode hand-derived R2.16 expectations |
| 16 | MINOR | purity prose too absolute | Reworded: depends only on explicit args incl. `*time.Location` |
| 17 | MINOR | tz loading unassigned | `LoadLocation` in `open_protocol.go` at config parse |
| 19 | MINOR | property #7 trivial for `rapid` | Restated as purity invariant (FanOut reads no clock) |
| 20 | MINOR | emulator validates emulator-width only | Noted: spec-width PID-23 path is golden-only |

### Adversarial spec review #2 (fresh context) — 2026-06-02

Verdict: **1 blocker, 3 majors, 2 minors (new); 11/12 prior issues fully resolved.**
The 12th (connection-generation) was "partial" because the mechanism wasn't
defined — now fixed by MAJOR-1 below. All new findings legitimate (adversary read
the Benthos `input.go` AckFunc contract directly).

| # | Sev | Finding | Resolution |
|---|-----|---------|------------|
| B1 | BLOCKER | Benthos AckFunc fires **at least once** (can repeat) → "exactly one 0062" risks two acks | Added one-shot idempotency guard (`sync.Once`/acked flag); Delivery table + Edge #27 + Property #8 double-invocation trace |
| M1 | MAJOR | "connection generation" asserted but undefined/u* implementable | Defined concrete mechanism: monotonic `generation uint64` on `Session`, captured `(gen, conn)` per ack, compared under `mu`, 0062 written to captured `conn` under `writeMu` |
| M2 | MAJOR | Subscription replies had no timeout → silent subscribe failure → reconnect loop | `Start` now awaits MID 0005/0004 per subscribe within `request_timeout`; fails on error/timeout |
| M3 | MAJOR | Back-pressure "controller gates on 0062" claim too strong | Softened: gating is controller-dependent (tied to Durability caveat); free-running controllers drop per own policy |
| m1 | MINOR | `read_timeout ≥ 2× keepalive` advisory only | Now config-load-validated (Edge #26) |
| m2 | MINOR | keep-alive **write** failure ⇒ reconnect unspecified | Edge #25 |

Convergence: all blockers/majors from both rounds resolved. Remaining items are
implementation obligations captured in the Divergences table, to be enforced by
Phase 2 tests + Phase 5 mutation testing.

### Adversarial spec review #3 (fresh context) — 2026-06-02

Verdict: **0 blockers, 3 majors, 6 minors. Converged: no.** All six round-2 fixes
confirmed RESOLVED. New majors were all in the ack-guard composition (introduced
or exposed by the round-2 edits):

| # | Sev | Finding | Resolution |
|---|-----|---------|------------|
| M1 | MAJOR | Table row "any subsequent invocation = no-op" contradicts "first successful sends" (err!=nil then err==nil would lose the ack) | Rewrote as ordered 3-step algorithm; guard keyed on *having sent*, not *subsequent*; Edge #27 reworded |
| M2 | MAJOR | once-guard vs generation-check ordering unspecified; re-push fresh-closure not stated | Step order fixed (err-check → generation-check → send-guard); stated each re-pushed result gets its own fresh closure |
| M3 | MAJOR | 0062 write-failure vs Edge #25 (reconnect?) unspecified | Stated: 0062 write failure is a benign no-op, does NOT trigger reconnect |
| m1–m6 | MINOR | lock order; single guard mechanism; stale-conn write; generic_subscribe confirm; Stop-during-ack | Lock order `mu`→`writeMu` stated; one `sent bool`; generic_subscribe fire-and-forget; Edge #28 (late ack after Stop) |

### Adversarial spec review #4 (fresh context) — 2026-06-02 — CONVERGED

Verdict: **0 blockers, 0 majors, 4 minors. CONVERGED: yes.** All three round-3
majors confirmed fixed; Delivery & Acknowledgement section verified internally
consistent and deadlock-free. The 4 minors (all applied):

| # | Sev | Finding | Resolution |
|---|-----|---------|------------|
| m1 | MINOR | Edge #28 rationale wrong (generation doesn't advance on Stop) | Reworded to write-failure path |
| m2 | MINOR | Property #8 didn't name fail→success trace | Added explicit trace |
| m3 | MINOR | MID 0005 decoder location unstated | Noted: parsed inline in `session.go` |
| m4 | MINOR | guard-mutex not in lock-order rule | Extended to `mu` → send-guard → `writeMu` |

**Phase 1 adversarial review CONVERGED** after 4 rounds (3B/9M/8m → 1B/3M/2m →
0B/3M/6m → 0B/0M/4m). remove-ai-slop pass applied (one personification fix).

## Phase 2 — Sibling-convention audit (fresh context) — 2026-06-02

Audited the plan against `sparkplug`, `sensorconnect`, `modbus`, `s7comm` source.
Conforms on: `RegisterBatchInput` + N-msg batch; `open_protocol_*` prefix +
`open_protocol_tag_name`; constructor-error validation (LintRule used once
repo-wide, never in an input); opting out of `AutoRetryNacksBatched` (sparkplug
precedent); `timestamp_ms` epoch-ms string (matches tag_processor:575 consumer).

**One HIGH divergence: self-managed reconnect goroutine.** No sibling input runs
its own reconnect loop — modbus/s7comm return `service.ErrNotConnected` and let
Benthos re-`Connect` (`modbus.go:783`, `s7comm.go:371`). **User decision
(2026-06-02): refactor to the Benthos-native model.** `Connect` =
dial+login+confirm-subscribe+start keep-alive (+ bump generation); `ReadBatch`
returns `service.ErrNotConnected` on loss; Benthos drives reconnect/backoff. The
generation-bound idempotent ack is preserved (Benthos may ack after Close/Connect).
Drops the `reconnect.max_backoff` config knob. Spec Session contract + plan Tasks
5–9 updated accordingly.

LOW divergences kept as-is (justified): `timestamp_ms` set by an input;
integration `//go:build integration` tag (siblings gate by env-var only).

## Phase 3 — Adversarial Roast (Sarcasmotron, fresh context, opus) — pass 1 — 2026-06-02

Verdict: **3 blockers, 6 majors, 8 minors** (2 withdrawn by the adversary itself).
Resolutions in Fix Batch 1 (`b3fec19`) + Fix Batch 2 (`db97a49`):

| Sev | Finding | Disposition |
|-----|---------|-------------|
| BLOCKER | `makeAck` used `sync.Once` → latched even on FAILED 0062 write; spec requires "set sent only on successful write" (Edge #27) | **Real bug, fixed:** `sent bool` set only after successful `write`; failed write logs + retries. Internal TDD test failed-before/passed-after |
| BLOCKER | `revision` config default `0`, spec mandates `1` | Fixed: `.Default(1)`, `.Examples(1)`, spec-accurate description |
| BLOCKER (test) | No test for AckFunc(err!=nil)→no 0062, nor one-batch⇒one-0062 at ReadBatch boundary | Added internal `TestReadBatchOneBatchOneAck` + `TestReadBatchNoAckOnDownstreamFailure` |
| MAJOR | Edge #21 warning never logged on downstream-delivery failure | Fixed: AckFunc warns + withholds 0062 on `e != nil` |
| MAJOR | Stale-ack test passed via closed-conn write failure, not the generation guard | Addressed: internal `TestMakeAckSuppressesStaleGenerationOnWritableConn` (writable conn + advanced gen → zero writes) |
| MAJOR | timestamp_ms test only coincidentally UTC; no non-UTC e2e | Added Europe/Berlin e2e test (timestamp_ms 2h earlier) |
| MAJOR | Edge #14 (unparseable timestamp) untested | Added test: 18 msgs, no `timestamp_ms`, raw `open_protocol_timestamp` kept |
| MAJOR | Reassembler Edge #5 / state-discarded invariant untested | Added: out-of-order error then fresh part-1 succeeds |
| MINOR | maxAssembledBytes part-1 branch; framer length lower-bound; property #1 rev≤0; garbage-ts property trivial | All added/strengthened (Fix Batch 2 TEST 4–7) |
| MINOR | dead `Decode` re-encodes revision-blind behavior | Removed `Decode` + its test |
| MINOR (dismissed) | `dialTimeout` hardcoded 10s | Defensible; spec lists no dial_timeout field. Doc-only |
| MINOR (dismissed) | generic_subscribe write-error aborts Connect | A write error = broken socket; failing Connect is correct. Spec "fire-and-forget" = no reply awaited, which holds |

Fix Batches both race/gofmt/vet clean; full module builds. Re-running adversary (pass 2) for convergence.

### Pass 2 (opus) — 0 blockers, 1 major, 2 minors; all prior fixes CONFIRMED
- MAJOR: Edge #9 warning not logged for non-rev-1 0061 raw passthrough → fixed (`2d1fc69`).
- MINOR: no concurrent-goroutine ack test → added (`2d1fc69`). MINOR: login revision not asserted on wire → added (`2d1fc69`).

### Pass 3 (opus) — VERDICT: 0 blockers, 0 majors, 2 minors. **CONVERGED: yes**
All 3 pass-2 fixes CONFIRMED. Remaining 2 minors: login read didn't skip an interposed keep-alive (fixed in `e88b6c7` as part of the ctx-aware handshake loop); property #4 is value-based not literal consumed-length (functionally kills the widthRest mutant — accepted).

### Prong 2 — Security + code-quality review (full impl) — Approved-with-nits
**No crafted input can panic any parser** (verified 10k random + adversarial inputs; all slicing bounds-checked). Findings, all fixed in `e88b6c7`:
- Important: `Reassembler.inflight` had no entry-count cap (could pin ~640 MB via many distinct partial MIDs) → added `MaxInflightMIDs = 64`.
- Important: `Connect` handshake loops ignored ctx (keep-alive flood could hold Connect) → login + subscribe loops now check `ctx.Err()`.
- Minor: `atoiTrim` returned MaxInt64 on overflow → returns 0; malformed MID 0004 logging fixed; `BuildMessage` guards oversized frames.

## Phases 3–4 CONVERGED — 2026-06-02
- Adversarial roast: 3 passes (3B/9M/8m → 0B/1M/2m → 0B/0M/2m-trivial), CONVERGED.
- Security/quality prong: Approved-with-nits, all Important findings resolved.
- 7 fix/test commits (`b3fec19`, `db97a49`, `2d1fc69`, `e88b6c7` + the two test commits). All race/gofmt/vet clean; whole module builds.
