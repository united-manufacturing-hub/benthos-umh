# VSDD Status: Atlas Copco Open Protocol Input Plugin

Intensity: **Full** (Phases 1 → 6)
Spec: `docs/superpowers/specs/2026-06-02-open-protocol-input-design.md`
Branch: `feature/open-protocol-input`

## Phase Completion

- [x] **Phase 1 — Spec Crystallization** (complete 2026-06-02, user-approved)
  - [x] Brainstorming: explore → questions → design converged
  - [x] Spec written (pitch preamble + behavioral contract + edge cases + verification strategy)
  - [x] Verified MID 0061 timestamp has no timezone against spec R2.16
  - [x] Adversarial spec review (fresh context) — CONVERGED after 4 rounds (3B/9M/8m → 0B/0M/4m)
  - [x] remove-ai-slop pass (one personification fix applied)
  - [x] **User approved** (2026-06-02)
- [ ] **Phase 2 — TDD Implementation** (in progress)
  - [x] writing-plans: bite-sized, TDD-ordered plan (`docs/superpowers/plans/2026-06-02-open-protocol-input.md`, 14 tasks)
  - [x] sibling-convention audit (fresh context): conforms; one HIGH divergence (self-managed reconnect) → **refactored to Benthos-native `Connect`/`Read`/`Close`** per user decision; spec + plan Tasks 5–9 reconciled
  - [x] Executing on worktree `feature/open-protocol-vsdd` (subagent-driven, TDD red-before-green, per-task commits + spec/quality review)
  - Task progress (race-clean throughout):
    - [x] Task 1 — `ParseControllerTime` (tz/DST) — `6970b5d`
    - [x] Task 2 — `FanOut` → 18 tags — `62be5bf`
    - [x] Task 3 — reassembly bounds — `ec229b1`
    - [x] Task 4 — golden rev-1 0061 decode (PID-23 widths) — `13e341f`
    - [x] Tasks 5–9 — Benthos-native session (Connect/Read/Close, generation-bound idempotent ack, subscribe-confirm, read_timeout) — `1215104`, review fixes `4b62bb7` (opus code-quality review: Approved-with-nits; I1 reassembler-under-mu + I2 Close-vs-loss fixed)
    - [ ] Tasks 10–12 — input config + revision guard + 18-message fan-out batch + `open_protocol_*` metadata
    - [ ] Task 13 — property tests (rapid)
    - [ ] Task 14 — integration test + docs
- [ ] Phase 3 — Adversarial Roast
- [ ] Phase 4 — Feedback Loop
- [ ] Phase 5 — Formal Hardening (mutation ≥ 95%)
- [ ] Phase 6 — Convergence

## Key Decisions (locked)

1. Read-only input; MID 0061 rev-1 only, verified per-telegram via header revision; else raw pass-through.
2. At-least-once delivery: forward, then AckFunc fires MID 0062.
3. Fan-out at source — 18 tags; asset identity (cell/channel/station/spindle/controller_name) → metadata/topic path.
4. Source timestamp parsed with configurable `timezone` (default UTC) → `timestamp_ms` metadata.
5. `open_protocol_*` metadata prefix (renamed from draft's `op_*`).
6. Pure core (header/parse/decode/fanout/timestamp) vs effectful shell (framer/session/input).
