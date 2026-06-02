# Open Protocol Input Plugin — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the `open_protocol` Benthos input so it conforms exactly to the converged spec (`docs/superpowers/specs/2026-06-02-open-protocol-input-design.md`), under strict TDD.

**Architecture:** A pure core (header/framing/decode/fan-out/timestamp — no I/O) and an effectful shell (`framer.go`, `session.go`, `open_protocol.go`). One MID 0061 rev-1 telegram fans out into 18 single-value tag messages in one Benthos batch; the batch's single `AckFunc` sends one MID 0062, idempotently and bound to a connection generation. At-least-once: forward then ack.

**Tech Stack:** Go, `github.com/redpanda-data/benthos/v4/public/service`, Ginkgo v2 + Gomega, `pgregory.net/rapid` (property tests), `gremlins` (mutation), dockerized GPL emulator (integration).

**Baseline:** A first draft exists on branch `feature/open-protocol-input` in `open_protocol_plugin/`. The Divergences table in the spec lists what must change. Pure-core files (`header.go`, `framer.go`, most of `mid.go`, `mid_0002.go`, `mid_0004.go`, `mid_0061.go`) are largely spec-conformant and are *verified + extended* with spec edge-case tests; `session.go` and `open_protocol.go` are substantially reworked; `fanout.go` and `timestamp.go` are new.

**Test layering (Testing Trophy):** the bulk of confidence comes from *integration* tests driving the input/session against an in-process fake controller (`fake_controller_test.go`) and a Benthos `StreamBuilder`. Unit tests cover the pure core; property tests cover invariants; the dockerized emulator is the one e2e-ish test.

**Module map:**
- `timestamp.go` *(new)* — `ParseControllerTime(string, *time.Location) (time.Time, error)`
- `fanout.go` *(new)* — `Tag` type + `FanOut(LastTightening) []Tag`
- `mid.go` *(modify)* — reassembly bounds (`maxParts`, `maxAssembledBytes`)
- `session.go` *(rewrite)* — Benthos-native lifecycle: `Connect(ctx)` (dial → login → confirm subscribes → bump `generation` → start connection-scoped keep-alive), `Read(ctx) (Result, error)` (blocking; consumes keepalive/0005/0004 inline, returns the next forwardable result + ack closure, or a connection error), `Close()`. No self-managed reconnect loop, no `out` channel, no `Start/Stop`. Per-result generation-bound idempotent ack; `read_timeout` read deadline; keep-alive-write-failure closes the conn.
- `open_protocol.go` *(rewrite)* — config (`timezone`, `read_timeout` + validation), revision guard + raw passthrough, 18-message fan-out batch, `open_protocol_*` metadata, single idempotent generation-bound `AckFunc`
- test files alongside each

---

## Task 1: `ParseControllerTime` — timezone-aware timestamp parsing (pure)

**Files:**
- Create: `open_protocol_plugin/timestamp.go`
- Test: `open_protocol_plugin/timestamp_test.go`

Implements spec "Timestamp semantics": parse `YYYY-MM-DD:HH:MM:SS` in a supplied `*time.Location`; DST via `time.ParseInLocation`; error on malformed input.

- [ ] **Step 1: Write the failing tests**

```go
// timestamp_test.go
package open_protocol_plugin_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

var _ = Describe("ParseControllerTime", func() {
	It("parses a well-formed timestamp in UTC", func() {
		t, err := op.ParseControllerTime("2026-06-02:14:30:15", time.UTC)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.UTC()).To(Equal(time.Date(2026, 6, 2, 14, 30, 15, 0, time.UTC)))
	})

	It("interprets the wall-clock in the supplied zone", func() {
		berlin, _ := time.LoadLocation("Europe/Berlin")
		t, err := op.ParseControllerTime("2026-06-02:14:30:15", berlin)
		Expect(err).NotTo(HaveOccurred())
		// 14:30 CEST (summer, +02:00) == 12:30 UTC
		Expect(t.UTC()).To(Equal(time.Date(2026, 6, 2, 12, 30, 15, 0, time.UTC)))
	})

	It("returns an error on a malformed timestamp", func() {
		_, err := op.ParseControllerTime("not-a-timestamp", time.UTC)
		Expect(err).To(HaveOccurred())
	})

	It("returns an error on the wrong separator/shape", func() {
		_, err := op.ParseControllerTime("2026-06-02 14:30:15", time.UTC)
		Expect(err).To(HaveOccurred())
	})
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./open_protocol_plugin/ -run TestOpenProtocol 2>&1 | head` (Ginkgo suite) — or `make test-open-protocol`.
Expected: FAIL — `undefined: op.ParseControllerTime`.

- [ ] **Step 3: Write minimal implementation**

```go
// timestamp.go
package open_protocol_plugin

import (
	"fmt"
	"time"
)

// controllerTimeLayout is the Open Protocol timestamp format (param 20/21):
// 19 ASCII chars, no timezone. See spec R2.16 §timestamp semantics.
const controllerTimeLayout = "2006-01-02:15:04:05"

// ParseControllerTime parses an Open Protocol wall-clock timestamp
// (YYYY-MM-DD:HH:MM:SS) in loc. The protocol carries no zone, so loc supplies
// it. DST-ambiguous/nonexistent local times follow time.ParseInLocation.
func ParseControllerTime(s string, loc *time.Location) (time.Time, error) {
	if loc == nil {
		loc = time.UTC
	}
	t, err := time.ParseInLocation(controllerTimeLayout, s, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("open protocol: invalid timestamp %q: %w", s, err)
	}
	return t, nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test-open-protocol`
Expected: PASS (the ParseControllerTime specs green).

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/timestamp.go open_protocol_plugin/timestamp_test.go
git commit -m "feat(open_protocol): timezone-aware controller timestamp parsing"
```

---

## Task 2: `FanOut` — decode → 18 tags (pure)

**Files:**
- Create: `open_protocol_plugin/fanout.go`
- Test: `open_protocol_plugin/fanout_test.go`

Implements spec "Output: rev-1 MID 0061 → 18 fanned tag messages" and FanOut behavioral contract. Pure: `LastTightening` → ordered `[]Tag`. No clock, no I/O (Property #7).

- [ ] **Step 1: Write the failing tests**

```go
// fanout_test.go
package open_protocol_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

var _ = Describe("FanOut", func() {
	sample := op.LastTightening{
		TorqueActual: 50.12, TorqueTarget: 50.00, TorqueMin: 45.00, TorqueMax: 55.00,
		AngleActual: 720, AngleTarget: 700, AngleMin: 650, AngleMax: 760,
		TorqueStatus: 1, AngleStatus: 1, TighteningOK: true, BatchStatus: 1,
		TighteningID: 42, VIN: "WVWZZZ1KZAW000001",
		JobID: 3, PsetNumber: 7, BatchCounter: 5, BatchSize: 10,
	}

	It("emits exactly 18 tags in a fixed order", func() {
		tags := op.FanOut(sample)
		names := make([]string, len(tags))
		for i, t := range tags {
			names[i] = t.Name
		}
		Expect(names).To(Equal([]string{
			"torque_actual", "torque_target", "torque_min", "torque_max",
			"angle_actual", "angle_target", "angle_min", "angle_max",
			"torque_status", "angle_status", "tightening_ok", "batch_status",
			"tightening_id", "vin", "job_id", "pset_number", "batch_counter", "batch_size",
		}))
	})

	It("maps each value with the correct type", func() {
		byName := map[string]any{}
		for _, t := range op.FanOut(sample) {
			byName[t.Name] = t.Value
		}
		Expect(byName["torque_actual"]).To(BeNumerically("~", 50.12, 0.001)) // float64
		Expect(byName["angle_actual"]).To(Equal(720))                        // int
		Expect(byName["tightening_ok"]).To(Equal(true))                      // bool
		Expect(byName["vin"]).To(Equal("WVWZZZ1KZAW000001"))                 // string
		Expect(byName["tightening_id"]).To(Equal(42))
	})
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test-open-protocol`
Expected: FAIL — `undefined: op.FanOut` (and `op.Tag`).

- [ ] **Step 3: Write minimal implementation**

```go
// fanout.go
package open_protocol_plugin

// Tag is one fanned-out measurement from a decoded result: a tag name and its
// scalar value (float64 | int | bool | string). See spec output contract.
type Tag struct {
	Name  string
	Value any
}

// FanOut maps a decoded rev-1 LastTightening to its 18 tags, in a fixed order.
// Pure: no I/O, no clock. The order and set are invariant for rev 1 (spec
// FanOut contract, Property #6).
func FanOut(lt LastTightening) []Tag {
	return []Tag{
		{"torque_actual", lt.TorqueActual},
		{"torque_target", lt.TorqueTarget},
		{"torque_min", lt.TorqueMin},
		{"torque_max", lt.TorqueMax},
		{"angle_actual", lt.AngleActual},
		{"angle_target", lt.AngleTarget},
		{"angle_min", lt.AngleMin},
		{"angle_max", lt.AngleMax},
		{"torque_status", lt.TorqueStatus},
		{"angle_status", lt.AngleStatus},
		{"tightening_ok", lt.TighteningOK},
		{"batch_status", lt.BatchStatus},
		{"tightening_id", lt.TighteningID},
		{"vin", lt.VIN},
		{"job_id", lt.JobID},
		{"pset_number", lt.PsetNumber},
		{"batch_counter", lt.BatchCounter},
		{"batch_size", lt.BatchSize},
	}
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test-open-protocol`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/fanout.go open_protocol_plugin/fanout_test.go
git commit -m "feat(open_protocol): fan-out decode result into 18 tags"
```

---

## Task 3: Reassembly bounds (`maxParts`, `maxAssembledBytes`)

**Files:**
- Modify: `open_protocol_plugin/mid.go` (the `Reassembler` type + `Push`)
- Test: `open_protocol_plugin/mid_test.go`

Implements spec Reassembler invariant + Edge #24 + Memory NFR.

- [ ] **Step 1: Write the failing tests**

```go
// add to mid_test.go, in the Reassembler Describe block
It("rejects a sequence exceeding maxParts", func() {
	rs := op.NewReassembler()
	// A header claiming 17 parts (> maxParts=16).
	h := op.Header{MID: 9000, TotalParts: 17, PartNumber: 1}
	_, ok, err := rs.Push(op.Telegram{Header: h, Data: []byte("x")})
	Expect(ok).To(BeFalse())
	Expect(err).To(HaveOccurred())
})

It("rejects when assembled size would exceed maxAssembledBytes", func() {
	rs := op.NewReassembler()
	big := make([]byte, 40*1024) // 40 KiB per part
	h1 := op.Header{MID: 9001, TotalParts: 3, PartNumber: 1}
	_, _, err := rs.Push(op.Telegram{Header: h1, Data: big})
	Expect(err).NotTo(HaveOccurred())
	h2 := op.Header{MID: 9001, TotalParts: 3, PartNumber: 2}
	_, _, err = rs.Push(op.Telegram{Header: h2, Data: big}) // 80 KiB > 64 KiB
	Expect(err).To(HaveOccurred())
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test-open-protocol`
Expected: FAIL — current `Reassembler` accepts both (no bounds).

- [ ] **Step 3: Write minimal implementation**

In `mid.go`, add constants and enforce them in `Push`:

```go
const (
	maxParts          = 16
	maxAssembledBytes = 64 * 1024
)
```

In `Push`, after `if t.Header.TotalParts <= 1 { return t, true, nil }`, add the part-count guard; and when starting a new partial or appending, enforce the byte ceiling:

```go
	if t.Header.TotalParts > maxParts {
		return Telegram{}, false, fmt.Errorf("open protocol: MID %04d declares %d parts, exceeds max %d", t.Header.MID, t.Header.TotalParts, maxParts)
	}
```

In the append branch (`p.buf = append(p.buf, t.Data...)`), guard before appending:

```go
	if len(p.buf)+len(t.Data) > maxAssembledBytes {
		delete(rs.inflight, mid)
		return Telegram{}, false, fmt.Errorf("open protocol: MID %04d assembled size exceeds %d bytes", mid, maxAssembledBytes)
	}
	p.buf = append(p.buf, t.Data...)
```

Also guard the first part's size when creating the partial.

- [ ] **Step 4: Run to verify it passes**

Run: `make test-open-protocol`
Expected: PASS (new bounds tests green; existing reassembly tests still green).

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/mid.go open_protocol_plugin/mid_test.go
git commit -m "feat(open_protocol): bound multi-part reassembly (maxParts, maxAssembledBytes)"
```

---

## Task 4: Golden-file decode tests + PID-23 exact-length assertion

**Files:**
- Create: `open_protocol_plugin/testdata/mid0061_spec_width.bin`, `testdata/mid0061_emulator_width.bin` (raw data fields)
- Test: `open_protocol_plugin/mid_0061_test.go`

Implements Verification properties #4 (hand-derived R2.16 values + exact widthRest length) and Edge #13. Goldens encode hand-derived expectations, not captured output (spec Tooling note).

- [ ] **Step 1: Write the failing tests**

```go
// mid_0061_test.go — golden, spec width (PID 23 = 10 digits)
It("decodes a hand-derived spec-width rev-1 0061 and consumes PID 23 exactly", func() {
	// Hand-built data field per R2.16 layout; PID 23 = 10-digit tightening id.
	data := op.BuildMID0061Data(op.MID0061Fixture{ // helper defined in test
		CellID: 1, ChannelID: 2, ControllerName: "STATION-A", VIN: "VIN0000001",
		JobID: 3, Pset: 7, BatchSize: 10, BatchCounter: 5,
		TStatus: 1, TorqStatus: 1, AngStatus: 1,
		TorqMin: 4500, TorqMax: 5500, TorqTarget: 5000, TorqActual: 5012,
		AngMin: 650, AngMax: 760, AngTarget: 700, AngActual: 720,
		TS: "2026-06-02:14:30:15", PsetTS: "2026-06-01:08:00:00",
		BatchStatus: 1, TighteningID: "0000000042", // 10 digits
	})
	tel := op.Telegram{Header: op.Header{MID: 61, Revision: 1}, Data: data}
	lt, err := op.ParseLastTightening(tel)
	Expect(err).NotTo(HaveOccurred())
	Expect(lt.TighteningID).To(Equal(42))
	Expect(lt.TorqueActual).To(BeNumerically("~", 50.12, 0.001))
	Expect(lt.AngleActual).To(Equal(720))
	Expect(lt.VIN).To(Equal("VIN0000001"))
})

It("decodes the emulator-width variant (PID 23 = 4 digits)", func() {
	data := op.BuildMID0061Data(op.MID0061Fixture{ /* ...same... */ TighteningID: "0042" })
	tel := op.Telegram{Header: op.Header{MID: 61, Revision: 1}, Data: data}
	lt, err := op.ParseLastTightening(tel)
	Expect(err).NotTo(HaveOccurred())
	Expect(lt.TighteningID).To(Equal(42))
})
```

> Note: `build0061Data` already exists in the draft test helpers. Generalize it into an exported (test-only) `BuildMID0061Data(MID0061Fixture)` so both widths are expressible; keep the existing helper working.

- [ ] **Step 2: Run to verify it fails**

Run: `make test-open-protocol`
Expected: FAIL — helper/fixture not yet defined.

- [ ] **Step 3: Write minimal implementation**

Add the `MID0061Fixture` + `BuildMID0061Data` test helper (in a `_test.go` file) that emits the parameter-ID-formatted data field at the layout widths from the spec table, with the tightening-id width taken from `len(fixture.TighteningID)`. No production change needed (the draft's greedy widthRest already handles both); the test pins the behavior.

- [ ] **Step 4: Run to verify it passes**

Run: `make test-open-protocol`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/mid_0061_test.go
git commit -m "test(open_protocol): golden rev-1 0061 decode (spec + emulator PID-23 widths)"
```

---

## Task 5: Reframe session to `Connect`/`Read`/`Close` + `generation`

**Files:**
- Modify: `open_protocol_plugin/session.go`
- Test: `open_protocol_plugin/session_test.go`, `open_protocol_plugin/fake_controller_test.go`

Replaces the draft's `Start`/`Stop`/`Telegrams()`/`serveLoop` with the benthos-umh native lifecycle (sibling idiom: `modbus`/`s7comm`). `Connect` dials + logs in + confirms subscribes + bumps a monotonic `generation` (spec "Connection binding") + starts a connection-scoped keep-alive writer; `Read` blocks for the next forwardable telegram; `Close` stops keep-alive + closes the conn. **No background reconnect loop** — Benthos re-invokes `Connect` when `ReadBatch` returns `ErrNotConnected` (Task 12).

> Migration note: update `newTestSession` and the existing draft session tests from `Start`/`Stop`/`Telegrams()` to `Connect`/`Close`/`Read`. Existing reconnect-loop tests are removed (reconnect is now Benthos's job; covered at the input layer in Task 12).

- [ ] **Step 1: Write the failing test**

```go
// session_test.go
It("connects (login + subscribe) and bumps the generation", func() {
	fc, err := newFakeController(goodHandler(nil))
	Expect(err).NotTo(HaveOccurred())
	defer fc.close()

	s := newTestSession(fc.addr(), []string{"last_tightening"})
	Expect(s.Connect(ctx)).To(Succeed())
	defer s.Close()
	Expect(s.Generation()).To(Equal(uint64(1)))

	// A second Connect (Benthos re-invoke) closes the old conn and bumps again.
	Expect(s.Connect(ctx)).To(Succeed())
	Expect(s.Generation()).To(Equal(uint64(2)))
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test-open-protocol`
Expected: FAIL — `undefined: (*Session).Connect/Read/Close/Generation`.

- [ ] **Step 3: Write minimal implementation**

In `session.go`: replace `Start`/`Stop`/`serveLoop`/`out chan` with:
- `generation uint64` on `Session` (guarded by `mu`).
- `Connect(ctx) error`: if a conn exists, close it + stop its keep-alive; then `connectAndHandshake` (dial → login → confirm subscribes, Task 8); on success set `s.conn`, bump `s.generation` under `mu`, and start the connection-scoped keep-alive goroutine (bound to this conn; Task 9).
- `Close() error`: idempotent; stop keep-alive, close conn.
- `Generation() uint64`: returns `s.generation` under `mu`.

```go
func (s *Session) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test-open-protocol`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/session.go open_protocol_plugin/session_test.go open_protocol_plugin/fake_controller_test.go
git commit -m "refactor(open_protocol): Connect/Read/Close session lifecycle + generation"
```

---

## Task 6: At-least-once — forward-then-ack via a per-result, generation-bound, idempotent ack closure

**Files:**
- Modify: `open_protocol_plugin/session.go` (remove ack-in-`handle` + drop; emit `(Telegram, ackFn)`); `open_protocol_plugin/mid.go` if a small result-carrier type helps
- Test: `open_protocol_plugin/session_test.go`

Implements the spec **Delivery & Acknowledgement** 3-step algorithm. **This is the highest-risk task — implement exactly the spec sequence.**

`Read(ctx)` must: (a) loop reading frames, consuming keepalive/0005/0004 inline and reassembling multi-part; (b) on the next forwardable telegram, return a `Result` pairing it with an ack closure capturing `(gen, conn)` and a per-result idempotency guard; (c) on connection loss / `read_timeout` (Task 9) return a non-nil error. It never sends the 0062 itself — that is the closure's job, called by the input's `AckFunc`.

```go
// Result couples a received, reassembled telegram with the callback that
// acknowledges it to the controller (MID 0062), exactly once, on the
// connection generation that produced it.
type Result struct {
	Telegram Telegram
	Ack      func() // idempotent; sends one 0062 on the captured conn iff still current
}
```

- [ ] **Step 1: Write the failing tests** (four of the five Property-#8 traces; reconnect trace is Task 7)

```go
// session_test.go
It("sends MID 0062 only after Ack() is called (not during Read)", func() {
	result := []byte(build0061Data(true))
	fc, err := newFakeController(goodHandler([][]byte{result}))
	Expect(err).NotTo(HaveOccurred())
	defer fc.close()

	s := newTestSession(fc.addr(), []string{"last_tightening"})
	Expect(s.Connect(ctx)).To(Succeed())
	defer s.Close()

	res, err := s.Read(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(res.Telegram.Header.MID).To(Equal(op.MIDLastTightening))
	// Before Ack, the controller must NOT have seen a 0062.
	Consistently(func() int { return fc.countMID(op.MIDLastTighteningAck) }, 200*time.Millisecond).Should(Equal(0))
	res.Ack()
	Expect(waitFor(time.Second, func() bool { return fc.countMID(op.MIDLastTighteningAck) == 1 })).To(BeTrue())
})

It("sends at most one 0062 even if Ack() is called twice", func() {
	// ... res, _ := s.Read(ctx); res.Ack(); res.Ack()
	// Expect countMID(0062) stays 1.
})
```

> Replace the draft's `Telegrams() <-chan Telegram` (and `serve`/`handle`) with `Read(ctx) (Result, error)`. Update existing session tests that drained `Telegrams()` to call `Read` and `.Ack()`.

- [ ] **Step 2: Run to verify it fails**

Run: `make test-open-protocol`
Expected: FAIL — `Read`/`Result.Ack` undefined; draft sent 0062 inside `handle`.

- [ ] **Step 3: Write minimal implementation**

In `session.go`, implement `Read` as the inline consume-loop and `makeAck` as the spec's 3-step closure:

```go
// Read returns the next forwardable telegram paired with its 0062 ack closure,
// or a connection error (surfaced to Benthos as service.ErrNotConnected).
func (s *Session) Read(ctx context.Context) (Result, error) {
	s.mu.Lock()
	conn, gen, fr := s.conn, s.generation, s.fr
	s.mu.Unlock()
	if conn == nil {
		return Result{}, errNotConnected
	}
	for {
		if s.cfg.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout)) // Task 9
		}
		frame, err := fr.ReadFrame()
		if err != nil {
			return Result{}, err // connection lost / idle → ErrNotConnected at input layer
		}
		tel, err := ParseTelegram(frame)
		if err != nil {
			s.log.Warnf("open protocol: dropping malformed telegram: %v", err)
			continue
		}
		switch tel.Header.MID {
		case MIDKeepAlive, MIDCommandAccepted:
			continue
		case MIDCommandError:
			ce, _ := ParseCommandError(tel)
			s.log.Warnf("open protocol: controller reported %v", ce)
			continue
		}
		complete, ok, rerr := s.reassembler.Push(tel)
		if rerr != nil {
			s.log.Warnf("open protocol: reassembly error: %v", rerr)
			continue
		}
		if !ok {
			continue
		}
		ackMID, needsAck := ackFor(complete.Header.MID)
		return Result{Telegram: complete, Ack: s.makeAck(gen, conn, ackMID, needsAck)}, nil
	}
}

// makeAck builds the idempotent, generation-bound 0062 sender (spec
// Delivery & Acknowledgement 3-step algorithm). Lock order: mu -> guard -> writeMu.
func (s *Session) makeAck(gen uint64, conn net.Conn, ackMID int, needsAck bool) func() {
	var once sync.Once
	return func() {
		if !needsAck {
			return
		}
		s.mu.Lock()
		current := s.generation
		s.mu.Unlock()
		if current != gen {
			return // stale: controller re-pushes on the new session (Edge #22)
		}
		once.Do(func() {
			if err := s.write(conn, ackMID, 1, nil); err != nil {
				s.log.Warnf("open protocol: 0062 ack write failed (benign): %v", err) // Edge #28; no reconnect
			}
		})
	}
}

var errNotConnected = fmt.Errorf("open protocol: not connected")
```

The `FrameReader` for the current connection is stored on `Session` (`s.fr`) in `Connect` so `Read` reuses the buffered reader.

> Note on the err!=nil-then-err==nil case: that ordering lives in the **input's** `AckFunc` (Task 12) — it only calls `res.Ack()` when Benthos passes `err==nil`, and the `once` guard ensures one 0062. The session closure has no err parameter.

- [ ] **Step 4: Run to verify it passes**

Run: `make test-open-protocol`
Expected: PASS (the four ack traces green; updated existing tests green).

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/session.go open_protocol_plugin/session_test.go
git commit -m "feat(open_protocol): at-least-once forward-then-ack with idempotent, generation-bound 0062"
```

---

## Task 7: Reconnect-before-ack trace (stale ack is a no-op)

**Files:**
- Modify: `open_protocol_plugin/session_test.go` (test only; behavior implemented in Task 6)

Implements Property #8 reconnect trace + Edge #22.

- [ ] **Step 1: Write the failing test**

```go
It("makes a stale Ack() (after re-Connect) a no-op on the new connection", func() {
	// 1. Connect (gen 1); Read one 0061 → res (its Ack captures gen 1, conn A).
	// 2. Re-Connect (gen 2, conn B) — simulates Benthos re-invoking Connect.
	// 3. Call res.Ack(): generation check (1 != 2) ⇒ no-op; conn B receives NO
	//    0062 attributable to the stale result.
	// fakeController tracks per-connection received MIDs to assert conn B's count.
})
```

- [ ] **Step 2: Run** — Expected: FAIL if Task 5/6's generation capture/check is wrong; PASS confirms it. (If it passes immediately, Task 5/6 already satisfied it — note per Red Gate and proceed.)

- [ ] **Step 3:** No new production code expected (Task 6 implements it). If the test reveals a gap, fix `makeAck`.

- [ ] **Step 4: Run** — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/session_test.go
git commit -m "test(open_protocol): stale ack after reconnect is a no-op"
```

---

## Task 8: Subscribe confirmation (await MID 0005/0004 within `request_timeout`)

**Files:**
- Modify: `open_protocol_plugin/session.go` (`connectAndHandshake`, called by `Connect`)
- Test: `open_protocol_plugin/session_test.go`

Implements spec Session `Connect` postcondition + Divergences "Subscribe".

- [ ] **Step 1: Write the failing tests**

```go
It("fails Start when a subscribe is rejected (MID 0004)", func() {
	fc, err := newFakeController(func(fc *fakeController, conn net.Conn) {
		defer conn.Close()
		fr := op.NewFrameReader(conn)
		for {
			tel, err := fc.readTelegram(fr)
			if err != nil { return }
			switch tel.Header.MID {
			case op.MIDCommunicationStart:
				ack := "01" + "0001" + "02" + "01" + "03" + padRight("UMHTestSim", 25)
				_ = sendFrame(conn, op.MIDCommunicationStartAck, 1, []byte(ack))
			case op.MIDLastTighteningSub:
				_ = sendFrame(conn, op.MIDCommandError, 1, []byte("006097")) // subscribe rejected
			}
		}
	})
	Expect(err).NotTo(HaveOccurred())
	defer fc.close()
	s := newTestSession(fc.addr(), []string{"last_tightening"})
	Expect(s.Connect(ctx)).NotTo(Succeed())
})

It("confirms a subscribe via MID 0005 (command accepted)", func() {
	// goodHandler updated to reply MID 0005 after MID 0060 — Connect succeeds.
})
```

- [ ] **Step 2: Run** — Expected: FAIL (current code is fire-and-forget; Connect succeeds even on 0004).

- [ ] **Step 3: Write minimal implementation**

In `connectAndHandshake`, after sending each subscribe MID, read the reply within `RequestTimeout`: MID 0005 ⇒ confirmed; MID 0004 ⇒ return the `CommandError`; a result telegram (e.g. 0061) ⇒ also confirms (hold it so the first `Read` returns it); timeout ⇒ error. Update `goodHandler` to reply MID 0005 on subscribe.

- [ ] **Step 4: Run** — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/session.go open_protocol_plugin/session_test.go open_protocol_plugin/fake_controller_test.go
git commit -m "feat(open_protocol): confirm subscriptions (MID 0005/0004) during Start"
```

---

## Task 9: `read_timeout` dead-link detection + keep-alive-write-failure → `Read` errors

**Files:**
- Modify: `open_protocol_plugin/session.go`
- Test: `open_protocol_plugin/session_test.go`

Implements spec Session invariant + Edge #23 + Edge #25. (The read deadline is set in `Read`, Task 6; this task wires `ReadTimeout` config + the keep-alive writer's failure path so both surface as a `Read` error → `ErrNotConnected` → Benthos re-`Connect`.)

- [ ] **Step 1: Write the failing tests**

```go
It("returns a Read error when no telegram arrives within read_timeout", func() {
	// Controller accepts login+subscribe, replies 0005, then goes silent.
	// With ReadTimeout=150ms (keepalive 50ms), Read must return a non-nil error.
	fc, err := newFakeController(silentAfterSubscribeHandler())
	Expect(err).NotTo(HaveOccurred())
	defer fc.close()
	s := newTestSessionWithReadTimeout(fc.addr(), []string{"last_tightening"}, 150*time.Millisecond)
	Expect(s.Connect(ctx)).To(Succeed())
	defer s.Close()
	_, err = s.Read(ctx)
	Expect(err).To(HaveOccurred())
})
```

- [ ] **Step 2: Run** — Expected: FAIL (no `ReadTimeout` field / no deadline yet).

- [ ] **Step 3: Write minimal implementation**

Add `ReadTimeout time.Duration` to `SessionConfig` (default applied in `NewSession`). The read deadline is already applied in `Read` (Task 6) — a deadline-exceeded `ReadFrame` error returns from `Read`. In the connection-scoped keep-alive goroutine (started in `Connect`), on a write error **close the conn** so the blocked/next `ReadFrame` in `Read` errors out (Edge #25); the goroutine then exits. No reconnect logic here — Benthos re-invokes `Connect`.

- [ ] **Step 4: Run** — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/session.go open_protocol_plugin/session_test.go
git commit -m "feat(open_protocol): read_timeout + keep-alive-failure surface as Read errors"
```

---

## Task 10: Input config — `timezone`, `read_timeout`, and config-load validation

**Files:**
- Modify: `open_protocol_plugin/open_protocol.go` (`configSpec`, `newOpenProtocolInput`)
- Test: `open_protocol_plugin/open_protocol_test.go`

Implements spec Config table + Edge #15 + Edge #26. `time.LoadLocation` runs here (shell), passing `*time.Location` into the core.

- [ ] **Step 1: Write the failing tests**

```go
It("rejects an unknown timezone at config load", func() {
	env := service.NewEnvironment()
	b := env.NewStreamBuilder()
	err := b.AddInputYAML(`
open_protocol:
  endpoint: "127.0.0.1:4545"
  timezone: "Mars/Phobos"
`)
	Expect(err).To(HaveOccurred())
})

It("rejects read_timeout < 2x keepalive_interval", func() {
	env := service.NewEnvironment()
	b := env.NewStreamBuilder()
	err := b.AddInputYAML(`
open_protocol:
  endpoint: "127.0.0.1:4545"
  keepalive_interval: 10s
  read_timeout: 5s
`)
	Expect(err).To(HaveOccurred())
})
```

- [ ] **Step 2: Run** — Expected: FAIL (fields/validation absent).

- [ ] **Step 3: Write minimal implementation**

Add `fieldTimezone` (default `"UTC"`) and `fieldReadTimeout` (default `"30s"`) to `configSpec` (both `.Advanced()`). In `newOpenProtocolInput`: `loc, err := time.LoadLocation(tz)` → return error on failure; validate `readTimeout >= 2*keepAlive` → return error otherwise; pass `loc` and `readTimeout` into the input/session config.

- [ ] **Step 4: Run** — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/open_protocol.go open_protocol_plugin/open_protocol_test.go
git commit -m "feat(open_protocol): timezone + read_timeout config with fail-fast validation"
```

---

## Task 11: Input — revision guard + raw passthrough routing

**Files:**
- Modify: `open_protocol_plugin/open_protocol.go`
- Test: `open_protocol_plugin/open_protocol_test.go`

Implements spec decode-time revision guard + Edge #9/#10 + "Output: any other telegram → one raw message".

- [ ] **Step 1: Write the failing tests**

```go
It("passes a non-rev-1 0061 through raw (no fan-out)", func() {
	// fakeController pushes a 0061 with header revision 2.
	// Expect a single raw message: meta open_protocol_mid=0061, revision=2,
	// payload == raw data field; NO open_protocol_tag_name set.
})

It("passes a non-0061 MID (alarm 0071) through raw", func() {
	// As existing draft test, but assert open_protocol_* metadata names.
})
```

- [ ] **Step 2: Run** — Expected: FAIL (current code decodes 0061 regardless of revision; metadata is `op_*`).

- [ ] **Step 3: Write minimal implementation**

In the telegram→message routing: decode+fan-out only when `tel.Header.MID == MIDLastTightening && tel.Header.Revision == 1`; otherwise emit one raw message. (Revision is already normalized by `ParseHeader`.)

- [ ] **Step 4: Run** — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/open_protocol.go open_protocol_plugin/open_protocol_test.go
git commit -m "feat(open_protocol): rev-1 decode guard; raw passthrough for other telegrams"
```

---

## Task 12: Input — 18-message fan-out batch, `open_protocol_*` metadata, single idempotent AckFunc

**Files:**
- Modify: `open_protocol_plugin/open_protocol.go` (`ReadBatch`, message construction)
- Test: `open_protocol_plugin/open_protocol_test.go`

Implements the spec Output contract + Input behavioral contract + Delivery & Acknowledgement at the Benthos layer. **Integration tests via StreamBuilder form the bulk of confidence here.**

- [ ] **Step 1: Write the failing tests**

```go
It("emits 18 fanned messages in one batch sharing one timestamp_ms, with open_protocol_* metadata", func() {
	// Drive a rev-1 0061 (timestamp param 20 = "2026-06-02:14:30:15") through
	// a StreamBuilder + consumer. Collect messages.
	// Assert:
	//  - exactly 18 messages with distinct open_protocol_tag_name values
	//  - all share identical timestamp_ms metadata
	//  - open_protocol_mid=0061, open_protocol_revision=1,
	//    open_protocol_vin / tightening_id present as TAG (not metadata)
	//  - torque_actual payload ~= 50.12 ; angle_actual == 720 ; tightening_ok == true
})

It("sends exactly one MID 0062 after the whole 18-message batch is delivered", func() {
	// Via fakeController: assert countMID(0062) == 1 after the batch is consumed.
})
```

- [ ] **Step 2: Run** — Expected: FAIL (current code emits one object with `op_*` meta; no fan-out; ack-before-forward).

- [ ] **Step 3: Write minimal implementation**

Wire the input lifecycle to the native session API: input `Connect(ctx)` → `session.Connect(ctx)`; input `Close(ctx)` → `session.Close()`.

Rework `ReadBatch`/message construction:
- Call `res, err := session.Read(ctx)`; on `err != nil` return `nil, nil, service.ErrNotConnected` (prompts Benthos to re-`Connect`).
- If rev-1 0061: decode → `FanOut` → build a `service.MessageBatch` of 18 messages. Compute `timestamp_ms` once via `ParseControllerTime(param20, loc)` (omit on parse error, Edge #14); set shared metadata on each message (`open_protocol_mid`, `_revision`, `_timestamp`, `_pset_change_timestamp`, `_cell_id`, `_channel_id`, `_station_id`, `_spindle_id`, `_controller_name`, `_endpoint`, `timestamp_ms`) plus per-message `open_protocol_tag_name` and the scalar payload (use `service.NewMessage(nil)` + `SetStructured(tag.Value)`).
- Else: single raw message (Task 11).
- Return the batch and an `AckFunc` that calls `res.Ack()` only when `err == nil` (the session closure handles idempotency + generation binding):

```go
func(ctx context.Context, err error) error {
	if err == nil {
		res.Ack() // sends the single 0062 once, iff still current
	}
	return nil
}
```

- Rename every `op_*` metadata key to `open_protocol_*`.

- [ ] **Step 4: Run** — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/open_protocol.go open_protocol_plugin/open_protocol_test.go
git commit -m "feat(open_protocol): 18-message fan-out batch with open_protocol_* metadata and single ack"
```

---

## Task 13: Property tests (rapid) — header round-trip + timestamp

**Files:**
- Create: `open_protocol_plugin/property_test.go`
- Modify: `go.mod` (add `pgregory.net/rapid` if absent)

Implements Verification properties #1 and #9.

- [ ] **Step 1: Write the failing tests**

```go
// property_test.go
func TestHeaderRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		mid := rapid.IntRange(1, 9999).Draw(t, "mid")
		rev := rapid.IntRange(1, 999).Draw(t, "rev")
		h, err := op.ParseHeader(op.BuildMessage(mid, rev, []byte("")))
		if err != nil { t.Fatal(err) }
		if h.MID != mid || h.Revision != rev {
			t.Fatalf("round-trip mismatch: mid %d->%d rev %d->%d", mid, h.MID, rev, h.Revision)
		}
	})
}

func TestParseControllerTimeRejectsGarbage(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.StringMatching(`[A-Za-z ]{1,25}`).Draw(t, "s")
		if _, err := op.ParseControllerTime(s, time.UTC); err == nil {
			t.Fatalf("expected error for %q", s)
		}
	})
}
```

- [ ] **Step 2: Run** — Expected: FAIL (rapid missing) → `go get pgregory.net/rapid` then FAIL→PASS as appropriate.

- [ ] **Step 3:** Add the dependency; no production change.

- [ ] **Step 4: Run** — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/property_test.go go.mod go.sum
git commit -m "test(open_protocol): property tests for header round-trip and timestamp parsing"
```

---

## Task 14: Update integration test + docs for the new output shape

**Files:**
- Modify: `open_protocol_plugin/integration_test.go` (assert fan-out output)
- Modify: `docs/input/open-protocol-input.md`

Implements spec Integration tooling + the durability/back-pressure caveats in user docs.

- [ ] **Step 1: Write the failing test changes**

Update the dockerized-emulator test to assert the new shape: collect messages with `open_protocol_mid == "0061"`, assert ≥ 18 fanned messages share one `timestamp_ms`, and that `open_protocol_tag_name` includes `torque_actual`. (Still gated by `TEST_OPEN_PROTOCOL`.)

- [ ] **Step 2: Run** — Run: `make test-open-protocol-integration` (requires Docker). Expected: FAIL until Tasks 10–12 land; then PASS.

- [ ] **Step 3: Update docs**

Rewrite `docs/input/open-protocol-input.md`: config table (incl. `timezone`, `read_timeout`), the 18-tag fan-out output + `open_protocol_*` metadata, a `tag_processor → uns` example showing `timestamp_ms` inheritance, and the at-least-once + reconnect-durability + back-pressure caveats verbatim from the spec NFRs.

- [ ] **Step 4: Run** — `make test-open-protocol` (unit) green; integration green with Docker.

- [ ] **Step 5: Commit**

```bash
git add open_protocol_plugin/integration_test.go docs/input/open-protocol-input.md
git commit -m "test+docs(open_protocol): integration assertions and user docs for fan-out output"
```

---

## Final verification (pre-Phase-3)

- [ ] `make test-open-protocol` — all unit/integration-lite tests green
- [ ] `make test-open-protocol-integration` (Docker) — green
- [ ] `golangci-lint run ./open_protocol_plugin/...` — clean
- [ ] `make license-check` — headers present on new files
- [ ] Confirm no `op_*` metadata keys remain: `grep -rn '"op_' open_protocol_plugin/` returns nothing
- [ ] Update `vsdd/status.md`: Phase 2 complete

## Spec coverage map

| Spec item | Task |
|-----------|------|
| Timestamp semantics / `ParseControllerTime` / DST | 1, 13 |
| 18-tag fan-out (`FanOut`, order, types) | 2, 12 |
| Reassembly bounds (Edge #24) | 3 |
| rev-1 0061 decode + PID-23 widths (Edge #13) | 4 |
| Connection generation | 5 |
| At-least-once forward-then-ack; idempotent; generation-bound (Delivery & Ack, Edge #21/#22/#27/#28) | 6, 7, 12 |
| Subscribe confirmation (Start postcondition) | 8 |
| `read_timeout` / keep-alive-write-failure (Edge #23/#25) | 9 |
| `timezone` + `read_timeout` config + validation (Edge #15/#26) | 10 |
| Revision guard + raw passthrough (Edge #9/#10) | 11 |
| `open_protocol_*` metadata rename | 11, 12 |
| Header round-trip property (#1) | 13 |
| Integration (emulator) + docs + caveats | 14 |
| FrameReader framing (Edge #1/#2/#3) | already in draft; verified by existing `framer_test.go` |
