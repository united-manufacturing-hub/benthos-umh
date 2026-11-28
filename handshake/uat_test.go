//go:build uat

// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scenario harness for the handshake engine. It builds only with the uat
// tag; make test-handshake runs it, as does
//
//	go test -tags uat -v ./handshake/
//
// Each TestUATnn function is one scenario. The machine is a Binding whose
// every answer is scripted per call, with a recorder that keeps what the
// machine was asked, in what order, and when. A scenario's result is
// Outcome, Err, CleanupErr and the operation log, which runScenario logs.
package handshake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// uatAddr is the machine's own address form, as a script writes it.
type uatAddr struct{ NodeID string }

// rec is one operation the machine was asked to perform. At is when the
// machine was asked, measured from the start of the run; Took is how long
// the machine took to answer.
type rec struct {
	Seq   int
	Op    string // read | write
	Addr  string
	Value any   // what a read returned, or what a write sent
	Err   error // how the machine refused, if it did
	At    time.Duration
	Took  time.Duration
}

func (r rec) String() string {
	s := fmt.Sprintf("%2d @%5dms %-5s %-16s", r.Seq, r.At.Milliseconds(), r.Op, r.Addr)
	if r.Op == "write" {
		s += fmt.Sprintf("= %v", r.Value)
	} else {
		s += fmt.Sprintf("-> %v", r.Value)
	}
	if r.Err != nil {
		s += fmt.Sprintf("  [refused: %v]", r.Err)
	}
	if r.Took > time.Millisecond {
		s += fmt.Sprintf(" (took %dms)", r.Took.Milliseconds())
	}
	return s
}

// reaction is what the machine does for one call: answer with a value,
// refuse with an error, and/or take its time. A nil plan entry means the
// machine behaves normally: a read returns what it holds, a write sticks.
type reaction struct {
	value     any
	err       error
	delay     time.Duration
	ignoreCtx bool // the delay stands whatever ctx says
	panicWith any  // the machine's driver blows up instead of answering
}

// machine is a Binding whose every operation is scripted per call. It
// records each operation in order, so a scenario asserts exactly what the
// machine was asked to do, and when.
type machine struct {
	mu       sync.Mutex
	start    time.Time
	recs     []rec
	values   map[string]any
	plans    map[string][]*reaction
	onWrite  func(addr string, value any) // runs after a write the machine accepted
	parseErr error                        // refused at ParseAddress once set
}

func newMachine(values map[string]any) *machine {
	m := &machine{values: map[string]any{}, plans: map[string][]*reaction{}}
	for k, v := range values {
		m.values[k] = v
	}
	return m
}

// plan appends scripted reactions for one address, consumed in call order.
func (m *machine) plan(addr string, rs ...*reaction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.plans[addr] = append(m.plans[addr], rs...)
}

func (m *machine) set(addr string, v any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[addr] = v
}

// mark starts the recorder's clock; call it right before Run.
func (m *machine) mark() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.start.IsZero() {
		m.start = time.Now()
	}
}

func (m *machine) Namespace() string { return "opcua" }

func (m *machine) ParseAddress(raw map[string]any) (uatAddr, error) {
	m.mu.Lock()
	parseErr := m.parseErr
	m.mu.Unlock()
	if parseErr != nil {
		return uatAddr{}, parseErr
	}
	s, _ := raw["nodeId"].(string)
	if s == "" {
		return uatAddr{}, errors.New("address has no nodeId")
	}
	return uatAddr{NodeID: s}, nil
}

func (m *machine) Read(ctx context.Context, a uatAddr) (ReadResponse, error) {
	v, err := m.act(ctx, "read", a, nil)
	return ReadResponse{Value: v}, err
}

func (m *machine) Write(ctx context.Context, a uatAddr, value any) error {
	_, err := m.act(ctx, "write", a, value)
	return err
}

func (m *machine) Describe(a uatAddr) string { return a.NodeID }

// act is every operation's path: consume one scripted reaction, take the
// scripted delay, then record what happened and when.
func (m *machine) act(ctx context.Context, op string, a uatAddr, sent any) (any, error) {
	m.mu.Lock()
	if m.start.IsZero() {
		m.start = time.Now()
	}
	start := m.start
	plan := m.plans[a.NodeID]
	var r *reaction
	if len(plan) > 0 {
		r, m.plans[a.NodeID] = plan[0], plan[1:]
	}
	onWrite := m.onWrite
	held := m.values[a.NodeID]
	m.mu.Unlock()

	enter := time.Now()
	if r != nil && r.panicWith != nil {
		// The machine's own driver blows up; recorded like any other
		// answer, because the machine was asked.
		m.mu.Lock()
		m.recs = append(m.recs, rec{
			Seq: len(m.recs) + 1, Op: op, Addr: a.NodeID,
			Value: sent, At: enter.Sub(start),
		})
		m.mu.Unlock()
		panic(r.panicWith)
	}
	var answered any
	var err error
	switch {
	case r != nil:
		answered, err = r.value, r.err
	case op == "write":
		answered, err = nil, nil
	default: // a read: what the machine holds
		answered, err = held, nil
	}
	if r != nil && r.delay > 0 {
		if r.ignoreCtx {
			time.Sleep(r.delay) // the machine that ignores its context
		} else {
			select {
			case <-time.After(r.delay):
			case <-ctx.Done():
				answered, err = nil, ctx.Err()
			}
		}
	}
	if err == nil && op == "write" {
		m.set(a.NodeID, sent)
		if onWrite != nil {
			onWrite(a.NodeID, sent)
		}
	}

	recorded := answered
	if op == "write" {
		recorded = sent
	}
	m.mu.Lock()
	m.recs = append(m.recs, rec{
		Seq: len(m.recs) + 1, Op: op, Addr: a.NodeID,
		Value: recorded, Err: err, At: enter.Sub(start), Took: time.Since(enter),
	})
	m.mu.Unlock()
	return answered, err
}

// ops returns the operation log, in order.
func (m *machine) ops() []rec {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]rec, len(m.recs))
	copy(out, m.recs)
	return out
}

// trace renders the log as one op per line: `read ns=2;s=Ready -> true`.
func (m *machine) trace() []string {
	var out []string
	for _, r := range m.ops() {
		if r.Op == "write" {
			out = append(out, fmt.Sprintf("write %s=%v", r.Addr, r.Value))
		} else {
			out = append(out, fmt.Sprintf("read %s->%v", r.Addr, r.Value))
		}
	}
	return out
}

// log renders the full log with timings, for the test output.
func (m *machine) log() string {
	var b strings.Builder
	for _, r := range m.ops() {
		fmt.Fprintf(&b, "    %s\n", r)
	}
	return strings.TrimRight(b.String(), "\n")
}

func (m *machine) count(op string, addr string) int {
	n := 0
	for _, r := range m.ops() {
		if r.Op == op && r.Addr == addr {
			n++
		}
	}
	return n
}

// uatConfig builds a config that validates, for scenarios to adapt.
func uatConfig(script string) Config {
	return Config{
		Script:              script,
		MaxScriptRuntime:    30 * time.Second,
		MaxOnSuccessRuntime: 10 * time.Second,
		MaxOnErrorRuntime:   10 * time.Second,
		PollInterval:        50 * time.Millisecond,
	}
}

// runScenario drives one scenario the way production drives the engine and
// logs everything a second person needs to open the result.
func runScenario(t *testing.T, cfg Config, m *machine, msg map[string]any) (Result, time.Duration) {
	t.Helper()
	engine, err := New(cfg, m)
	if err != nil {
		t.Fatalf("New refused the scenario's config: %v", err)
	}
	defer func() { _ = engine.Close() }()
	m.mark()
	t0 := time.Now()
	res := engine.Run(context.Background(), msg)
	wall := time.Since(t0)
	t.Logf("RESULT: outcome=%v err=%v cleanupErr=%v wall=%v", res.Outcome, res.Err, res.CleanupErr, wall)
	t.Logf("OPERATION LOG:\n%s", m.log())
	return res, wall
}

func expectOutcome(t *testing.T, res Result, want Outcome) {
	t.Helper()
	if res.Outcome != want {
		t.Errorf("outcome %v, want %v (err %v, cleanupErr %v)", res.Outcome, want, res.Err, res.CleanupErr)
	}
}

func expectTrace(t *testing.T, m *machine, want []string) {
	t.Helper()
	got := m.trace()
	if len(got) != len(want) {
		t.Errorf("machine saw %d operations, want %d:\n  got:  %v", len(got), len(want), got)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("operation %d is %q, want %q\n  full log:\n%s", i+1, got[i], want[i], m.log())
			return
		}
	}
}

// happyHandshake is the handshake most scenarios fault: guard reads, a
// payload write, read-back, the committing write, the machine's own
// acknowledgement, end.
const happyHandshake = `
	const READY    = { nodeId: "ns=2;s=Ready" };
	const SETPOINT = { nodeId: "ns=2;s=Setpoint" };
	const APPLY    = { nodeId: "ns=2;s=Apply" };
	const LOADED   = { nodeId: "ns=2;s=Loaded" };

	if (opcua.read(READY) !== true || opcua.read(LOADED) !== false) {
		return opcua.error_not_ready();
	}
	const setpoint = Math.round(msg.payload.value * 100);
	opcua.write(SETPOINT, setpoint);
	if (opcua.read(SETPOINT) !== setpoint) {
		return opcua.error_persistent("setpoint did not stick");
	}
	opcua.write(APPLY, true);
	opcua.wait_until(LOADED, v => v === true, 5000);
	return opcua.success({ value: msg.payload.value });
`

// Happy path: the machine behaves. Delivered, onSuccess ran, onError
// did not, and the operation log shows each write exactly once.
func TestUAT01_HappyPath(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false,
	})
	// The machine takes two polls to raise its own Loaded flag, exactly as
	// a PLC would, and holds it once raised. The guard read at the top of
	// the script consumes the first scripted answer, so the flag's answers
	// are: guard false, poll false, poll false, poll true.
	m.plan("ns=2;s=Loaded",
		&reaction{value: false},
		&reaction{value: false},
		&reaction{value: false},
		&reaction{value: true},
	)
	m.onWrite = func(addr string, v any) {
		if addr == "ns=2;s=Apply" && v == true {
			m.set("ns=2;s=Loaded", true)
		}
	}
	cfg := uatConfig(happyHandshake)
	cfg.OnSuccess = `opcua.write({nodeId: "ns=2;s=Apply"}, false);`
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=ErrorFlag"}, true);`

	res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42.5}})

	expectOutcome(t, res, Delivered)
	if res.Err != nil || res.CleanupErr != nil {
		t.Errorf("a delivered run carries no fault: err %v, cleanupErr %v", res.Err, res.CleanupErr)
	}
	if d, ok := res.Data.(map[string]any); !ok || d["value"] != 42.5 {
		t.Errorf("Data is %v, want {value: 42.5}", res.Data)
	}
	// The exact transaction, in order, each write once.
	expectTrace(t, m, []string{
		"read ns=2;s=Ready->true",
		"read ns=2;s=Loaded->false",
		"write ns=2;s=Setpoint=4250",
		"read ns=2;s=Setpoint->4250",
		"write ns=2;s=Apply=true",
		"read ns=2;s=Loaded->false",
		"read ns=2;s=Loaded->false",
		"read ns=2;s=Loaded->true",
		// onSuccess, after the run ended
		"write ns=2;s=Apply=false",
	})
}

// Never ready: wait_until's predicate never holds. The wait's own
// timeout ends the run, not the script deadline; Transient; onError ran.
func TestUAT02_NeverReadyIsTransient(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false,
	})
	cfg := uatConfig(`
	const READY  = { nodeId: "ns=2;s=Ready" };
	const APPLY  = { nodeId: "ns=2;s=Apply" };
	const LOADED = { nodeId: "ns=2;s=Loaded" };

	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	opcua.write(APPLY, true);
	opcua.wait_until(LOADED, v => v === true, 1500);
	return opcua.success({});
`)
	cfg.MaxScriptRuntime = 30 * time.Second // far beyond the wait, so Validate accepts it
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, wall := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 1}})

	expectOutcome(t, res, Transient)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "timed out") {
		t.Errorf("the wait's own timeout should be the fault, got %v", res.Err)
	}
	if wall < 1500*time.Millisecond || wall > 5*time.Second {
		t.Errorf("run took %v, want the wait's own 1500ms timeout, not the script deadline", wall)
	}
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run exactly once:\n%s", m.log())
	}
	if n := len(m.ops()); n < 4 {
		t.Errorf("machine saw only %d operations, want the guard, the write and the polls", n)
	}
}

// Never ready with { onTimeout: "persistent" }: same, but Persistent.
func TestUAT03_NeverReadyOnTimeoutPersistent(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false,
	})
	cfg := uatConfig(`
	const READY  = { nodeId: "ns=2;s=Ready" };
	const APPLY  = { nodeId: "ns=2;s=Apply" };
	const LOADED = { nodeId: "ns=2;s=Loaded" };

	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	opcua.write(APPLY, true);
	opcua.wait_until(LOADED, v => v === true, 1500, { onTimeout: "persistent" });
	return opcua.success({});
`)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 1}})

	expectOutcome(t, res, Persistent)
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run exactly once:\n%s", m.log())
	}
}

// Slow machine, deadline fires mid-transaction: a read blocks past
// MaxScriptRuntime. Persistent, onError ran, and the machine was
// not asked to do anything after the deadline passed.
func TestUAT04_DeadlineFiresMidTransaction(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false,
	})
	// The confirm read blocks past the deadline; a binding that honors
	// its context returns ctx's own error when the deadline fires.
	m.plan("ns=2;s=Loaded", &reaction{delay: 800 * time.Millisecond})
	cfg := uatConfig(`
	const READY    = { nodeId: "ns=2;s=Ready" };
	const SETPOINT = { nodeId: "ns=2;s=Setpoint" };
	const APPLY    = { nodeId: "ns=2;s=Apply" };
	const LOADED   = { nodeId: "ns=2;s=Loaded" };

	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	opcua.write(SETPOINT, 4200);
	opcua.write(APPLY, true);
	opcua.read(LOADED);
	return opcua.success({});
`)
	cfg.MaxScriptRuntime = 500 * time.Millisecond
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, wall := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42}})

	// The deadline stopped the script mid-transaction; the run is
	// persistent however the binding worded its refusal.
	expectOutcome(t, res, Persistent)
	if res.Err == nil || !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Errorf("the deadline should be the fault, got %v", res.Err)
	}
	if wall < 500*time.Millisecond || wall > 5*time.Second {
		t.Errorf("run took %v, want ~500ms (the deadline) plus the blocked read's tail", wall)
	}
	// Nothing the script asked for started after the deadline passed;
	// the cleanup block's own write is allowed and expected.
	for _, r := range m.ops() {
		if r.Addr == "ns=2;s=Reset" {
			continue
		}
		if r.At >= cfg.MaxScriptRuntime {
			t.Errorf("machine was asked for %s %s at %v, after the %v deadline",
				r.Op, r.Addr, r.At, cfg.MaxScriptRuntime)
		}
	}
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// A binding that ignores its context entirely: it blocks for 10s
// whatever ctx says, with MaxScriptRuntime at 1s. Does Run return at 1s or
// at 10s? Measured, and reported either way.
func TestUAT05_BindingIgnoresItsContext(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false,
	})
	// Ten seconds, and the context can say what it likes.
	m.plan("ns=2;s=Loaded", &reaction{value: true, delay: 10 * time.Second, ignoreCtx: true})
	cfg := uatConfig(`
	const READY    = { nodeId: "ns=2;s=Ready" };
	const SETPOINT = { nodeId: "ns=2;s=Setpoint" };
	const APPLY    = { nodeId: "ns=2;s=Apply" };
	const LOADED   = { nodeId: "ns=2;s=Loaded" };

	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	opcua.write(SETPOINT, 4200);
	opcua.write(APPLY, true);
	opcua.read(LOADED);
	return opcua.success({});
`)
	cfg.MaxScriptRuntime = 1 * time.Second
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, wall := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42}})

	t.Logf("MEASUREMENT: Run returned after %v; MaxScriptRuntime is 1s", wall)
	// The measurement this scenario exists for: goja's interrupt is read in
	// the bytecode loop, so a script parked inside a Go call is not
	// interruptible. Expect ~10s, Persistent, onError still run. The
	// wall time itself is the finding.
	expectOutcome(t, res, Persistent)
	if res.Err == nil || !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Errorf("the deadline should be the fault, got %v", res.Err)
	}
	if wall < 10*time.Second || wall > 20*time.Second {
		t.Errorf("MEASUREMENT OUT OF RANGE: Run returned after %v, want the binding's 10s", wall)
	}
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// Commit write fails with ErrPersistent: Persistent.
func TestUAT06_CommitWriteFailsPersistent(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true, "ns=2;s=Loaded": false})
	m.plan("ns=2;s=Apply", &reaction{err: fmt.Errorf("BadTypeMismatch: %w", ErrPersistent)})
	cfg := uatConfig(happyHandshake)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42}})

	expectOutcome(t, res, Persistent)
	if res.Err == nil || !errors.Is(res.Err, ErrPersistent) {
		t.Errorf("Err must carry the binding's own persistent fault, got %v", res.Err)
	}
	if n := m.count("write", "ns=2;s=Apply"); n != 1 {
		t.Errorf("the committing write was attempted %d times, want once", n)
	}
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// Commit write fails unmarked: Transient.
func TestUAT07_CommitWriteFailsUnmarked(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true, "ns=2;s=Loaded": false})
	m.plan("ns=2;s=Apply", &reaction{err: errors.New("connection reset by peer")})
	cfg := uatConfig(happyHandshake)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42}})

	expectOutcome(t, res, Transient)
	if n := m.count("write", "ns=2;s=Apply"); n != 1 {
		t.Errorf("the committing write was attempted %d times, want once", n)
	}
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// A failure the script catches and recovers from, then succeeds. An
// optional read fails; the script catches it, writes, and calls success.
//
// The script's explicit ending wins, and a fault the script caught stays
// caught: a try/catch around an operation is the script telling the engine
// the failure was expected, so the engine does not second-guess it. A
// script that swallows a fault it should not is a script bug, for the
// person who reviews the script rather than for the engine.
func TestUAT08_ScriptCatchesAFailureAndSucceeds(t *testing.T) {
	t.Run("persistent-marked fault, caught and recovered from", func(t *testing.T) {
		m := newMachine(map[string]any{"ns=2;s=Ready": true})
		m.plan("ns=2;s=Optional", &reaction{err: fmt.Errorf("BadTypeMismatch: %w", ErrPersistent)})
		cfg := uatConfig(`
		const SETPOINT = { nodeId: "ns=2;s=Setpoint" };
		let optional = null;
		try {
			optional = opcua.read({ nodeId: "ns=2;s=Optional" });
		} catch (e) {
			// Optional on this machine; the recipe goes ahead without it.
		}
		opcua.write(SETPOINT, 4200);
		return opcua.success({ optional: optional });
	`)
		res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42}})
		expectOutcome(t, res, Delivered)
		if res.Err != nil {
			t.Errorf("a delivered run carries no Err, got %v", res.Err)
		}
	})
	t.Run("unmarked fault, caught and recovered from", func(t *testing.T) {
		m := newMachine(map[string]any{"ns=2;s=Ready": true})
		m.plan("ns=2;s=Optional", &reaction{err: errors.New("machine unreachable")})
		cfg := uatConfig(`
		const SETPOINT = { nodeId: "ns=2;s=Setpoint" };
		let optional = null;
		try {
			optional = opcua.read({ nodeId: "ns=2;s=Optional" });
		} catch (e) { }
		opcua.write(SETPOINT, 4200);
		return opcua.success({ optional: optional });
	`)
		res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42}})
		expectOutcome(t, res, Delivered)
	})
	// The answer does not depend on which caught fault came first. Both
	// orders are asserted, because an order-dependent answer is the defect
	// these two tests exist to keep out.
	t.Run("transient then persistent, both caught, delivers", func(t *testing.T) {
		res := twoCaughtFaults(t,
			fmt.Errorf("machine unreachable"),
			fmt.Errorf("BadTypeMismatch: %w", ErrPersistent))
		expectOutcome(t, res, Delivered)
		if res.Err != nil {
			t.Errorf("a delivered run carries no Err, got %v", res.Err)
		}
	})
	t.Run("persistent then transient, both caught, delivers", func(t *testing.T) {
		res := twoCaughtFaults(t,
			fmt.Errorf("BadTypeMismatch: %w", ErrPersistent),
			fmt.Errorf("machine unreachable"))
		expectOutcome(t, res, Delivered)
		if res.Err != nil {
			t.Errorf("a delivered run carries no Err, got %v", res.Err)
		}
	})
}

// twoCaughtFaults runs one script shape — two reads that fail and are
// caught, then a write and success — with the two faults in the order
// given. It fails unless each planned reaction fired, so a node id the
// plan and the script spell differently cannot pass with no fault in it.
func twoCaughtFaults(t *testing.T, first error, second error) Result {
	t.Helper()
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	m.plan("ns=2;s=First", &reaction{err: first})
	m.plan("ns=2;s=Second", &reaction{err: second})
	cfg := uatConfig(`
		try { opcua.read({ nodeId: "ns=2;s=First" }); } catch (e) { }
		try { opcua.read({ nodeId: "ns=2;s=Second" }); } catch (e) { }
		opcua.write({ nodeId: "ns=2;s=Setpoint" }, 4200);
		return opcua.success({});
		`)
	res, _ := runScenario(t, cfg, m, map[string]any{})
	if n := m.count("read", "ns=2;s=First"); n != 1 {
		t.Fatalf("the first faulted read ran %d times, want once -- the plan did not fire:\n%s", n, m.log())
	}
	if n := m.count("read", "ns=2;s=Second"); n != 1 {
		t.Fatalf("the second faulted read ran %d times, want once -- the plan did not fire:\n%s", n, m.log())
	}
	return res
}

// A fault the script does not catch decides the outcome, and it is the
// fault that stopped the script, not an earlier one it caught and carried
// on from. Otherwise a caught "machine unreachable" would mask the
// persistent refusal that ended the run, and the message would be retried
// forever.
func TestUAT08b_AnUncaughtFaultDecides_NotAnEarlierCaughtOne(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	m.plan("ns=2;s=First", &reaction{err: errors.New("machine unreachable")})
	m.plan("ns=2;s=Second", &reaction{err: fmt.Errorf("BadTypeMismatch: %w", ErrPersistent)})
	cfg := uatConfig(`
		try { opcua.read({ nodeId: "ns=2;s=First" }); } catch (e) { }
		opcua.read({ nodeId: "ns=2;s=Second" });
		return opcua.success({});
		`)
	res, _ := runScenario(t, cfg, m, map[string]any{})
	if n := m.count("read", "ns=2;s=First"); n != 1 {
		t.Fatalf("the caught read ran %d times, want once -- the plan did not fire:\n%s", n, m.log())
	}
	expectOutcome(t, res, Persistent)
	if !errors.Is(res.Err, ErrPersistent) {
		t.Errorf("the uncaught persistent refusal should be the reported fault, got %v", res.Err)
	}
}

// onError itself fails: Persistent, with CleanupErr naming onError.
func TestUAT09_OnErrorItselfFails(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	m.plan("ns=2;s=Reset", &reaction{err: errors.New("machine unreachable")})
	cfg := uatConfig(`return opcua.error_transient("machine busy");`)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{})

	expectOutcome(t, res, Persistent) // the cleanup did not finish
	if res.CleanupErr == nil || !strings.Contains(res.CleanupErr.Error(), "onError") {
		t.Errorf("CleanupErr must name onError, got %v", res.CleanupErr)
	}
}

// onSuccess fails after a delivery: the delivery stands,
// with CleanupErr naming onSuccess.
func TestUAT10_OnSuccessFailsAfterDelivery(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	cfg := uatConfig(`
	opcua.write({nodeId: "ns=2;s=Apply"}, true);
	return opcua.success({});
`)
	cfg.OnSuccess = `throw new Error("success cleanup bug");`

	res, _ := runScenario(t, cfg, m, map[string]any{})

	expectOutcome(t, res, Delivered)
	if res.CleanupErr == nil || !strings.Contains(res.CleanupErr.Error(), "onSuccess") {
		t.Errorf("CleanupErr must name onSuccess, got %v", res.CleanupErr)
	}
	if res.Data == nil {
		t.Errorf("the delivery stands: Data should survive a failed onSuccess")
	}
}

// Uncaught exception in the script: Persistent, onError ran.
func TestUAT11_UncaughtExceptionInTheScript(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	cfg := uatConfig(`
	const SETPOINT = { nodeId: "ns=2;s=Setpoint" };
	opcua.write(SETPOINT, 4200);
	throw new Error("script bug: bad remainder logic");
`)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42}})

	expectOutcome(t, res, Persistent)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "script bug") {
		t.Errorf("Err should carry the script's own exception, got %v", res.Err)
	}
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// Script ends without calling any ending: Persistent, onError ran.
func TestUAT12_ScriptEndsWithoutAnEnding(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	cfg := uatConfig(`
	const READY = { nodeId: "ns=2;s=Ready" };
	opcua.read(READY);
	// Falls off the end: no success, no error_* call.
`)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{})

	expectOutcome(t, res, Persistent)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "ended without calling") {
		t.Errorf("Err should say the script ended without an ending, got %v", res.Err)
	}
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// Script catches the ending's own throw and carries on, then writes
// again. The transaction ended at success; the machine must not be
// touched again from the script, and the outcome is the first ending.
func TestUAT13_ScriptCatchesTheEndingAndCarriesOn(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	cfg := uatConfig(`
	const APPLY = { nodeId: "ns=2;s=Apply" };
	opcua.write(APPLY, true);
	try {
		return opcua.success({ done: true });
	} catch (e) {
		// The ending threw; a buggy script carries on.
	}
	opcua.write({nodeId: "ns=2;s=Extra"}, 7);
	return opcua.success({ done: true });
`)
	cfg.OnSuccess = ``

	res, _ := runScenario(t, cfg, m, map[string]any{})

	// The first ending wins and is the outcome.
	expectOutcome(t, res, Delivered)
	// But the machine must not be asked for anything after the ending:
	// the transaction is over, and every operation refuses.
	if n := m.count("write", "ns=2;s=Extra"); n != 0 {
		t.Errorf("the machine was written after the transaction ended:\n%s", m.log())
	}
	expectTrace(t, m, []string{"write ns=2;s=Apply=true"})
}

// State between runs: a script that sets a global and defines a
// function, then a second run that looks for them. Both must be gone.
func TestUAT14_NothingCarriesBetweenRuns(t *testing.T) {
	m := newMachine(map[string]any{})
	cfg := uatConfig(`
	if (globalThis.leftover !== undefined || globalThis.helper !== undefined) {
		return opcua.error_persistent("state leaked between runs");
	}
	globalThis.leftover = 42;
	globalThis.helper = function () { return 1; };
	return opcua.success({ fresh: true });
`)

	engine, err := New(cfg, m)
	if err != nil {
		t.Fatalf("New refused the scenario's config: %v", err)
	}
	defer func() { _ = engine.Close() }()

	for run := 1; run <= 3; run++ {
		res := engine.Run(context.Background(), map[string]any{})
		if res.Outcome != Delivered {
			t.Fatalf("run %d: outcome %v, err %v: state leaked between runs",
				run, res.Outcome, res.Err)
		}
	}
}

// A wait computed from the message: msg carries 60s, MaxScriptRuntime
// is 2s. Config validation cannot see this wait. Measure: does Run come
// back at ~2s, with what outcome, and does the clamp hold?
func TestUAT15_WaitComputedFromTheMessage(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false, // never raises
	})
	cfg := uatConfig(`
	const READY  = { nodeId: "ns=2;s=Ready" };
	const APPLY  = { nodeId: "ns=2;s=Apply" };
	const LOADED = { nodeId: "ns=2;s=Loaded" };

	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	opcua.write(APPLY, true);
	// The timeout is the message's, not a literal: Validate cannot see it.
	opcua.wait_until(LOADED, v => v === true, msg.payload.waitMs);
	return opcua.success({});
`)
	cfg.MaxScriptRuntime = 2 * time.Second
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, wall := runScenario(t, cfg, m,
		map[string]any{"payload": map[string]any{"value": 1, "waitMs": float64(60000)}})

	t.Logf("MEASUREMENT: computed 60s wait under a 2s budget returned after %v as %v", wall, res.Outcome)
	// The clamp must hold: the wait ends at its clamped timeout
	// before the deadline, so the run is the wait's own transient timeout,
	// not a deadline stop.
	if wall < 1950*time.Millisecond || wall > 5*time.Second {
		t.Errorf("run took %v, want ~2s: the clamp should end the wait before the deadline", wall)
	}
	if res.Outcome != Transient {
		t.Errorf("outcome %v (err %v): the clamped wait should time out on its own, transient", res.Outcome, res.Err)
	}
	if res.Err != nil && errors.Is(res.Err, context.DeadlineExceeded) {
		t.Errorf("the deadline fired mid-wait: the clamp did not hold (err %v)", res.Err)
	}
}

// Caller's context canceled mid-wait: the run ends, Persistent, and
// onError still runs, on its own clock.
func TestUAT16_CallersContextCancelledMidWait(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false,
	})
	cfg := uatConfig(`
	const READY  = { nodeId: "ns=2;s=Ready" };
	const APPLY  = { nodeId: "ns=2;s=Apply" };
	const LOADED = { nodeId: "ns=2;s=Loaded" };

	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	opcua.write(APPLY, true);
	opcua.wait_until(LOADED, v => v === true, 30000);
	return opcua.success({});
`)
	cfg.MaxScriptRuntime = 60 * time.Second // clears the literal wait, so Validate accepts it
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`
	cfg.MaxOnErrorRuntime = 5 * time.Second

	engine, err := New(cfg, m)
	if err != nil {
		t.Fatalf("New refused the scenario's config: %v", err)
	}
	defer func() { _ = engine.Close() }()
	m.mark()

	// Cancel mid-wait, ahead of the clamp: a deadline-based cancellation
	// cannot land mid-wait, because the clamp ends the wait ~50ms before
	// any deadline.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(150*time.Millisecond, cancel)
	t0 := time.Now()
	res := engine.Run(ctx, map[string]any{"payload": map[string]any{"value": 1}})
	wall := time.Since(t0)
	t.Logf("RESULT: outcome=%v err=%v cleanupErr=%v wall=%v", res.Outcome, res.Err, res.CleanupErr, wall)
	t.Logf("OPERATION LOG:\n%s", m.log())

	expectOutcome(t, res, Persistent)
	if res.Err == nil || !errors.Is(res.Err, context.Canceled) {
		t.Errorf("the cancellation should be the fault, got %v", res.Err)
	}
	if wall > 2*time.Second {
		t.Errorf("run took %v, want ~150ms plus the cleanup", wall)
	}
	// onError ran after the caller gave up, on its own clock.
	var resetAt time.Duration
	resetWrites := 0
	for _, r := range m.ops() {
		if r.Op == "write" && r.Addr == "ns=2;s=Reset" {
			resetWrites++
			resetAt = r.At
		}
	}
	if resetWrites != 1 {
		t.Errorf("onError did not run on its own clock:\n%s", m.log())
	}
	if resetAt < 150*time.Millisecond {
		t.Errorf("onError ran at %v, before the caller canceled", resetAt)
	}
}

// A cleanup whose binding ignores its budget: onError blocks 5s
// under MaxOnErrorRuntime of 1s. Run cannot return before the machine does;
// measured and reported, the same class of finding as TestUAT05.
func TestUAT17_CleanupBindingIgnoresItsBudget(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	m.plan("ns=2;s=Reset", &reaction{delay: 5 * time.Second, ignoreCtx: true})
	cfg := uatConfig(`return opcua.error_transient("machine busy");`)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`
	cfg.MaxOnErrorRuntime = 1 * time.Second

	res, wall := runScenario(t, cfg, m, map[string]any{})

	t.Logf("MEASUREMENT: Run with a 1s onError budget returned after %v", wall)
	expectOutcome(t, res, Persistent) // the cleanup did not finish
	if res.CleanupErr == nil || !strings.Contains(res.CleanupErr.Error(), "onError") {
		t.Errorf("CleanupErr must name onError, got %v", res.CleanupErr)
	}
	if wall < 5*time.Second || wall > 15*time.Second {
		t.Errorf("MEASUREMENT OUT OF RANGE: Run returned after %v, want the binding's 5s", wall)
	}
}

// A machine that accepts a write and then reports the old value on
// read-back ("this machine accepts writes it then ignores"). The script's
// read-back guard should catch it and the run should be Persistent.
func TestUAT18_MachineAcceptsWriteThenReportsOldValue(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false, "ns=2;s=Setpoint": 0,
	})
	// The write sticks in the machine's own state, but the first read-back
	// answers with the stale value, as a PLC mid-scan-cycle does.
	m.plan("ns=2;s=Setpoint",
		nil,                        // the write: accepted
		&reaction{value: int64(0)}, // the read-back: stale
	)
	cfg := uatConfig(happyHandshake)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 42.5}})

	expectOutcome(t, res, Persistent)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "did not stick") {
		t.Errorf("the read-back guard should be the fault, got %v", res.Err)
	}
	if m.count("write", "ns=2;s=Apply") != 0 {
		t.Errorf("the committing write must not be reached when the read-back fails:\n%s", m.log())
	}
	if m.count("write", "ns=2;s=Reset") != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// A flag that only becomes correct after N reads, and flaps on the
// way: wait_until passes on the first true, and a confirm read right after
// can see false again. Read skew inside one handshake, measured.
func TestUAT19_FlappingFlagReadSkew(t *testing.T) {
	m := newMachine(map[string]any{
		"ns=2;s=Ready": true, "ns=2;s=Loaded": false,
	})
	m.plan("ns=2;s=Loaded",
		&reaction{value: false},
		&reaction{value: false},
		&reaction{value: true},  // the wait's answer
		&reaction{value: false}, // the confirm read, one scan cycle later
	)
	m.onWrite = func(addr string, v any) {
		if addr == "ns=2;s=Apply" && v == true {
			m.set("ns=2;s=Loaded", true)
		}
	}
	cfg := uatConfig(`
	const READY  = { nodeId: "ns=2;s=Ready" };
	const APPLY  = { nodeId: "ns=2;s=Apply" };
	const LOADED = { nodeId: "ns=2;s=Loaded" };

	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	opcua.write(APPLY, true);
	opcua.wait_until(LOADED, v => v === true, 5000, { pollMs: 50 });
	const confirm = opcua.read(LOADED);
	return opcua.success({ confirmed: confirm === true });
`)

	res, _ := runScenario(t, cfg, m, map[string]any{"payload": map[string]any{"value": 1}})

	// Measurement: the wait passed on read 3, and the confirm read saw
	// false. The delivery stands (the machine did raise the flag); what
	// this records is the read-skew a script author must know about.
	expectOutcome(t, res, Delivered)
	if d, ok := res.Data.(map[string]any); !ok || d["confirmed"] != false {
		t.Errorf("Data is %v: expected the confirm read to see the flag back down (read skew)", res.Data)
	}
}

// A script that catches its ending and spins: the deadline kills it, and
// the run must not be reported as the ending it recorded on the way. A run
// the deadline stopped is persistent and runs onError; the delivery it
// half-announced is not a delivery.
func TestUAT21_DeadlineOutrunsACaughtEnding(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	cfg := uatConfig(`
	const READY = { nodeId: "ns=2;s=Ready" };
	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	try {
		return opcua.success({x: 1});
	} catch (e) {}
	for (;;) {}
`)
	cfg.MaxScriptRuntime = 150 * time.Millisecond
	cfg.OnSuccess = `opcua.write({nodeId: "ns=2;s=SuccessMarker"}, true);`
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=ErrorMarker"}, true);`

	res, wall := runScenario(t, cfg, m, map[string]any{})

	expectOutcome(t, res, Persistent)
	if res.Data != nil {
		t.Errorf("a run the deadline stopped carries no data, got %v", res.Data)
	}
	if res.Err == nil || !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Errorf("the deadline should be the fault, got %v", res.Err)
	}
	if wall < 150*time.Millisecond || wall > 5*time.Second {
		t.Errorf("run took %v, want ~150ms (the deadline) plus the cleanup", wall)
	}
	if n := m.count("write", "ns=2;s=SuccessMarker"); n != 0 {
		t.Errorf("onSuccess ran for a run the deadline stopped:\n%s", m.log())
	}
	if n := m.count("write", "ns=2;s=ErrorMarker"); n != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// A wait without its predicate, a customer's typo, must be a readable
// persistent fault with the cleanup run, not a panic that takes the
// process and leaves the machine armed.
func TestUAT22_WaitWithoutAPredicateIsAFaultNotAPanic(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Loaded": false})
	cfg := uatConfig(`
	const LOADED = { nodeId: "ns=2;s=Loaded" };
	opcua.write({nodeId: "ns=2;s=Apply"}, true);
	opcua.wait_until(LOADED);
	return opcua.success({});
`)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{})

	expectOutcome(t, res, Persistent)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "predicate") {
		t.Errorf("Err must say the predicate is missing, got %v", res.Err)
	}
	if n := m.count("write", "ns=2;s=Reset"); n != 1 {
		t.Errorf("onError did not run:\n%s", m.log())
	}
}

// A machine whose driver panics mid-handshake: the run is a
// persistent fault, the machine is handed back by onError, and the process
// survives. The recover lives at the runtime seam, so nothing a script or a
// binding can do takes the host with it.
func TestUAT23_AMachineThatPanicsIsAFaultNotACrash(t *testing.T) {
	m := newMachine(map[string]any{"ns=2;s=Ready": true})
	m.plan("ns=2;s=Loaded", &reaction{panicWith: "driver bug: unmapped status 0x0f"})
	cfg := uatConfig(`
	const READY  = { nodeId: "ns=2;s=Ready" };
	const APPLY  = { nodeId: "ns=2;s=Apply" };
	const LOADED = { nodeId: "ns=2;s=Loaded" };
	if (opcua.read(READY) !== true) { return opcua.error_not_ready(); }
	opcua.write(APPLY, true);
	opcua.read(LOADED);
	return opcua.success({});
`)
	cfg.OnError = `opcua.write({nodeId: "ns=2;s=Reset"}, false);`

	res, _ := runScenario(t, cfg, m, map[string]any{})

	expectOutcome(t, res, Persistent)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "panicked") {
		t.Errorf("Err must say the run panicked, got %v", res.Err)
	}
	if n := m.count("write", "ns=2;s=Reset"); n != 1 {
		t.Errorf("onError did not run after the panic:\n%s", m.log())
	}
}

// A cleanup with no budget would wait forever: an omitted deadline
// field in the YAML is all it takes, so it is refused at deploy, with both
// names in the error.
func TestUAT24_AnUnbudgetedCleanupIsRefusedAtDeploy(t *testing.T) {
	m := newMachine(map[string]any{})
	cfg := uatConfig(`return opcua.error_not_ready();`)
	cfg.OnError = `opcua.wait_until({nodeId: "ns=2;s=Idle"}, v => v === true, 86400000);`
	cfg.MaxOnErrorRuntime = 0

	_, err := New(cfg, m)
	if err == nil {
		t.Fatalf("New accepted an onError with no deadline: a machine that never idles would hang Run and Close")
	}
	for _, want := range []string{"onError", "MaxOnErrorRuntime"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal must name %s, got: %v", want, err)
		}
	}
}

// A malformed address: what ParseAddress refuses. Marked persistent is
// persistent; unmarked is transient, like every unmarked binding fault, so
// a binding must mark its own parse faults persistent.
func TestUAT20_MalformedAddress(t *testing.T) {
	t.Run("parse error marked persistent", func(t *testing.T) {
		m := newMachine(map[string]any{})
		m.mu.Lock()
		m.parseErr = fmt.Errorf("nodeId is not a NodeID: %w", ErrPersistent)
		m.mu.Unlock()
		cfg := uatConfig(`opcua.write({nodeId: "ns=2;s=Apply"}, true); return opcua.success({});`)
		res, _ := runScenario(t, cfg, m, map[string]any{})
		expectOutcome(t, res, Persistent)
	})
	t.Run("parse error unmarked is transient", func(t *testing.T) {
		m := newMachine(map[string]any{})
		m.mu.Lock()
		m.parseErr = errors.New("nodeId is not a NodeID")
		m.mu.Unlock()
		cfg := uatConfig(`opcua.write({nodeId: "ns=2;s=Apply"}, true); return opcua.success({});`)
		res, _ := runScenario(t, cfg, m, map[string]any{})
		expectOutcome(t, res, Transient)
	})
}

// A wait_until that times out inside onError: the machine was never
// handed back, so the run is persistent whatever the block's JavaScript did
// with the throw, and the caught and the uncaught shape agree.
func TestUAT25_ACleanupWaitThatTimesOutDoesNotHandTheMachineBack(t *testing.T) {
	// The machine is busy and never stops being busy, and the cleanup
	// waits for idle, then resets.
	t.Run("the block catches the timeout and carries on", func(t *testing.T) {
		m := newMachine(map[string]any{"Busy": true})
		cfg := uatConfig(`return opcua.error_transient("machine busy");`)
		cfg.OnError = `
			try {
				opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 200);
				opcua.write({nodeId: "Reset"}, true);
			} catch (err) { }
		`
		res, _ := runScenario(t, cfg, m, map[string]any{})

		expectOutcome(t, res, Persistent) // the cleanup did not finish
		if res.CleanupErr == nil {
			t.Fatalf("CleanupErr must report the wait that timed out, got nil")
		}
		if !strings.Contains(res.CleanupErr.Error(), "onError") {
			t.Errorf("CleanupErr must name onError, got %v", res.CleanupErr)
		}
		if !strings.Contains(res.CleanupErr.Error(), "Busy") {
			t.Errorf("CleanupErr must name the wait that timed out, got %v", res.CleanupErr)
		}
		// The reset inside the same try never reached the machine: the
		// timeout throws, and a throw skips the rest of the try. The
		// report says so, rather than claiming the handback finished.
		if m.count("write", "Reset") != 0 {
			t.Errorf("a throw inside the try skips the reset:\n%s", m.log())
		}
	})
	// The other half: a wait timeout inside a cleanup
	// ends nothing, so a block that catches the wait alone still resets.
	// "The machine is not touched after the script has ended" protects the
	// transaction a script's first ending decided; a cleanup has no
	// transaction left to protect and the handback is its job.
	t.Run("a block that catches the wait alone still resets", func(t *testing.T) {
		m := newMachine(map[string]any{"Busy": true})
		cfg := uatConfig(`return opcua.error_transient("machine busy");`)
		cfg.OnError = `
			try {
				opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 200);
			} catch (err) { }
			opcua.write({nodeId: "Reset"}, true);
		`
		res, _ := runScenario(t, cfg, m, map[string]any{})

		expectOutcome(t, res, Persistent) // the wait still timed out
		if res.CleanupErr == nil {
			t.Fatalf("CleanupErr must report the wait that timed out, got nil")
		}
		if m.count("write", "Reset") != 1 {
			t.Errorf("the reset after a caught timeout never reached the machine:\n%s", m.log())
		}
	})
	// The same cleanup without the try/catch: the throw aborts the block,
	// so the reset does not run (the script chose not to carry on), and
	// the report agrees with the caught shape.
	t.Run("the same cleanup without the try/catch agrees", func(t *testing.T) {
		m := newMachine(map[string]any{"Busy": true})
		cfg := uatConfig(`return opcua.error_transient("machine busy");`)
		cfg.OnError = `
			opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 200);
			opcua.write({nodeId: "Reset"}, true);
		`
		res, _ := runScenario(t, cfg, m, map[string]any{})

		expectOutcome(t, res, Persistent)
		if res.CleanupErr == nil {
			t.Fatalf("CleanupErr must report the wait that timed out, got nil")
		}
		if !strings.Contains(res.CleanupErr.Error(), "onError") {
			t.Errorf("CleanupErr must name onError, got %v", res.CleanupErr)
		}
		if !strings.Contains(res.CleanupErr.Error(), "Busy") {
			t.Errorf("CleanupErr must name the wait that timed out, got %v", res.CleanupErr)
		}
		if m.count("write", "Reset") != 0 {
			t.Errorf("an aborted block must not have written:\n%s", m.log())
		}
	})
	// The boundary: a cleanup wait that holds in time is not a failure.
	// The machine went idle, the reset landed, the handback finished.
	t.Run("a cleanup wait that held is not a failed cleanup", func(t *testing.T) {
		m := newMachine(map[string]any{"Busy": false})
		cfg := uatConfig(`return opcua.error_transient("machine busy");`)
		cfg.OnError = `
			opcua.wait_until({nodeId: "Busy"}, function(v){ return v === false; }, 5000);
			opcua.write({nodeId: "Reset"}, true);
		`
		res, _ := runScenario(t, cfg, m, map[string]any{})

		expectOutcome(t, res, Transient)
		if res.CleanupErr != nil {
			t.Errorf("a cleanup wait that held finished the cleanup: %v", res.CleanupErr)
		}
		if m.count("write", "Reset") != 1 {
			t.Errorf("the reset never reached the machine:\n%s", m.log())
		}
	})
}
