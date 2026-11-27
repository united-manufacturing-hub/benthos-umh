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

package handshake

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// lateWriteBinding counts the writes that reach it after their context is
// done, which a binding that does not check ctx would send to the machine.
type lateWriteBinding struct {
	*fakeBinding
	late atomic.Int32
}

func (b *lateWriteBinding) Write(ctx context.Context, addr string, value any) error {
	if ctx.Err() != nil {
		b.late.Add(1)
	}
	return b.fakeBinding.Write(ctx, addr, value)
}

func TestRun_NoOperationReachesTheBindingAfterTheDeadline(t *testing.T) {
	fake := &fakeBinding{values: map[string]any{}}
	fake.blockRead = func(ctx context.Context) (ReadResponse, error) {
		<-ctx.Done()
		return ReadResponse{}, ctx.Err()
	}
	b := &lateWriteBinding{fakeBinding: fake}
	engine, err := New(Config{
		Script:           `try { opcua.read({nodeId: "A"}) } catch (e) {} opcua.write({nodeId: "B"}, 1); return opcua.success({});`,
		MaxScriptRuntime: 100 * time.Millisecond,
	}, Binding[string](b))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		res := engine.Run(t.Context(), map[string]any{})
		if res.Outcome != Persistent {
			t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
		}
	}
	if n := b.late.Load(); n > 0 {
		t.Fatalf("%d write(s) reached the binding after the deadline", n)
	}
}

func TestRun_ASelfReferencingEndingDetailDoesNotCrash(t *testing.T) {
	engine := newTestEngine(t, &fakeBinding{}, func(c *Config) {
		c.Script = `const o = {}; o.self = o; return opcua.error_transient(o);`
	})
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
}

func TestRun_SuccessDataThatCannotBeEncodedIsAScriptMistake(t *testing.T) {
	engine := newTestEngine(t, &fakeBinding{}, func(c *Config) {
		c.Script = `const o = {}; o.self = o; return opcua.success(o);`
	})
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent || !strings.Contains(res.Err.Error(), "cannot be encoded") {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
}

func TestWaitUntil_ASubMillisecondPollIsOneMillisecond(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Loaded": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `opcua.wait_until({nodeId: "Loaded"}, v => v === true, 100, {pollMs: 0.5}); return opcua.success({});`
	})
	engine.Run(t.Context(), map[string]any{})
	// 100ms at 1ms is at most about 100 reads; no pause at all is thousands.
	if n := b.count("read Loaded"); n > 200 {
		t.Fatalf("%d reads in a 100ms wait: the poll interval truncated to zero", n)
	}
}

func TestWaitUntil_RefusesANegativeTimeout(t *testing.T) {
	engine := newTestEngine(t, &fakeBinding{values: map[string]any{"Loaded": false}}, func(c *Config) {
		c.Script = `opcua.wait_until({nodeId: "Loaded"}, v => v === true, -1e13); return opcua.success({});`
	})
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
}

func TestRun_TheFaultThatStoppedTheRunDecidesTheClassOverAnEarlierCaughtOne(t *testing.T) {
	b := &fakeBinding{
		values: map[string]any{"A": 1},
		errs: map[string]error{
			"read:A":  io.EOF,
			"write:B": fmt.Errorf("refused: %w: %w", ErrPersistent, io.EOF),
		},
	}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `try { opcua.read({nodeId: "A"}) } catch (e) {} opcua.write({nodeId: "B"}, 1); return opcua.success({});`
	})
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent || !strings.Contains(res.Err.Error(), "write B") {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
}

// A NaN timeout is a computed timeout gone wrong, such as a missing message
// field times 1000. The wait refuses it before it reads the machine, with a
// message that says so, and the refusal is a script fault: Persistent.
func TestWaitUntil_RefusesANaNTimeout(t *testing.T) {
	for _, timeout := range []string{"NaN", "0/0", "msg.missing * 1000"} {
		t.Run(timeout, func(t *testing.T) {
			b := &fakeBinding{values: map[string]any{"Loaded": true}}
			engine := newTestEngine(t, b, func(c *Config) {
				c.Script = `opcua.wait_until({nodeId: "Loaded"}, v => v === true, ` + timeout + `); return opcua.success({});`
			})
			res := engine.Run(t.Context(), map[string]any{})
			if res.Outcome != Persistent || res.Err == nil || !strings.Contains(res.Err.Error(), "not a number") {
				t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
			}
			if n := b.count("read Loaded"); n != 0 {
				t.Fatalf("a refused wait read the machine %d time(s)", n)
			}
		})
	}
}

// A script that catches its own success and calls success again delivered
// once, with the data of the first call: the first ending decided the
// transaction, and the second cannot rewrite what it returned.
func TestRun_TheFirstSuccessDataWins(t *testing.T) {
	engine := newTestEngine(t, &fakeBinding{}, func(c *Config) {
		c.Script = `try { opcua.success({n: 1}) } catch (e) {} return opcua.success({n: 2});`
	})
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Delivered {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	d, ok := res.Data.(map[string]any)
	if !ok || d["n"] != int64(1) {
		t.Fatalf("Data is %#v, want the first success's {n: 1}", res.Data)
	}
}

// A typo in a wait's options used to read as the default, so a wait meant to
// end persistent ended Transient and the message was retried. Validate
// refuses a literal one, naming the block and the value.
func TestConfig_ValidateRefusesAnUnknownWaitOption(t *testing.T) {
	for _, tc := range []struct{ name, opts, want string }{
		{"a misspelled onTimeout value", `{onTimeout: "persitent"}`, `"persitent"`},
		{"an onTimeout value of the wrong case", `{"onTimeout": "Persistent"}`, `"Persistent"`},
		{"a misspelled option", `{onTimout: "persistent"}`, `"onTimout"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.OnError = `opcua.wait_until({nodeId: "Idle"}, v => v === true, 100, ` + tc.opts + `);`
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted the options %s", tc.opts)
			}
			for _, want := range []string{"onError", tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("the refusal must name %s, got: %v", want, err)
				}
			}
		})
	}
	for _, opts := range []string{
		`{onTimeout: "persistent"}`, `{onTimeout: "transient", pollMs: 10}`, `{}`, `{onTimeout: msg.mode}`,
	} {
		cfg := validConfig()
		cfg.Script = `opcua.wait_until({nodeId: "Idle"}, v => v === true, 100, ` + opts + `); return opcua.success({});`
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate refused the options %s: %v", opts, err)
		}
	}
}

// A computed onTimeout value is checked when the wait runs: a wrong one is a
// wrong script, Persistent, refused before the machine is read.
func TestWaitUntil_RefusesAComputedUnknownOnTimeout(t *testing.T) {
	b := &fakeBinding{values: map[string]any{"Loaded": false}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `opcua.wait_until({nodeId: "Loaded"}, v => v === true, 50, {onTimeout: msg.mode}); return opcua.success({});`
	})
	res := engine.Run(t.Context(), map[string]any{"mode": "persitent"})
	if res.Outcome != Persistent || res.Err == nil || !strings.Contains(res.Err.Error(), `"persitent"`) {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if n := b.count("read Loaded"); n != 0 {
		t.Fatalf("a refused wait read the machine %d time(s)", n)
	}

	res = engine.Run(t.Context(), map[string]any{"mode": "transient"})
	if res.Outcome != Transient {
		t.Fatalf("an explicit transient wait that ran out: outcome %v, err %v", res.Outcome, res.Err)
	}
}
