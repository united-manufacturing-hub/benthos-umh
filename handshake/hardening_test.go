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
