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
	"testing"
	"time"
)

// These two pin what goja's reflective binding did, after the entries were
// rewritten to own their own argument conversion. Both behaviors were broken
// by that rewrite and the whole suite stayed green, so they are here to make
// the next rewrite go red instead.

type compatMachine struct{ reads int }

func (m *compatMachine) Namespace() string { return "opcua" }
func (m *compatMachine) ParseAddress(raw map[string]any) (string, error) {
	s, _ := raw["nodeId"].(string)
	return s, nil
}
func (m *compatMachine) Describe(a string) string { return a }
func (m *compatMachine) Read(_ context.Context, _ string) (ReadResponse, error) {
	m.reads++
	return ReadResponse{Value: "DONE"}, nil
}
func (m *compatMachine) Write(_ context.Context, _ string, _ any) error { return nil }

func runCompat(t *testing.T, script string) (Result, int) {
	t.Helper()
	m := &compatMachine{}
	engine, err := New[string](Config{
		Script:           script,
		MaxScriptRuntime: time.Second,
		PollInterval:     20 * time.Millisecond,
	}, m)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Close()
	return engine.Run(context.Background(), map[string]any{}), m.reads
}

// TestWaitUntil_ATruthyPredicateSatisfiesTheWait pins JavaScript's own rule.
// goja's reflective binding read a predicate's result with ToBoolean, so
// `v.length`, 1 and "ok" satisfied a wait. A Go type assertion reads every one
// of them as false, the wait runs its full timeout, and in a cleanup block that
// turns a retryable message into a permanent failure with the machine armed.
func TestWaitUntil_ATruthyPredicateSatisfiesTheWait(t *testing.T) {
	for _, body := range []string{
		`return v.length;`, `return 1;`, `return "ok";`, `return {ready:true};`,
	} {
		res, reads := runCompat(t, `opcua.wait_until({nodeId:"Busy"}, function(v){ `+body+` }, 200);
			opcua.success({ok:1});`)
		if res.Outcome != Delivered {
			t.Errorf("predicate %q did not satisfy the wait: %v after %d reads", body, res.Outcome, reads)
		}
		if reads != 1 {
			t.Errorf("predicate %q polled %d times; a satisfied wait reads once", body, reads)
		}
	}
	// The control: a falsy result must still not satisfy it.
	res, _ := runCompat(t, `opcua.wait_until({nodeId:"Busy"}, function(v){ return 0; }, 60);
		opcua.success({ok:1});`)
	if res.Outcome == Delivered {
		t.Error("a falsy predicate satisfied the wait")
	}
}

// TestWaitUntil_ATimeoutCoercesLikeJavaScript pins the other half: goja's
// ToFloat accepted a numeric string and a boolean, and a timeout read out of a
// message often arrives as a string. A value ToFloat cannot read is still an
// error.
func TestWaitUntil_ATimeoutCoercesLikeJavaScript(t *testing.T) {
	for _, arg := range []string{`200`, `"200"`, `true`} {
		res, _ := runCompat(t, `opcua.wait_until({nodeId:"Busy"}, function(v){ return v === "DONE"; }, `+arg+`);
			opcua.success({ok:1});`)
		if res.Outcome != Delivered {
			t.Errorf("timeout %s was refused: %v", arg, res.Err)
		}
	}
	for _, arg := range []string{`{}`, `"abc"`} {
		res, _ := runCompat(t, `opcua.wait_until({nodeId:"Busy"}, function(v){ return v === "DONE"; }, `+arg+`);
			opcua.success({ok:1});`)
		if res.Outcome != Persistent {
			t.Errorf("timeout %s was accepted; a value ToFloat cannot read is an error", arg)
		}
	}
}
