package handshake

import (
	"context"
	"testing"
	"time"
)

type endingMachine struct{}

func (endingMachine) Namespace() string { return "opcua" }
func (endingMachine) ParseAddress(raw map[string]any) (string, error) {
	s, _ := raw["nodeId"].(string)
	return s, nil
}
func (endingMachine) Describe(a string) string { return a }
func (endingMachine) Read(ctx context.Context, a string) (ReadResponse, error) {
	return ReadResponse{Value: true}, nil
}
func (endingMachine) Write(ctx context.Context, a string, v any) error { return nil }

func runEnding(t *testing.T, script string) Result {
	t.Helper()
	e, err := New[string](Config{Script: script, MaxScriptRuntime: time.Second}, endingMachine{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer e.Close()
	return e.Run(context.Background(), map[string]any{})
}

// TestRun_AScriptThatNeverEndsIsPersistent pins the spec's rule that a script
// which returns without declaring an ending is a wrong script, and a wrong
// script cannot be fixed by repeating. classify's default case had no test.
func TestRun_AScriptThatNeverEndsIsPersistent(t *testing.T) {
	res := runEnding(t, `var x = 1 + 1;`)
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, want Persistent (err %v)", res.Outcome, res.Err)
	}
	if res.Err == nil {
		t.Fatal("a run that never ended must say so")
	}
}

// TestRun_TheFirstEndingWins pins runState.finish's guard: a script that
// catches an ending's throw and calls another cannot change its mind.
func TestRun_TheFirstEndingWins(t *testing.T) {
	res := runEnding(t, `try { opcua.error_transient("first"); } catch (e) { }
		try { opcua.success({second: true}); } catch (e) { }`)
	if res.Outcome != Transient {
		t.Fatalf("outcome %v, want Transient: the first ending decides (err %v)", res.Outcome, res.Err)
	}
	if res.Data != nil {
		t.Fatalf("the second ending's data reached the caller: %v", res.Data)
	}
}
