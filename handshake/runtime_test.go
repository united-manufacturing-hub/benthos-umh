package handshake

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// LIB-7: the script's own variables are reset for every run. A script's
// globals must not reach the next message, which is what the idempotency
// limit rests on.
func TestRun_CarriesNothingBetweenRuns(t *testing.T) {
	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `
			if (typeof globalThis.x === "undefined") {
				globalThis.x = 1;
				return opcua.error_transient("first run");
			}
			return opcua.success({carried: globalThis.x});
		`
	})

	// The first run leaves a global behind; the second run must not see it.
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("first run: outcome %v, err %v", res.Outcome, res.Err)
	}
	res = engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Transient {
		t.Fatalf("a global reached the next message: outcome %v, data %v", res.Outcome, res.Data)
	}
}

// LIB-13: the value a binding returns reaches the script unchanged. The
// binding decides what counts as a value and what counts as an error.
func TestRead_PassesTheBindingsValueThroughUnchanged(t *testing.T) {
	values := map[string]any{
		"flag":  true,
		"count": int64(-5),
		"name":  "keep me",
		"ratio": 1.5,
	}
	for addr, want := range values {
		t.Run(fmt.Sprintf("%T", want), func(t *testing.T) {
			b := &fakeBinding{values: map[string]any{addr: want}}
			engine := newTestEngine(t, b, func(c *Config) {
				c.Script = fmt.Sprintf(`
					const v = opcua.read({nodeId: %q});
					if (v !== %s) { return opcua.error_persistent("changed: " + v); }
					return opcua.success({saw: v});
				`, addr, jsLiteral(want))
			})
			res := engine.Run(t.Context(), map[string]any{})
			if res.Outcome != Delivered {
				t.Fatalf("value did not pass through unchanged: outcome %v, err %v",
					res.Outcome, res.Err)
			}
		})
	}
}

// jsLiteral renders a Go value the way the script wrote it.
func jsLiteral(v any) string {
	switch v := v.(type) {
	case bool:
		return fmt.Sprintf("%v", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case string:
		return fmt.Sprintf("%q", v)
	}
	return "undefined"
}

// The engine offers call_method only when the binding can call (BND-7's
// optional half), and a callable address carries its object and its method in
// one object the script passes whole.
func TestRun_OffersCallMethodOnlyWhenTheBindingCanCall(t *testing.T) {
	t.Run("offered when the binding can call", func(t *testing.T) {
		b := &fakeCaller{fakeBinding: fakeBinding{}, outputs: []any{int64(7)}}
		engine := newTestEngine(t, b, func(c *Config) {
			c.Script = `
				const out = opcua.call_method({nodeId: "ns=2;s=Method", objectId: "ns=2;s=Object"}, [1, 2]);
				if (out[0] !== 7) { return opcua.error_persistent("wrong outputs: " + out); }
				return opcua.success({called: true});
			`
		})
		res := engine.Run(t.Context(), map[string]any{})
		if res.Outcome != Delivered {
			t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
		}
		if n := b.count("call ns=2;s=Method([1 2])"); n != 1 {
			t.Fatalf("method called %d times, want 1", n)
		}
	})

	t.Run("absent when the binding cannot", func(t *testing.T) {
		b := &fakeBinding{}
		engine := newTestEngine(t, b, func(c *Config) {
			c.Script = `
				opcua.call_method({nodeId: "ns=2;s=Method"}, []);
				return opcua.success({called: true});
			`
		})
		res := engine.Run(t.Context(), map[string]any{})
		// A script asking a protocol without methods is a wrong script, and a
		// wrong script is persistent.
		if res.Outcome != Persistent {
			t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
		}
		if res.Data != nil {
			t.Fatalf("a failed run carries no data, got %v", res.Data)
		}
	})
}

// The seam to pkg/js_runtime is an interface, so a fake can stand in where a
// real runtime would be slow. This is that fake, proving the injection works.
type fakeRuntime struct {
	compileErr error
	runErr     error
}

func (f fakeRuntime) Compile(name, _ string) (any, error) {
	if f.compileErr != nil {
		return nil, fmt.Errorf("%s: %w", name, f.compileErr)
	}
	return &fakeProgram{name: name}, nil
}

func (f fakeRuntime) Run(_ context.Context, _ any, _ map[string]any) (any, error) {
	if f.runErr != nil {
		return nil, f.runErr
	}
	return nil, nil
}

func (f fakeRuntime) Close() error { return nil }

type fakeProgram struct{ name string }

// A fake runtime stands in the seam: the engine drives it through the same
// interface the real package will satisfy.
func TestRuntime_FakeStandsInTheSeam(t *testing.T) {
	restore := newRuntime
	newRuntime = func() scriptRuntime {
		return fakeRuntime{runErr: fmt.Errorf("stopped: %w", context.Canceled)}
	}
	defer func() { newRuntime = restore }()

	b := &fakeBinding{values: map[string]any{}}
	engine := newTestEngine(t, b, func(c *Config) {
		c.Script = `return opcua.success({});`
	})
	res := engine.Run(t.Context(), map[string]any{})
	if res.Outcome != Persistent {
		t.Fatalf("outcome %v, err %v", res.Outcome, res.Err)
	}
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("Err does not wrap the runtime's error: %v", res.Err)
	}
}
