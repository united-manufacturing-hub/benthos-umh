package handshake

import (
	"context"
	"fmt"
	"sync"
)

// fakeBinding is one machine for tests: it records every operation in order
// and answers reads from a table. Its address is the nodeId a script passes.
// It is the protocol the engine is tested against, so it is deliberately
// ordinary: no retries, no batching, nothing the seam does not describe.
type fakeBinding struct {
	mu     sync.Mutex
	ops    []string       // every operation, in the order the machine saw them
	values map[string]any // what Read answers, by nodeId
	errs   map[string]error
}

func (f *fakeBinding) Namespace() string { return "opcua" }

func (f *fakeBinding) ParseAddress(raw map[string]any) (string, error) {
	id, _ := raw["nodeId"].(string)
	if id == "" {
		// No retry makes a typo into an address.
		return "", fmt.Errorf("address has no nodeId: %w", ErrPersistent)
	}
	return id, nil
}

func (f *fakeBinding) log(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, op)
}

// count reports how many recorded operations start with op.
func (f *fakeBinding) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, recorded := range f.ops {
		if recorded == op {
			n++
		}
	}
	return n
}

func (f *fakeBinding) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ops...)
}

// errFor returns the injected error for one operation on one address, if any.
func (f *fakeBinding) errFor(op, addr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errs == nil {
		return nil
	}
	return f.errs[op+":"+addr]
}

func (f *fakeBinding) Read(_ context.Context, addr string) (ReadResponse, error) {
	f.log("read " + addr)
	if err := f.errFor("read", addr); err != nil {
		return ReadResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	value, ok := f.values[addr]
	if !ok {
		// Loud on purpose: a test that reads an unconfigured address is a
		// broken test, not a machine answering.
		return ReadResponse{}, fmt.Errorf("no value configured for %s: %w", addr, ErrPersistent)
	}
	return ReadResponse{Value: value}, nil
}

func (f *fakeBinding) Write(_ context.Context, addr string, value any) error {
	f.log(fmt.Sprintf("write %s=%v", addr, value))
	return f.errFor("write", addr)
}

func (f *fakeBinding) Describe(addr string) string { return addr }

// fakeCaller is a fakeBinding whose protocol has callable addresses, so the
// engine offers call_method to it (the optional half of the seam).
type fakeCaller struct {
	fakeBinding
	callErr  error
	outputs  []any
	lastCall []any
}

func (f *fakeCaller) Call(_ context.Context, addr string, inputs []any) (CallResponse, error) {
	f.log(fmt.Sprintf("call %s(%v)", addr, inputs))
	f.mu.Lock()
	f.lastCall = append([]any(nil), inputs...)
	f.mu.Unlock()
	if f.callErr != nil {
		return CallResponse{}, f.callErr
	}
	return CallResponse{Outputs: f.outputs}, nil
}
