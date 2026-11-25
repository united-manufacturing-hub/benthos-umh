// Copyright 2026 UMH Systems GmbH
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

package opcua_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/united-manufacturing-hub/benthos-umh/handshake"
)

// fakeMachine is the fake device the BND rows run against: it records every
// call in order and answers from canned handlers.
type fakeMachine struct {
	readFn  func(ctx context.Context, req *ua.ReadRequest) (*ua.ReadResponse, error)
	writeFn func(ctx context.Context, req *ua.WriteRequest) (*ua.WriteResponse, error)
	callFn  func(ctx context.Context, req *ua.CallMethodRequest) (*ua.CallMethodResult, error)

	ctxs   []context.Context // the ctx of every call, in order
	ops    []string          // every call, in order
	writes int
}

func (f *fakeMachine) record(ctx context.Context, op string) {
	f.ctxs = append(f.ctxs, ctx)
	f.ops = append(f.ops, op)
}

func (f *fakeMachine) Read(ctx context.Context, req *ua.ReadRequest) (*ua.ReadResponse, error) {
	f.record(ctx, "read "+req.NodesToRead[0].NodeID.String())
	if f.readFn == nil {
		return nil, errors.New("fakeMachine: no readFn configured")
	}
	return f.readFn(ctx, req)
}

func (f *fakeMachine) Write(ctx context.Context, req *ua.WriteRequest) (*ua.WriteResponse, error) {
	f.record(ctx, "write "+req.NodesToWrite[0].NodeID.String())
	f.writes++
	if f.writeFn == nil {
		return nil, errors.New("fakeMachine: no writeFn configured")
	}
	return f.writeFn(ctx, req)
}

func (f *fakeMachine) Call(ctx context.Context, req *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
	f.record(ctx, "call "+req.MethodID.String()+" on "+req.ObjectID.String())
	if f.callFn == nil {
		return nil, errors.New("fakeMachine: no callFn configured")
	}
	return f.callFn(ctx, req)
}

// newTestBinding binds the engine to one fake machine.
func newTestBinding(fake *fakeMachine) *Binding {
	return &Binding{client: func() opcuaClient { return fake }}
}

// mustVariant drops the error of ua.NewVariant, which cannot fail for the
// values the tests hand it.
func mustVariant(v *ua.Variant, err error) *ua.Variant {
	if err != nil {
		panic(err)
	}
	return v
}

// readValue answers one read with a status and a variant.
func readValue(status ua.StatusCode, v *ua.Variant) func(context.Context, *ua.ReadRequest) (*ua.ReadResponse, error) {
	return func(context.Context, *ua.ReadRequest) (*ua.ReadResponse, error) {
		return &ua.ReadResponse{Results: []*ua.DataValue{{Status: status, Value: v}}}, nil
	}
}

// writeStatus answers one write with a status.
func writeStatus(code ua.StatusCode) func(context.Context, *ua.WriteRequest) (*ua.WriteResponse, error) {
	return func(context.Context, *ua.WriteRequest) (*ua.WriteResponse, error) {
		return &ua.WriteResponse{Results: []ua.StatusCode{code}}, nil
	}
}

func TestBinding_NamespaceIsTheProtocolsOwnWord(t *testing.T) {
	if got := newTestBinding(&fakeMachine{}).Namespace(); got != "opcua" {
		t.Errorf("Namespace: got %q, want %q", got, "opcua")
	}
}

func TestParseAddress(t *testing.T) {
	b := newTestBinding(&fakeMachine{})
	cases := []struct {
		name    string
		raw     map[string]any
		want    Address
		wantErr bool
	}{
		{
			name: "a nodeId alone",
			raw:  map[string]any{"nodeId": "ns=2;s=Ready"},
			want: Address{NodeID: "ns=2;s=Ready"},
		},
		{
			name: "nodeId with dataType",
			raw:  map[string]any{"nodeId": "ns=2;s=Setpoint", "dataType": "Int32"},
			want: Address{NodeID: "ns=2;s=Setpoint", DataType: "Int32"},
		},
		{
			name: "an objectId for a method call",
			raw:  map[string]any{"nodeId": "ns=2;s=Start", "objectId": "ns=2;s=Machine"},
			want: Address{NodeID: "ns=2;s=Start", ObjectID: "ns=2;s=Machine"},
		},
		{
			name:    "no nodeId",
			raw:     map[string]any{"dataType": "Int32"},
			wantErr: true,
		},
		{
			name:    "a nil address",
			raw:     nil,
			wantErr: true,
		},
		{
			name:    "a nodeId that is not a string",
			raw:     map[string]any{"nodeId": 42},
			wantErr: true,
		},
		{
			name:    "a nodeId whose namespace is out of range",
			raw:     map[string]any{"nodeId": "ns=70000;s=Ready"},
			wantErr: true,
		},
		{
			name:    "a nodeId that is not a number where one is needed",
			raw:     map[string]any{"nodeId": "i=abc"},
			wantErr: true,
		},
		{
			// gopcua's grammar: the s= prefix may be omitted for string node
			// ids in namespace 0, so a bare word is a node id.
			name: "a bare word is a ns=0 string id",
			raw:  map[string]any{"nodeId": "Ready"},
			want: Address{NodeID: "Ready"},
		},
		{
			name:    "an unknown dataType",
			raw:     map[string]any{"nodeId": "ns=2;s=Setpoint", "dataType": "Float42"},
			wantErr: true,
		},
		{
			name:    "a dataType that is not a string",
			raw:     map[string]any{"nodeId": "ns=2;s=Setpoint", "dataType": 42},
			wantErr: true,
		},
		{
			name:    "an objectId that is not a node id",
			raw:     map[string]any{"nodeId": "ns=2;s=Start", "objectId": "i=abc"},
			wantErr: true,
		},
		{
			name: "unknown keys are left alone",
			raw:  map[string]any{"nodeId": "ns=2;s=Ready", "futureOption": true},
			want: Address{NodeID: "ns=2;s=Ready"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := b.ParseAddress(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected a persistent error, got address %+v", got)
				}
				if !errors.Is(err, handshake.ErrPersistent) {
					t.Errorf("a bad address is persistent, and no retry makes a typo into one: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
	// ParseAddress performs no I/O (BND-4): nothing reached the machine.
	if len(b.client().(*fakeMachine).ops) != 0 {
		t.Errorf("ParseAddress touched the connection: %v", b.client().(*fakeMachine).ops)
	}
}

func TestBinding_Describe(t *testing.T) {
	b := newTestBinding(&fakeMachine{})
	if got := b.Describe(Address{NodeID: "ns=2;s=Ready"}); got != "ns=2;s=Ready" {
		t.Errorf("without a dataType: got %q, want %q", got, "ns=2;s=Ready")
	}
	if got := b.Describe(Address{NodeID: "ns=2;s=Setpoint", DataType: "Int32"}); got != "ns=2;s=Setpoint (Int32)" {
		t.Errorf("with a dataType: got %q", got)
	}
}

// BND-2: a status the server will give again is marked persistent; a
// transport fault is not.
func TestClassify_MarksRecurringStatusesPersistent(t *testing.T) {
	recurring := []ua.StatusCode{
		ua.StatusBadTypeMismatch,
		ua.StatusBadNodeIDUnknown,
		ua.StatusBadAttributeIDInvalid,
		ua.StatusBadIndexRangeInvalid,
		ua.StatusBadIndexRangeNoData,
		ua.StatusBadNotWritable,
		ua.StatusBadNotReadable,
		ua.StatusBadUserAccessDenied,
		ua.StatusBadServiceUnsupported,
		ua.StatusBadMethodInvalid,
		ua.StatusBadArgumentsMissing,
		ua.StatusBadInvalidArgument,
	}
	for _, code := range recurring {
		err := statusErr("write", "ns=2;s=Setpoint (Int32)", code)
		if !errors.Is(err, handshake.ErrPersistent) {
			t.Errorf("%s must be persistent: %v", code, err)
		}
	}
	transient := []ua.StatusCode{
		ua.StatusOK, // never classified, but must not be persistent if it is
		ua.StatusUncertain,
		ua.StatusBadTimeout,
		ua.StatusBadConnectionClosed,
		ua.StatusBadTooManyPublishRequests,
	}
	for _, code := range transient {
		err := statusErr("write", "ns=2;s=Setpoint (Int32)", code)
		if errors.Is(err, handshake.ErrPersistent) {
			t.Errorf("%s must not be persistent: %v", code, err)
		}
	}
}

// BND-5: only the documented Go types convert; anything else is persistent.
func TestConvert_RejectsUndocumentedTypes(t *testing.T) {
	cases := []struct {
		name     string
		dataType string
		value    any
	}{
		{"an object as Int32", "Int32", map[string]any{"value": 42}},
		{"an array as Int32", "Int32", []any{1, 2, 3}},
		{"nil as Boolean", "Boolean", nil},
		{"a struct as Double", "Double", struct{ X int }{1}},
		{"a time as String", "String", time.Now()},
		{"a bool as Int32", "Int32", true},
		{"an int64 as Boolean", "Boolean", int64(1)},
		{"an int64 as String", "String", int64(42)},
		{"a float as Boolean", "Boolean", 42.5},
		{"a string as Int32", "Int32", "42"},
		{"a string as Boolean", "Boolean", "true"},
		{"a string as DateTime", "DateTime", "not a time"},
		{"an unknown dataType", "Float42", 42.5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			variant, err := convertValue(tc.dataType, tc.value)
			if err == nil {
				t.Fatalf("expected a persistent error, got variant %v", variant)
			}
			if !errors.Is(err, handshake.ErrPersistent) {
				t.Errorf("an inconvertible value is persistent: %v", err)
			}
		})
	}
}

// The documented types convert, to the address's declared type.
func TestConvert_ConvertsTheDocumentedTypes(t *testing.T) {
	cases := []struct {
		name     string
		dataType string
		value    any
		want     any
	}{
		{"bool to Boolean", "Boolean", true, true},
		{"int64 to Int32", "Int32", int64(42), int32(42)},
		{"integral float64 to Int32", "Int32", 42.0, int32(42)},
		{"float64 to Double", "Double", 42.5, 42.5},
		{"float64 to Float", "Float", 42.5, float32(42.5)},
		{"int64 to Float", "Float", int64(42), float32(42)},
		{"int64 to SByte", "SByte", int64(-42), int8(-42)},
		{"int64 to Byte", "Byte", int64(255), uint8(255)},
		{"int64 to Int16", "Int16", int64(-32768), int16(-32768)},
		{"int64 to UInt16", "UInt16", int64(65535), uint16(65535)},
		{"int64 to UInt32", "UInt32", int64(42), uint32(42)},
		{"int64 to Int64", "Int64", int64(-42), int64(-42)},
		{"int64 to UInt64", "UInt64", int64(42), uint64(42)},
		{"string to String", "String", "hello", "hello"},
		{"string to DateTime", "DateTime", "2024-03-15T12:00:00Z", time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC)},
		{"json.Number integer to Int32", "Int32", json.Number("42"), int32(42)},
		{"json.Number float to Double", "Double", json.Number("42.5"), 42.5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			variant, err := convertValue(tc.dataType, tc.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := variant.Value()
			if fmt.Sprintf("%T(%v)", got, got) != fmt.Sprintf("%T(%v)", tc.want, tc.want) {
				t.Errorf("got %T(%v), want %T(%v)", got, got, tc.want, tc.want)
			}
		})
	}
	outOfRange := []struct {
		name     string
		dataType string
		value    any
	}{
		{"int64 to SByte out of range", "SByte", int64(128)},
		{"int64 to Byte out of range", "Byte", int64(-1)},
		{"int64 to Int16 out of range", "Int16", int64(40000)},
		{"int64 to UInt16 out of range", "UInt16", int64(-1)},
		{"int64 to Int32 out of range", "Int32", int64(5000000000)},
		{"int64 to UInt32 out of range", "UInt32", int64(-1)},
		{"float64 to Int32 fractional", "Int32", 42.5},
		{"float64 to Float out of range", "Float", 1e300},
	}
	for _, tc := range outOfRange {
		t.Run(tc.name, func(t *testing.T) {
			variant, err := convertValue(tc.dataType, tc.value)
			if err == nil {
				t.Fatalf("expected an error, got variant %v", variant)
			}
			if !errors.Is(err, handshake.ErrPersistent) {
				t.Errorf("a value that will not convert is persistent: %v", err)
			}
		})
	}
}

func TestBinding_Read(t *testing.T) {
	node := Address{NodeID: "ns=2;s=Ready"}
	ctx := context.Background()

	t.Run("returns the node's value", func(t *testing.T) {
		fake := &fakeMachine{readFn: readValue(ua.StatusOK, mustVariant(ua.NewVariant(int32(7))))}
		resp, err := newTestBinding(fake).Read(ctx, node)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Value != int64(7) {
			t.Errorf("got %#v, want int64(7)", resp.Value)
		}
		if got := fake.ops; len(got) != 1 || !strings.HasPrefix(got[0], "read ns=2;s=Ready") {
			t.Errorf("the machine recorded %v", got)
		}
		if fake.ctxs[0] != ctx {
			t.Error("the ctx must be the one the caller passed")
		}
	})

	t.Run("an Uncertain is still a value", func(t *testing.T) {
		fake := &fakeMachine{readFn: readValue(ua.StatusUncertain, mustVariant(ua.NewVariant(true)))}
		resp, err := newTestBinding(fake).Read(ctx, node)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Value != true {
			t.Errorf("got %#v, want true", resp.Value)
		}
	})

	t.Run("a Bad status is an error", func(t *testing.T) {
		for _, tc := range []struct {
			status     ua.StatusCode
			persistent bool
		}{
			{ua.StatusBadTimeout, false},
			{ua.StatusBadNodeIDUnknown, true},
		} {
			fake := &fakeMachine{readFn: readValue(tc.status, nil)}
			_, err := newTestBinding(fake).Read(ctx, node)
			if err == nil {
				t.Fatalf("%s: expected an error", tc.status)
			}
			if got := errors.Is(err, handshake.ErrPersistent); got != tc.persistent {
				t.Errorf("%s: persistent=%v, want %v: %v", tc.status, got, tc.persistent, err)
			}
		}
	})

	t.Run("a transport fault is transient", func(t *testing.T) {
		fake := &fakeMachine{readFn: func(context.Context, *ua.ReadRequest) (*ua.ReadResponse, error) {
			return nil, errors.New("connection closed")
		}}
		_, err := newTestBinding(fake).Read(ctx, node)
		if err == nil {
			t.Fatal("expected an error")
		}
		if errors.Is(err, handshake.ErrPersistent) {
			t.Errorf("a transport fault must not be persistent: %v", err)
		}
	})

	t.Run("no connection is transient", func(t *testing.T) {
		b := &Binding{client: func() opcuaClient { return nil }}
		_, err := b.Read(ctx, node)
		if err == nil {
			t.Fatal("expected an error")
		}
		if errors.Is(err, handshake.ErrPersistent) {
			t.Errorf("a missing connection must not be persistent: %v", err)
		}
	})

	t.Run("maps what the server holds into the documented Go types", func(t *testing.T) {
		cases := []struct {
			name string
			have *ua.Variant
			want any
		}{
			{"bool", mustVariant(ua.NewVariant(true)), true},
			{"int8", mustVariant(ua.NewVariant(int8(-1))), int64(-1)},
			{"uint16", mustVariant(ua.NewVariant(uint16(2))), int64(2)},
			{"float32", mustVariant(ua.NewVariant(float32(1.5))), 1.5},
			{"string", mustVariant(ua.NewVariant("x")), "x"},
			{"DateTime", mustVariant(ua.NewVariant(time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC))), "2024-03-15T12:00:00Z"},
			{"array", mustVariant(ua.NewVariant([]int32{1, 2})), []int64{1, 2}},
		}
		for _, tc := range cases {
			fake := &fakeMachine{readFn: readValue(ua.StatusOK, tc.have)}
			resp, err := newTestBinding(fake).Read(ctx, node)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if fmt.Sprintf("%#v", resp.Value) != fmt.Sprintf("%#v", tc.want) {
				t.Errorf("%s: got %#v, want %#v", tc.name, resp.Value, tc.want)
			}
		}
	})

	t.Run("a value with no documented form is persistent", func(t *testing.T) {
		fake := &fakeMachine{readFn: readValue(ua.StatusOK, mustVariant(ua.NewVariant([]byte("bytes"))))}
		_, err := newTestBinding(fake).Read(ctx, node)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !errors.Is(err, handshake.ErrPersistent) {
			t.Errorf("a byte string has no documented form, so the fault is persistent: %v", err)
		}
	})
}

func TestBinding_Write(t *testing.T) {
	node := Address{NodeID: "ns=2;s=Setpoint", DataType: "Int32"}
	ctx := context.Background()

	t.Run("returns nil only when the machine accepted it", func(t *testing.T) {
		fake := &fakeMachine{writeFn: writeStatus(ua.StatusOK)}
		if err := newTestBinding(fake).Write(ctx, node, int64(42)); err != nil {
			t.Fatal(err)
		}
		if fake.ctxs[0] != ctx {
			t.Error("the ctx must be the one the caller passed")
		}
	})

	t.Run("sends the value in the address's declared type", func(t *testing.T) {
		var got *ua.Variant
		fake := &fakeMachine{writeFn: func(_ context.Context, req *ua.WriteRequest) (*ua.WriteResponse, error) {
			got = req.NodesToWrite[0].Value.Value
			return &ua.WriteResponse{Results: []ua.StatusCode{ua.StatusOK}}, nil
		}}
		if err := newTestBinding(fake).Write(ctx, node, int64(42)); err != nil {
			t.Fatal(err)
		}
		if got == nil || got.Value() != int32(42) {
			t.Errorf("got %#v, want int32(42)", got)
		}
	})

	t.Run("a status the server gives again is persistent, a transport fault is not", func(t *testing.T) {
		for _, tc := range []struct {
			name       string
			writeFn    func(context.Context, *ua.WriteRequest) (*ua.WriteResponse, error)
			persistent bool
		}{
			{
				"BadTypeMismatch",
				writeStatus(ua.StatusBadTypeMismatch), true,
			},
			{
				"BadTimeout",
				writeStatus(ua.StatusBadTimeout), false,
			},
			{
				"no result",
				func(context.Context, *ua.WriteRequest) (*ua.WriteResponse, error) {
					return &ua.WriteResponse{}, nil
				}, false,
			},
			{
				"connection closed",
				func(context.Context, *ua.WriteRequest) (*ua.WriteResponse, error) {
					return nil, errors.New("connection closed")
				}, false,
			},
		} {
			fake := &fakeMachine{writeFn: tc.writeFn}
			err := newTestBinding(fake).Write(ctx, node, int64(42))
			if err == nil {
				t.Fatalf("%s: expected an error", tc.name)
			}
			if got := errors.Is(err, handshake.ErrPersistent); got != tc.persistent {
				t.Errorf("%s: persistent=%v, want %v: %v", tc.name, got, tc.persistent, err)
			}
			if fake.writes != 1 {
				t.Errorf("%s: the binding attempted the write %d times, want 1 (BND-6)", tc.name, fake.writes)
			}
		}
	})

	t.Run("an address without a dataType is persistent", func(t *testing.T) {
		fake := &fakeMachine{writeFn: writeStatus(ua.StatusOK)}
		err := newTestBinding(fake).Write(ctx, Address{NodeID: "ns=2;s=Setpoint"}, int64(42))
		if err == nil {
			t.Fatal("expected an error")
		}
		if !errors.Is(err, handshake.ErrPersistent) {
			t.Errorf("a write without a declared type is persistent: %v", err)
		}
		if fake.writes != 0 {
			t.Errorf("nothing may reach the machine: %d writes", fake.writes)
		}
	})

	t.Run("a value that will not convert is persistent and never sent", func(t *testing.T) {
		fake := &fakeMachine{writeFn: writeStatus(ua.StatusOK)}
		err := newTestBinding(fake).Write(ctx, node, "not a number")
		if err == nil || !errors.Is(err, handshake.ErrPersistent) {
			t.Fatalf("expected a persistent error, got %v", err)
		}
		if fake.writes != 0 {
			t.Errorf("nothing may reach the machine: %d writes", fake.writes)
		}
	})
}

func TestBinding_Call(t *testing.T) {
	method := Address{NodeID: "ns=2;s=Start", ObjectID: "ns=2;s=Machine"}
	ctx := context.Background()

	t.Run("returns the outputs in the order the machine declares them", func(t *testing.T) {
		fake := &fakeMachine{callFn: func(_ context.Context, req *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
			if req.ObjectID.String() != "ns=2;s=Machine" {
				t.Errorf("object: got %s", req.ObjectID)
			}
			if req.MethodID.String() != "ns=2;s=Start" {
				t.Errorf("method: got %s", req.MethodID)
			}
			return &ua.CallMethodResult{
				StatusCode:      ua.StatusOK,
				OutputArguments: []*ua.Variant{mustVariant(ua.NewVariant("done")), mustVariant(ua.NewVariant(int32(3)))},
			}, nil
		}}
		resp, err := newTestBinding(fake).Call(ctx, method, []any{true})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Outputs) != 2 || resp.Outputs[0] != "done" || resp.Outputs[1] != int64(3) {
			t.Errorf("got %#v, want [done 3]", resp.Outputs)
		}
		if fake.ctxs[0] != ctx {
			t.Error("the ctx must be the one the caller passed")
		}
	})

	t.Run("converts the inputs the script passed", func(t *testing.T) {
		var got []*ua.Variant
		fake := &fakeMachine{callFn: func(_ context.Context, req *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
			got = req.InputArguments
			return &ua.CallMethodResult{StatusCode: ua.StatusOK}, nil
		}}
		if _, err := newTestBinding(fake).Call(ctx, method, []any{true, int64(42), "x", 1.5}); err != nil {
			t.Fatal(err)
		}
		want := []any{true, int64(42), "x", 1.5}
		if len(got) != len(want) {
			t.Fatalf("got %d inputs, want %d", len(got), len(want))
		}
		for i := range want {
			if fmt.Sprintf("%T(%v)", got[i].Value(), got[i].Value()) != fmt.Sprintf("%T(%v)", want[i], want[i]) {
				t.Errorf("input %d: got %T(%v), want %T(%v)", i, got[i].Value(), got[i].Value(), want[i], want[i])
			}
		}
	})

	t.Run("an address without an objectId is persistent", func(t *testing.T) {
		fake := &fakeMachine{}
		_, err := newTestBinding(fake).Call(ctx, Address{NodeID: "ns=2;s=Start"}, nil)
		if err == nil || !errors.Is(err, handshake.ErrPersistent) {
			t.Fatalf("expected a persistent error, got %v", err)
		}
		if len(fake.ops) != 0 {
			t.Errorf("nothing may reach the machine: %v", fake.ops)
		}
	})

	t.Run("the method's own status is classified like a write's", func(t *testing.T) {
		for _, tc := range []struct {
			status     ua.StatusCode
			persistent bool
		}{
			{ua.StatusBadMethodInvalid, true},
			{ua.StatusBadTimeout, false},
		} {
			fake := &fakeMachine{callFn: func(context.Context, *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
				return &ua.CallMethodResult{StatusCode: tc.status}, nil
			}}
			_, err := newTestBinding(fake).Call(ctx, method, nil)
			if err == nil {
				t.Fatalf("%s: expected an error", tc.status)
			}
			if got := errors.Is(err, handshake.ErrPersistent); got != tc.persistent {
				t.Errorf("%s: persistent=%v, want %v: %v", tc.status, got, tc.persistent, err)
			}
		}
	})

	t.Run("a transport fault is transient", func(t *testing.T) {
		fake := &fakeMachine{callFn: func(context.Context, *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
			return nil, errors.New("connection closed")
		}}
		_, err := newTestBinding(fake).Call(ctx, method, nil)
		if err == nil {
			t.Fatal("expected an error")
		}
		if errors.Is(err, handshake.ErrPersistent) {
			t.Errorf("a transport fault must not be persistent: %v", err)
		}
	})

	t.Run("an input with no documented form is persistent", func(t *testing.T) {
		fake := &fakeMachine{}
		_, err := newTestBinding(fake).Call(ctx, method, []any{map[string]any{}})
		if err == nil || !errors.Is(err, handshake.ErrPersistent) {
			t.Fatalf("expected a persistent error, got %v", err)
		}
		if len(fake.ops) != 0 {
			t.Errorf("nothing may reach the machine: %v", fake.ops)
		}
	})
}
