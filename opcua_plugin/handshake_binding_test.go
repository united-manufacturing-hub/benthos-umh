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
	"math"
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

// BND-2: a status the server will give again for the same request is marked
// persistent; a fault of the moment is not. The rule applied: will the
// server answer the same way if the request is sent again unchanged? The
// table is written from the OPC UA meaning of each code, independently of
// the map it checks, so a code missing from the map is caught, and the codes
// that stay transient make that refusal visible too.
func TestClassify_MarksRecurringStatusesPersistent(t *testing.T) {
	cases := []struct {
		status     ua.StatusCode
		persistent bool
		why        string
	}{
		// The server refuses the request itself and will refuse it again.
		{ua.StatusBadTypeMismatch, true, "the value supplied is not of the attribute's type"},
		{ua.StatusBadNodeIDUnknown, true, "there is no such node in the address space"},
		{ua.StatusBadNodeIDInvalid, true, "the node id's syntax is not valid — a typo in the script"},
		{ua.StatusBadAttributeIDInvalid, true, "the node has no such attribute"},
		{ua.StatusBadIndexRangeInvalid, true, "the index range's syntax is invalid"},
		{ua.StatusBadIndexRangeNoData, true, "no data exists in the requested range"},
		{ua.StatusBadNotReadable, true, "the access level does not allow reading the node"},
		{ua.StatusBadNotWritable, true, "the access level does not allow writing the node"},
		{ua.StatusBadOutOfRange, true, "the value is out of range for the node — the server's own refusal of the request"},
		{ua.StatusBadNotSupported, true, "the requested operation is not supported"},
		{ua.StatusBadNotFound, true, "a requested item was not found"},
		{ua.StatusBadObjectDeleted, true, "the node was deleted; the request will never reach it again"},
		{ua.StatusBadWriteNotSupported, true, "the server does not support writing this combination of value, status and timestamps"},
		{ua.StatusBadUserAccessDenied, true, "the configured user may not perform the operation"},
		{ua.StatusBadServiceUnsupported, true, "the server does not offer the service"},
		{ua.StatusBadMethodInvalid, true, "the method id does not refer to a method of the object"},
		{ua.StatusBadArgumentsMissing, true, "the script did not pass all of the method's inputs"},
		{ua.StatusBadInvalidArgument, true, "one of the arguments is invalid"},
		{ua.StatusBadTooManyArguments, true, "the script passed more inputs than the method takes"},
		// Faults of the moment: a retry can fix these.
		{ua.StatusBadTimeout, false, "the operation timed out"},
		{ua.StatusBadCommunicationError, false, "a low-level communication error occurred"},
		{ua.StatusBadNoCommunication, false, "communication with the data source is defined but not established right now"},
		{ua.StatusBadNotConnected, false, "the official text reads as a permanent configuration gap, but a source that is not connected now may be connected later; left transient so a reconnected device is picked up"},
		{ua.StatusBadSessionIDInvalid, false, "the session was lost; a reconnect re-establishes it"},
		{ua.StatusBadSecureChannelClosed, false, "the secure channel closed; a reconnect re-establishes it"},
		{ua.StatusBadOutOfMemory, false, "a resource of the moment"},
		{ua.StatusBadResourceUnavailable, false, "a resource of the moment"},
		{ua.StatusBadTCPNotEnoughResources, false, "a resource of the moment"},
		{ua.StatusBadTooManyPublishRequests, false, "server overload of the moment"},
		{ua.StatusBadTooManyOperations, false, "server overload of the moment"},
		{ua.StatusBadInternalError, false, "an internal error occurred; the cause is unknown, so repeating may fix it"},
		{ua.StatusBadUnexpectedError, false, "an unexpected error; the cause is unknown"},
		{ua.StatusBadShutdown, false, "the server is shutting down; it comes back"},
		{ua.StatusBadServerHalted, false, "the server stopped; it comes back"},
		{ua.StatusBadDeviceFailure, false, "the device behind the node failed; it may be repaired"},
		{ua.StatusBadOutOfService, false, "the data source is not operational right now"},
		{ua.StatusBadNotExecutable, false, "the method's executable attribute says whether it can run *currently*; the machine may allow it later"},
		// Never classified at all: these carry a value.
		{ua.StatusOK, false, "Good carries a value"},
		{ua.StatusUncertain, false, "Uncertain carries a value — how KEPServerEX answers the first read of a healthy tag"},
	}
	seen := map[ua.StatusCode]bool{}
	for _, tc := range cases {
		if seen[tc.status] {
			t.Errorf("%s appears twice in the table", tc.status)
		}
		seen[tc.status] = true
		err := statusErr("write", "ns=2;s=Setpoint (Int32)", tc.status)
		if got := errors.Is(err, handshake.ErrPersistent); got != tc.persistent {
			t.Errorf("%s: persistent=%v, want %v (%s): %v", tc.status, got, tc.persistent, tc.why, err)
		}
	}
	// No status may enter the map without a row here: every classification
	// must carry its reason in this table.
	for code := range recurringStatuses {
		if !seen[code] {
			t.Errorf("%s is classified persistent but has no row in the table; add it with its reason", code)
		}
	}
}

// The docs page promises that read, write and call_method are one round
// trip each, and this pins it: the fake machine records every call, and a
// Browse or a Read smuggled into Call — the defect the assembled-stack
// review found in the UAT's variant of this file, which is not in this
// branch — would break it.
func TestBinding_EachOperationIsOneRoundTrip(t *testing.T) {
	ctx := context.Background()
	fake := &fakeMachine{
		readFn:  readValue(ua.StatusOK, mustVariant(ua.NewVariant(true))),
		writeFn: writeStatus(ua.StatusOK),
		callFn: func(context.Context, *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
			return &ua.CallMethodResult{StatusCode: ua.StatusOK}, nil
		},
	}
	b := newTestBinding(fake)

	if _, err := b.Read(ctx, Address{NodeID: "ns=2;s=Ready"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(ctx, Address{NodeID: "ns=2;s=Setpoint", DataType: "Int32"}, int64(42)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Call(ctx, Address{NodeID: "ns=2;s=Start", ObjectID: "ns=2;s=Machine"}, []any{true}); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"read ns=2;s=Ready",
		"write ns=2;s=Setpoint",
		"call ns=2;s=Start on ns=2;s=Machine",
	}
	if strings.Join(fake.ops, "|") != strings.Join(want, "|") {
		t.Errorf("the machine saw %v, want exactly %v", fake.ops, want)
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
		{"float64 2^53 to Int64", "Int64", 9007199254740992.0, int64(9007199254740992)},
		{"json.Number MaxInt64 to Int64", "Int64", json.Number("9223372036854775807"), int64(math.MaxInt64)},
		{"json.Number MinInt64 to Int64", "Int64", json.Number("-9223372036854775808"), int64(math.MinInt64)},
		{"float64 above MaxInt64 to UInt64", "UInt64", 1e19, uint64(10000000000000000000)},
		{"json.Number MaxUint64 to UInt64", "UInt64", json.Number("18446744073709551615"), uint64(math.MaxUint64)},
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
	// Two values that used to sit in the table above, asserting that they
	// convert. They do not any more, and that is the ruling: above 2^53 a
	// float64 cannot say which integer the message contained. Measured --
	// MinInt64 and MinInt64+1 are the same float64, as are 2^53 and 2^53+1 --
	// so converting either writes a number nobody asked for and reports
	// success. A script that needs these carries them as a string.
	inexact := []struct {
		name     string
		dataType string
		value    any
	}{
		{"float64 2^53+2 to Int64", "Int64", 9007199254740994.0},
		{"float64 MinInt64 boundary to Int64", "Int64", -9223372036854775808.0},
	}
	for _, tc := range inexact {
		t.Run(tc.name+" is refused", func(t *testing.T) {
			if _, err := convertValue(tc.dataType, tc.value); err == nil {
				t.Error("converted a value whose digits JavaScript had already lost")
			} else if !strings.Contains(err.Error(), "cannot be written exactly") {
				t.Errorf("refused for the wrong reason: %v", err)
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
		// Int64 and UInt64 refused a value the smaller types always refused:
		// an out-of-range float→int conversion is implementation-defined in
		// Go, and a host that saturates writes the clamped value to the
		// machine with a success. Measured on arm64: UInt64 1e19 wrote
		// 9223372036854775807; amd64 lands on MinInt64 instead, so the
		// plugin's behavior differed by build architecture.
		{"float64 above the range to Int64", "Int64", 1e19},
		{"float64 below the range to Int64", "Int64", -1e19},
		{"float64 at 2^63 to Int64", "Int64", 9223372036854775808.0},
		{"json.Number above the range to Int64", "Int64", json.Number("9223372036854775808")},
		{"float64 at 2^64 to UInt64", "UInt64", 18446744073709551616.0},
		{"float64 far above the range to UInt64", "UInt64", 1.8446744073709552e19},
		{"json.Number above the range to UInt64", "UInt64", json.Number("18446744073709551616")},
		{"float64 negative to UInt64", "UInt64", -42.0},
		{"json.Number negative to UInt64", "UInt64", json.Number("-42")},
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

// typedMethodFake is a machine that declares its method's inputs the way a
// real server does — a HasProperty reference named InputArguments whose value
// is the argument list — and refuses any call whose variants are not the
// declared types, the way opc-plc does (BND-7, UAT scenario 7).
type typedMethodFake struct {
	*fakeMachine
	declared []*ua.Argument
}

func (f *typedMethodFake) Browse(ctx context.Context, req *ua.BrowseRequest) (*ua.BrowseResponse, error) {
	f.record(ctx, "browse "+req.NodesToBrowse[0].NodeID.String())
	return &ua.BrowseResponse{Results: []*ua.BrowseResult{{
		References: []*ua.ReferenceDescription{{
			NodeID:     ua.NewExpandedNodeID(ua.NewStringNodeID(2, "Start.InArgs"), "", 0),
			BrowseName: &ua.QualifiedName{Name: "InputArguments"},
			NodeClass:  ua.NodeClassVariable,
		}},
	}}}, nil
}

// methodReadArguments answers the read of the InputArguments property with
// the fake's declared argument list.
func methodReadArguments(declared []*ua.Argument) func(context.Context, *ua.ReadRequest) (*ua.ReadResponse, error) {
	return func(_ context.Context, _ *ua.ReadRequest) (*ua.ReadResponse, error) {
		eos := make([]*ua.ExtensionObject, len(declared))
		for i, a := range declared {
			eos[i] = ua.NewExtensionObject(a)
		}
		return &ua.ReadResponse{Results: []*ua.DataValue{{
			Status: ua.StatusOK,
			Value:  mustVariant(ua.NewVariant(eos)),
		}}}, nil
	}
}

// strictCall answers a call by checking every input variant against the
// declared type, as a strict server does.
func strictCall(declared []*ua.Argument, output *ua.Variant) func(context.Context, *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
	return func(_ context.Context, req *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
		if len(req.InputArguments) != len(declared) {
			return &ua.CallMethodResult{StatusCode: ua.StatusBadArgumentsMissing}, nil
		}
		for i, arg := range req.InputArguments {
			if arg.Type() != ua.TypeID(declared[i].DataType.IntID()) {
				return &ua.CallMethodResult{
					StatusCode:           ua.StatusBadInvalidArgument,
					InputArgumentResults: []ua.StatusCode{ua.StatusBadTypeMismatch},
				}, nil
			}
		}
		return &ua.CallMethodResult{StatusCode: ua.StatusOK, OutputArguments: []*ua.Variant{output}}, nil
	}
}

func TestCall_ConvertsInputsToTheTypesTheMethodDeclares(t *testing.T) {
	ctx := context.Background()
	method := Address{NodeID: "ns=2;s=Start", ObjectID: "ns=2;s=Machine"}

	t.Run("a method that declares Float and UInt32 accepts a float64 and an int64 from the script", func(t *testing.T) {
		fake := &typedMethodFake{fakeMachine: &fakeMachine{
			readFn: methodReadArguments([]*ua.Argument{
				{Name: "Float value", DataType: ua.NewNumericNodeID(0, 10), ValueRank: -1},
				{Name: "UInt32 value", DataType: ua.NewNumericNodeID(0, 7), ValueRank: -1},
			}),
			callFn: strictCall([]*ua.Argument{
				{Name: "Float value", DataType: ua.NewNumericNodeID(0, 10), ValueRank: -1},
				{Name: "UInt32 value", DataType: ua.NewNumericNodeID(0, 7), ValueRank: -1},
			}, mustVariant(ua.NewVariant(float32(5.5)))),
		}, declared: []*ua.Argument{
			{Name: "Float value", DataType: ua.NewNumericNodeID(0, 10), ValueRank: -1},
			{Name: "UInt32 value", DataType: ua.NewNumericNodeID(0, 7), ValueRank: -1},
		}}
		resp, err := (&Binding{client: func() opcuaClient { return fake }}).Call(ctx, method, []any{2.5, int64(3)})
		if err != nil {
			t.Fatalf("the call must invoke the method with its declared types: %v", err)
		}
		if len(resp.Outputs) != 1 || resp.Outputs[0] != float64(5.5) {
			t.Errorf("outputs: got %#v", resp.Outputs)
		}
	})

	t.Run("a value that will not convert to the declared type is persistent and never sent", func(t *testing.T) {
		declared := []*ua.Argument{{Name: "Float value", DataType: ua.NewNumericNodeID(0, 10), ValueRank: -1}}
		fake := &typedMethodFake{fakeMachine: &fakeMachine{
			readFn: methodReadArguments(declared),
			callFn: strictCall(declared, nil),
		}}
		_, err := (&Binding{client: func() opcuaClient { return fake }}).Call(ctx, method, []any{"not a number"})
		if err == nil || !errors.Is(err, handshake.ErrPersistent) {
			t.Fatalf("expected a persistent error, got %v", err)
		}
		for _, op := range fake.ops {
			if strings.HasPrefix(op, "call ") {
				t.Errorf("nothing may reach the machine: %v", fake.ops)
			}
		}
	})

	t.Run("a client that cannot browse keeps the Go-type mapping", func(t *testing.T) {
		var got []*ua.Variant
		fake := &fakeMachine{callFn: func(_ context.Context, req *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
			got = req.InputArguments
			return &ua.CallMethodResult{StatusCode: ua.StatusOK}, nil
		}}
		if _, err := newTestBinding(fake).Call(ctx, method, []any{2.5}); err != nil {
			t.Fatal(err)
		}
		if got[0].Type() != ua.TypeIDDouble || got[0].Value() != 2.5 {
			t.Errorf("input 0: got %T(%v), want a float64 double", got[0].Value(), got[0].Value())
		}
	})
}

// TestCall_DeclaredArrayInputKeepsTheGoTypeMapping pins the ValueRank guard
// in declaredInputTypes: a method input the method declares as an ARRAY
// (ValueRank >= 0) has no single value form, so the binding must not convert
// the script's value to the declared SCALAR type — the Go-type mapping
// (variantForValue) takes it, exactly as it did before the declared-type
// conversion existed (BND-7's promise: "a declared array input keeps the
// Go-type mapping, so a lenient server sees exactly what it saw before").
//
// Verified red by deleting the `if arg.ValueRank >= 0 { continue }` guard:
// the declared scalar type ("Int32" for an Int32[] argument) then reaches
// convertValue, which (a) converts an int64 input to an Int32 variant — the
// server sees a value shape it never saw before — and (b) refuses a float64
// input ("value 2.5 is not an integer") as a persistent fault, so a call
// that works today dead-letters without anything reaching the machine.
//
// Why the assertions detect the hazard rather than something near it: each
// assertion is on the variant the fake server receives (its OPC UA type and
// value), or on the call never having been sent. Applying the declared
// scalar type to an array-declared input is the only code path that can
// change that variant's type or turn the call into a persistent refusal, so
// both subtests fail exactly when the guard is gone and pass only when the
// Go-type mapping took the input. (Stated honestly: an actual array value
// — a script passing [1,2,3] — is refused by the Go-type mapping today as
// well; what the guard protects is that the array-declared input keeps that
// mapping's behavior, including the calls it accepts.)
//
// The existing typedMethodFake already browses and answers the
// InputArguments property, so no fake was extended and nothing was
// constructed directly: the test drives Binding.Call end to end, the way a
// script's opcua.call_method reaches the machine, because the protected
// behavior is what the machine receives.
func TestCall_DeclaredArrayInputKeepsTheGoTypeMapping(t *testing.T) {
	ctx := context.Background()
	method := Address{NodeID: "ns=2;s=Start", ObjectID: "ns=2;s=Machine"}

	// One argument declared as an array of Int32 (ValueRank >= 0 is OPC UA's
	// "an array"): DataType names the scalar element type, Int32.
	arrayArg := &ua.Argument{Name: "values", DataType: ua.NewNumericNodeID(0, 6), ValueRank: 1}

	call := func(t *testing.T, input any) ([]*ua.Variant, error) {
		t.Helper()
		var received []*ua.Variant
		fake := &typedMethodFake{fakeMachine: &fakeMachine{
			readFn: methodReadArguments([]*ua.Argument{arrayArg}),
			callFn: func(_ context.Context, req *ua.CallMethodRequest) (*ua.CallMethodResult, error) {
				received = req.InputArguments
				return &ua.CallMethodResult{StatusCode: ua.StatusOK}, nil
			},
		}, declared: []*ua.Argument{arrayArg}}
		_, err := (&Binding{client: func() opcuaClient { return fake }}).Call(ctx, method, []any{input})
		return received, err
	}

	t.Run("an int64 input is sent as the Go type, not the declared scalar type", func(t *testing.T) {
		received, err := call(t, int64(5))
		if err != nil {
			t.Fatalf("a scalar to an array-declared input must keep the Go-type mapping: %v", err)
		}
		if len(received) != 1 {
			t.Fatalf("the machine received %d variants, want 1", len(received))
		}
		// variantForValue sends an int64 as an Int64 variant. The deleted
		// guard would make convertValue("Int32", 5) send an Int32 variant —
		// a different value on the wire than the mapping promises.
		if received[0].Type() != ua.TypeIDInt64 || received[0].Value() != int64(5) {
			t.Errorf("input 0: got type %v value %v, want the Go-type mapping's Int64 5 (an array-declared input must not be converted to the declared scalar type Int32)",
				received[0].Type(), received[0].Value())
		}
	})

	t.Run("a float64 input that works today does not dead-letter", func(t *testing.T) {
		// A script's non-integral number (goja exports 2.5 as float64) is
		// convertible by the Go-type mapping today; convertValue("Int32", 2.5)
		// refuses it as a persistent fault, so a working call dead-letters
		// with the guard deleted.
		received, err := call(t, 2.5)
		if err != nil {
			t.Fatalf("a float64 to an array-declared input must keep working: %v", err)
		}
		if len(received) != 1 {
			t.Fatalf("the machine received %d variants, want 1", len(received))
		}
		if received[0].Type() != ua.TypeIDDouble || received[0].Value() != 2.5 {
			t.Errorf("input 0: got type %v value %v, want the Go-type mapping's Double 2.5",
				received[0].Type(), received[0].Value())
		}
	})
}
