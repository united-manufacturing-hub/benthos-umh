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

// This file is the whole of the OPC UA binding to the handshake engine: six
// methods, and the decisions that are OPC UA's rather than the engine's.
package opcua_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/gopcua/opcua/ua"

	"github.com/united-manufacturing-hub/benthos-umh/handshake"
)

// An Address is OPC UA's form of a unified address: what a script passes to
// read, write and call_method, for example { nodeId: "ns=2;s=Ready",
// dataType: "Boolean" }. A script writes OPC UA's own words, because a script
// is written against one machine; the engine above never sees them. [NodeID]
// is the grammar.
//
// For a method call the address carries both halves of the pair the Call
// service needs: ObjectID is the node the method lives on, and NodeID is the
// method itself.
//
// [NodeID]: https://reference.opcfoundation.org/Core/Part3/v105/docs/8.2
type Address struct {
	NodeID   string
	DataType string
	ObjectID string
}

// A Binding is one OPC UA connection, used by one handshake engine. It is not
// safe for concurrent use, which is what the engine promises it.
type Binding struct {
	// client returns the OPC UA connection the binding runs on, read per
	// operation so a reconnect between two scripts is picked up. It returns
	// nil when the plugin is not connected. The indirection exists so tests
	// can hand the binding a machine that records every call in order.
	client func() opcuaClient
	_      [0]func()
}

// opcuaClient is the slice of *opcua.Client the binding speaks through.
type opcuaClient interface {
	Read(ctx context.Context, req *ua.ReadRequest) (*ua.ReadResponse, error)
	Write(ctx context.Context, req *ua.WriteRequest) (*ua.WriteResponse, error)
	Call(ctx context.Context, req *ua.CallMethodRequest) (*ua.CallMethodResult, error)
}

// A browser is a client that can browse the address space. *opcua.Client is
// one; the binding asks for the interface rather than assuming it, so a
// client that cannot browse keeps the Go-type mapping of method inputs.
type browser interface {
	Browse(ctx context.Context, req *ua.BrowseRequest) (*ua.BrowseResponse, error)
}

// declaredInputTypes reads the types a method declares for its inputs, in
// declaration order, from the InputArguments property every OPC UA method
// carries. An entry the binding cannot convert — a non-built-in type, or an
// array — comes back as "", and Call falls back to the Go-type mapping for
// that input. A method whose property cannot be read at all reports ok=false.
func (b *Binding) declaredInputTypes(ctx context.Context, c opcuaClient, methodID *ua.NodeID) ([]string, bool) {
	bc, ok := c.(browser)
	if !ok {
		return nil, false
	}
	resp, err := bc.Browse(ctx, &ua.BrowseRequest{
		NodesToBrowse: []*ua.BrowseDescription{{
			NodeID:          methodID,
			BrowseDirection: ua.BrowseDirectionForward,
			ReferenceTypeID: ua.NewNumericNodeID(0, 46), // HasProperty
			IncludeSubtypes: true,
			ResultMask:      uint32(ua.BrowseResultMaskAll),
		}},
	})
	if err != nil || len(resp.Results) == 0 {
		return nil, false
	}
	var propertyID *ua.NodeID
	for _, ref := range resp.Results[0].References {
		if ref.BrowseName != nil && ref.BrowseName.Name == "InputArguments" {
			propertyID = ref.NodeID.NodeID
			break
		}
	}
	if propertyID == nil {
		return nil, false
	}
	readResp, err := c.Read(ctx, &ua.ReadRequest{NodesToRead: []*ua.ReadValueID{
		{NodeID: propertyID, AttributeID: ua.AttributeIDValue},
	}})
	if err != nil || len(readResp.Results) == 0 || readResp.Results[0].Value == nil {
		return nil, false
	}
	eos, ok := readResp.Results[0].Value.Value().([]*ua.ExtensionObject)
	if !ok {
		return nil, false
	}
	types := make([]string, len(eos))
	for i, eo := range eos {
		arg, ok := eo.Value.(*ua.Argument)
		if !ok || arg.DataType == nil {
			return nil, false
		}
		// An array input has no single value form, so the declared type
		// cannot guide the conversion; the Go-type mapping takes it.
		if arg.ValueRank >= 0 {
			continue
		}
		types[i] = builtinTypeName(arg.DataType.IntID())
	}
	return types, true
}

// builtinTypeName maps a built-in data type id to the name convertValue
// converts, and "" to what the binding cannot use.
func builtinTypeName(id uint32) string {
	switch id {
	case 1:
		return "Boolean"
	case 2:
		return "SByte"
	case 3:
		return "Byte"
	case 4:
		return "Int16"
	case 5:
		return "UInt16"
	case 6:
		return "Int32"
	case 7:
		return "UInt32"
	case 8:
		return "Int64"
	case 9:
		return "UInt64"
	case 10:
		return "Float"
	case 11:
		return "Double"
	case 12:
		return "String"
	case 13:
		return "DateTime"
	default:
		return ""
	}
}

// NewBinding binds the handshake engine to one OPC UA connection. The
// connection is read per operation, so the binding follows a reconnect and
// stays valid for the output's lifetime.
func NewBinding(conn *OPCUAConnection) *Binding {
	return &Binding{client: func() opcuaClient {
		if conn == nil {
			return nil
		}
		return conn.Client
	}}
}

// The seam, checked at compile time rather than at the first write to a
// press. OPC UA has methods, so this binding also satisfies the optional half.
var (
	_ handshake.Binding[Address] = (*Binding)(nil)
	_ handshake.Caller[Address]  = (*Binding)(nil)
)

// Namespace is the word a script calls this protocol by: opcua.read,
// opcua.write.
func (b *Binding) Namespace() string { return "opcua" }

// supportedDataTypes are the OPC UA types a script may declare on an address,
// the same set the nodeMappings of the deprecated path accept.
var supportedDataTypes = map[string]bool{
	"Boolean": true, "SByte": true, "Byte": true, "Int16": true, "UInt16": true,
	"Int32": true, "UInt32": true, "Int64": true, "UInt64": true, "Float": true,
	"Double": true, "String": true, "DateTime": true,
}

// ParseAddress reads the object a script passed. What does not parse is a
// persistent fault: no retry makes a typo into an address. It performs no
// I/O (BND-4).
func (b *Binding) ParseAddress(raw map[string]any) (Address, error) {
	if raw == nil {
		return Address{}, persistentErr("an opcua address is an object, for example { nodeId: \"ns=2;s=Ready\", dataType: \"Boolean\" }")
	}
	nodeID, err := addressString(raw, "nodeId")
	if err != nil {
		return Address{}, err
	}
	if nodeID == "" {
		return Address{}, persistentErr("an opcua address needs a nodeId, for example { nodeId: \"ns=2;s=Ready\" }")
	}
	if _, parseErr := ua.ParseNodeID(nodeID); parseErr != nil {
		return Address{}, persistentErr("nodeId %q is not a node id: %v", nodeID, parseErr)
	}
	dataType, err := addressString(raw, "dataType")
	if err != nil {
		return Address{}, err
	}
	if dataType != "" && !supportedDataTypes[dataType] {
		return Address{}, persistentErr("unsupported dataType %q: supported types are Boolean, SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Float, Double, String, DateTime", dataType)
	}
	objectID, err := addressString(raw, "objectId")
	if err != nil {
		return Address{}, err
	}
	if objectID != "" {
		if _, err := ua.ParseNodeID(objectID); err != nil {
			return Address{}, persistentErr("objectId %q is not a node id: %v", objectID, err)
		}
	}
	return Address{NodeID: nodeID, DataType: dataType, ObjectID: objectID}, nil
}

// addressString reads one string field of an address object, refusing the
// fields that are present but not strings.
func addressString(raw map[string]any, key string) (string, error) {
	v, exists := raw[key]
	if !exists || v == nil {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", persistentErr("the address field %q must be a string, got %T", key, v)
	}
	return s, nil
}

// Read returns the node's value. A Bad status is returned as an error; Good
// and Uncertain both return the value, because an Uncertain is how
// KEPServerEX answers the first read of a healthy tag.
func (b *Binding) Read(ctx context.Context, t Address) (handshake.ReadResponse, error) {
	c, err := b.dial()
	if err != nil {
		return handshake.ReadResponse{}, err
	}
	nodeID, err := ua.ParseNodeID(t.NodeID)
	if err != nil {
		return handshake.ReadResponse{}, persistentErr("read %s: nodeId %q is not a node id: %v", b.Describe(t), t.NodeID, err)
	}
	req := &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{
			{NodeID: nodeID, AttributeID: ua.AttributeIDValue},
		},
	}
	resp, err := c.Read(ctx, req)
	if err != nil {
		return handshake.ReadResponse{}, fmt.Errorf("read %s: %w", b.Describe(t), err)
	}
	if len(resp.Results) == 0 {
		return handshake.ReadResponse{}, fmt.Errorf("read %s: the server returned no result", b.Describe(t))
	}
	dv := resp.Results[0]
	if isBadStatus(dv.Status) {
		return handshake.ReadResponse{}, statusErr("read", b.Describe(t), dv.Status)
	}
	value, err := variantToValue(dv.Value)
	if err != nil {
		return handshake.ReadResponse{}, persistentErr("read %s: %v", b.Describe(t), err)
	}
	return handshake.ReadResponse{Value: value}, nil
}

// Write sends one value, converted to the address's declared type. A value
// that will not convert, or a status the server gives again for the same
// request, such as BadTypeMismatch, is marked [handshake.ErrPersistent];
// transport faults are not.
func (b *Binding) Write(ctx context.Context, t Address, value any) error {
	c, err := b.dial()
	if err != nil {
		return err
	}
	if t.DataType == "" {
		return persistentErr("write %s: the address declares no dataType, so the value cannot be converted", b.Describe(t))
	}
	nodeID, err := ua.ParseNodeID(t.NodeID)
	if err != nil {
		return persistentErr("write %s: nodeId %q is not a node id: %v", b.Describe(t), t.NodeID, err)
	}
	variant, err := convertValue(t.DataType, value)
	if err != nil {
		return persistentErr("write %s: %v", b.Describe(t), err)
	}
	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{
			{
				NodeID:      nodeID,
				AttributeID: ua.AttributeIDValue,
				Value: &ua.DataValue{
					Value:        variant,
					EncodingMask: ua.DataValueValue,
				},
			},
		},
	}
	resp, err := c.Write(ctx, req)
	if err != nil {
		return fmt.Errorf("write %s: %w", b.Describe(t), err)
	}
	if len(resp.Results) == 0 {
		return fmt.Errorf("write %s: the server returned no result", b.Describe(t))
	}
	if code := resp.Results[0]; code != ua.StatusOK {
		return statusErr("write", b.Describe(t), code)
	}
	return nil
}

// Call invokes an OPC UA method, the method being t.NodeID on t.ObjectID, and
// returns its output arguments in the order the server declares them. The
// same split of persistent and transient applies, with the method's own
// status taking the place of the write's.
func (b *Binding) Call(ctx context.Context, t Address, inputs []any) (handshake.CallResponse, error) {
	c, err := b.dial()
	if err != nil {
		return handshake.CallResponse{}, err
	}
	if t.ObjectID == "" {
		return handshake.CallResponse{}, persistentErr(
			"call %s: the address declares no objectId, the node the method lives on; a call needs both, for example { nodeId: \"ns=2;s=Start\", objectId: \"ns=2;s=Machine\" }", b.Describe(t))
	}
	if t.NodeID == "" {
		return handshake.CallResponse{}, persistentErr("call: the address declares no nodeId, the method to invoke")
	}
	objectID, err := ua.ParseNodeID(t.ObjectID)
	if err != nil {
		return handshake.CallResponse{}, persistentErr("call %s: objectId %q is not a node id: %v", b.Describe(t), t.ObjectID, err)
	}
	methodID, err := ua.ParseNodeID(t.NodeID)
	if err != nil {
		return handshake.CallResponse{}, persistentErr("call %s: nodeId %q is not a node id: %v", b.Describe(t), t.NodeID, err)
	}
	// BND-7: a method declares its own argument types, and a strict server —
	// opc-plc among them — refuses a call whose variants are not those exact
	// types, so the binding converts each input to the declared type before
	// it sends. A client that cannot introspect, or a method that does not
	// declare its inputs, keeps the Go-type mapping below, which a lenient
	// server accepts.
	types, ok := b.declaredInputTypes(ctx, c, methodID)
	args := make([]*ua.Variant, len(inputs))
	for i, input := range inputs {
		if ok && i < len(types) && types[i] != "" {
			variant, convErr := convertValue(types[i], input)
			if convErr != nil {
				return handshake.CallResponse{}, persistentErr("call %s: input %d: %v", b.Describe(t), i, convErr)
			}
			args[i] = variant
			continue
		}
		variant, convErr := variantForValue(input)
		if convErr != nil {
			return handshake.CallResponse{}, persistentErr("call %s: input %d: %v", b.Describe(t), i, convErr)
		}
		args[i] = variant
	}
	result, err := c.Call(ctx, &ua.CallMethodRequest{
		ObjectID:       objectID,
		MethodID:       methodID,
		InputArguments: args,
	})
	if err != nil {
		return handshake.CallResponse{}, fmt.Errorf("call %s: %w", b.Describe(t), err)
	}
	if result.StatusCode != ua.StatusOK {
		return handshake.CallResponse{}, statusErr("call", b.Describe(t), result.StatusCode)
	}
	outputs := make([]any, len(result.OutputArguments))
	for i, variant := range result.OutputArguments {
		value, err := variantToValue(variant)
		if err != nil {
			return handshake.CallResponse{}, persistentErr("call %s: output %d: %v", b.Describe(t), i, err)
		}
		outputs[i] = value
	}
	return handshake.CallResponse{Outputs: outputs}, nil
}

// Describe renders an address the way an OPC UA engineer writes one, so an
// error names something they can paste into a client.
func (b *Binding) Describe(t Address) string {
	if t.DataType == "" {
		return t.NodeID
	}
	return fmt.Sprintf("%s (%s)", t.NodeID, t.DataType)
}

// dial returns the connection the next operation runs on. A missing
// connection is transient: the plugin reconnects, and the flow may ask again.
func (b *Binding) dial() (opcuaClient, error) {
	if b.client == nil {
		return nil, errors.New("opcua: the binding has no connection")
	}
	c := b.client()
	if c == nil {
		return nil, errors.New("opcua: not connected")
	}
	return c, nil
}

// statusSeverityMask reads the two bits that say how a status went: 00 Good,
// 01 Uncertain, 10 Bad.
const statusSeverityMask = ua.StatusCode(0xC0000000)

// isBadStatus reports whether the server answered Bad, as opposed to Good or
// Uncertain; both of those carry a value.
func isBadStatus(code ua.StatusCode) bool {
	return code&statusSeverityMask == 0x80000000
}

// recurringStatuses are the ones the server will give again for the same
// request, so repeating cannot fix them (BND-2). The rule applied: would
// the server answer the same way if this request were sent again unchanged?
// Transport faults, timeouts, session losses and overload are absent on
// purpose — those are the ones a retry can fix. Every entry carries its
// reason in TestClassify_MarksRecurringStatusesPersistent, which refuses a
// map entry without a row there.
var recurringStatuses = map[ua.StatusCode]bool{
	ua.StatusBadTypeMismatch:       true,
	ua.StatusBadNodeIDUnknown:      true,
	ua.StatusBadNodeIDInvalid:      true,
	ua.StatusBadAttributeIDInvalid: true,
	ua.StatusBadIndexRangeInvalid:  true,
	ua.StatusBadIndexRangeNoData:   true,
	ua.StatusBadNotWritable:        true,
	ua.StatusBadNotReadable:        true,
	ua.StatusBadOutOfRange:         true,
	ua.StatusBadNotSupported:       true,
	ua.StatusBadNotFound:           true,
	ua.StatusBadObjectDeleted:      true,
	ua.StatusBadWriteNotSupported:  true,
	ua.StatusBadUserAccessDenied:   true,
	ua.StatusBadServiceUnsupported: true,
	ua.StatusBadMethodInvalid:      true,
	ua.StatusBadArgumentsMissing:   true,
	ua.StatusBadInvalidArgument:    true,
	ua.StatusBadTooManyArguments:   true,
}

// statusErr renders a status the server sent, marking the ones it will give
// again persistent.
func statusErr(op string, describe string, code ua.StatusCode) error {
	err := fmt.Errorf("%s %s: %w", op, describe, code)
	if recurringStatuses[code] {
		return fmt.Errorf("%w: %w", handshake.ErrPersistent, err)
	}
	return err
}

// persistentErr builds a fault that repeating cannot fix.
func persistentErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", handshake.ErrPersistent, fmt.Sprintf(format, args...))
}

// convertValue converts one value an engine handed over into the declared
// OPC UA type. Only the documented Go types convert (BND-5): bool, int64,
// float64, string, and json.Number, which is how the message's JSON numbers
// reach the script. Anything else is persistent, and nothing is sent.
func convertValue(dataType string, value any) (*ua.Variant, error) {
	switch dataType {
	case "Boolean":
		v, ok := value.(bool)
		if !ok {
			return nil, typeErr(value, dataType)
		}
		return ua.NewVariant(v)

	case "SByte":
		v, err := intValue(value, -128, 127)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(int8(v))

	case "Byte":
		v, err := intValue(value, 0, 255)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(uint8(v))

	case "Int16":
		v, err := intValue(value, -32768, 32767)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(int16(v))

	case "UInt16":
		v, err := intValue(value, 0, 65535)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(uint16(v))

	case "Int32":
		v, err := intValue(value, math.MinInt32, math.MaxInt32)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(int32(v))

	case "UInt32":
		v, err := intValue(value, 0, math.MaxUint32)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(uint32(v))

	case "Int64":
		v, err := int64Value(value)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(v)

	case "UInt64":
		v, err := uint64Value(value)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(v)

	case "Float":
		v, err := float64Value(value)
		if err != nil {
			return nil, err
		}
		if v > math.MaxFloat32 || v < -math.MaxFloat32 {
			return nil, persistentErr("value %v is out of range for Float", value)
		}
		return ua.NewVariant(float32(v))

	case "Double":
		v, err := float64Value(value)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(v)

	case "String":
		v, ok := value.(string)
		if !ok {
			return nil, typeErr(value, dataType)
		}
		return ua.NewVariant(v)

	case "DateTime":
		s, ok := value.(string)
		if !ok {
			return nil, typeErr(value, dataType)
		}
		v, err := parseDateTime(s)
		if err != nil {
			return nil, err
		}
		return ua.NewVariant(v)

	default:
		return nil, persistentErr("unsupported dataType %q: supported types are Boolean, SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Float, Double, String, DateTime", dataType)
	}
}

// typeErr rejects a value whose Go type the binding does not document.
func typeErr(value any, dataType string) error {
	return persistentErr("value %v of type %T cannot be converted to %s: only bool, int64, float64 and string convert", value, value, dataType)
}

// int64Value reads a value as an integer, refusing what is not one.
// maxInt64Float is 2^63 as a float64: the smallest float no int64 can hold.
// A value at or above it, or below -2^63, is refused before the conversion,
// because Go leaves an out-of-range float→int conversion implementation-
// defined: a host that saturates would otherwise write the clamped value to
// the machine and report success, and hosts differ (arm64 saturates to
// MaxInt64, amd64 to MinInt64), so the plugin would behave differently per
// build architecture. An integral float inside the range converts exactly —
// a float64 is a precise number, and if it is whole and in range it is the
// value; the 2^53 fidelity limit belongs to whatever decoded the message,
// which is why json.Number's digits are preferred wherever the value
// arrives that way.
const maxInt64Float = 9223372036854775808.0 // 2^63

// maxExactFloat is 2^53, the largest integer a float64 represents exactly.
// Above it the gaps between representable values exceed 1, so a different
// integer maps to the same float and the original digits are unrecoverable.
const maxExactFloat = float64(1 << 53)

func int64Value(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case float64:
		if v != math.Trunc(v) {
			return 0, persistentErr("value %v is not an integer", value)
		}
		// Above 2^53 a float64 cannot say which integer the message
		// contained. Measured: MinInt64 and MinInt64+1 are the same float64,
		// as are 2^53 and 2^53+1 -- so converting either writes a number
		// nobody asked for and reports success, and the read-back cannot
		// catch it because it compares the corrupted value against itself.
		// A script that touched the value has already lost the digits inside
		// JavaScript, and nothing here gets them back.
		//
		// So refuse, which is this binding's rule everywhere else: a value
		// that does not convert is persistent and nothing is sent, never
		// rounded or clamped. A plant engineer can act on a refusal naming
		// the value; nobody can act on a machine quietly holding a different
		// number (Jeremy, 2026-11-26).
		if v > maxExactFloat || v < -maxExactFloat {
			return 0, persistentErr(
				"value %.0f cannot be written exactly: a number this large loses digits in "+
					"JavaScript, so carry it through the message as a string instead", v)
		}
		if v >= maxInt64Float || v < -maxInt64Float {
			return 0, persistentErr("value %v is out of range", value)
		}
		return int64(v), nil
	case json.Number:
		// The digits are exact; prefer them over the float.
		if i, err := v.Int64(); err == nil {
			return i, nil
		}
		f, err := v.Float64()
		if err != nil {
			return 0, persistentErr("value %v cannot be converted to a number", value)
		}
		if f != math.Trunc(f) {
			return 0, persistentErr("value %v is not an integer", value)
		}
		if f >= maxInt64Float || f < -maxInt64Float {
			return 0, persistentErr("value %v is out of range", value)
		}
		return int64(f), nil
	default:
		return 0, typeErr(value, "an integer")
	}
}

// maxUint64Float is 2^64 as a float64: the smallest float no uint64 can
// hold. UInt64 is checked against the uint64 range, not the int64 one, so a
// value between MaxInt64 and MaxUint64 — legal for a UInt64 tag — still
// converts instead of being refused as out of int64 range.
const maxUint64Float = 18446744073709551616.0 // 2^64

// uint64Value reads a value as an unsigned 64-bit integer.
func uint64Value(value any) (uint64, error) {
	switch v := value.(type) {
	case int64:
		if v < 0 {
			return 0, persistentErr("value %v is out of range", value)
		}
		return uint64(v), nil
	case float64:
		if v != math.Trunc(v) {
			return 0, persistentErr("value %v is not an integer", value)
		}
		if v < 0 || v >= maxUint64Float {
			return 0, persistentErr("value %v is out of range", value)
		}
		return uint64(v), nil
	case json.Number:
		// The digits carry the exact value for the whole uint64 range.
		if u, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
			return u, nil
		}
		f, err := v.Float64()
		if err != nil {
			return 0, persistentErr("value %v cannot be converted to a number", value)
		}
		if f != math.Trunc(f) {
			return 0, persistentErr("value %v is not an integer", value)
		}
		if f < 0 || f >= maxUint64Float {
			return 0, persistentErr("value %v is out of range", value)
		}
		return uint64(f), nil
	default:
		return 0, typeErr(value, "an unsigned integer")
	}
}

// intValue reads a value as an integer inside the declared type's range.
func intValue(value any, lo int64, hi int64) (int64, error) {
	v, err := int64Value(value)
	if err != nil {
		return 0, err
	}
	if v < lo || v > hi {
		return 0, persistentErr("value %v is out of range", value)
	}
	return v, nil
}

// float64Value reads a value as a float.
func float64Value(value any) (float64, error) {
	switch v := value.(type) {
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, persistentErr("value %v cannot be converted to a number", value)
		}
		return f, nil
	default:
		return 0, typeErr(value, "a number")
	}
}

// dateTimeFormats are the formats a DateTime may arrive in, the same ones the
// deprecated path accepts.
var dateTimeFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// parseDateTime parses a DateTime the script passed as a string.
func parseDateTime(s string) (time.Time, error) {
	for _, format := range dateTimeFormats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, persistentErr("value %q cannot be converted to DateTime: not a recognized time format", s)
}

// variantForValue converts one method input, whose type the address does not
// declare, to the closest variant the Go type names: bool, int64, float64
// and string. A json.Number keeps its integer shape when it has one, so an
// integer reaches the method as an integer, never as a double. A method whose
// declared input type is narrower than the Go type needs the conversion to
// the declared type in Call; this fallback serves the lenient server.
func variantForValue(value any) (*ua.Variant, error) {
	switch v := value.(type) {
	case bool:
		return ua.NewVariant(v)
	case int64:
		return ua.NewVariant(v)
	case float64:
		return ua.NewVariant(v)
	case string:
		return ua.NewVariant(v)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return ua.NewVariant(i)
		}
		if f, err := v.Float64(); err == nil {
			return ua.NewVariant(f)
		}
		return nil, persistentErr("value %v cannot be converted to a number", value)
	default:
		return nil, persistentErr("value %v of type %T cannot be passed to a method: only bool, int64, float64 and string convert", value, value)
	}
}

// variantToValue maps what the server holds into the documented Go types:
// bool, int64, float64, string, or a slice of those for an array tag. A
// value with no documented form, such as a byte string, is persistent.
func variantToValue(variant *ua.Variant) (any, error) {
	if variant == nil {
		return nil, nil
	}
	switch v := variant.Value().(type) {
	case bool:
		return v, nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return nil, persistentErr("value %d does not fit a documented type", v)
		}
		return int64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return v, nil
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	case []bool:
		return v, nil
	case []int8:
		return int64s(v), nil
	case []int16:
		return int64s(v), nil
	case []int32:
		return int64s(v), nil
	case []int64:
		return v, nil
	case []uint8:
		// Go cannot tell these apart -- a ByteString and an array of Byte
		// both decode to []uint8 -- but the variant can, and refusing both
		// told the owner of a perfectly ordinary Byte[] node that their tag
		// was a byte string. Every other integer array converts; a Byte
		// array is not special, and a single Byte already reads fine.
		if variant.Type() == ua.TypeIDByte {
			return int64s(v), nil
		}
		return nil, persistentErr("a ByteString has no documented value form: " +
			"read it as a String, or as a Byte array if that is what the node holds")
	case []uint16:
		return int64s(v), nil
	case []uint32:
		return int64s(v), nil
	case []uint64:
		values := make([]int64, len(v))
		for i, e := range v {
			if e > math.MaxInt64 {
				return nil, persistentErr("value %d does not fit a documented type", e)
			}
			values[i] = int64(e)
		}
		return values, nil
	case []float32:
		values := make([]float64, len(v))
		for i, e := range v {
			values[i] = float64(e)
		}
		return values, nil
	case []float64:
		return v, nil
	case []string:
		return v, nil
	default:
		return nil, persistentErr("value of type %T has no documented value form", v)
	}
}

// int64s widens a slice of any integer type into the documented int64 form.
func int64s[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32](v []T) []int64 {
	values := make([]int64, len(v))
	for i, e := range v {
		values[i] = int64(e)
	}
	return values
}
