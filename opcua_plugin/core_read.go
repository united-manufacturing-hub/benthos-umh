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

package opcua_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// statusIsBad reports whether a StatusCode has BAD severity per OPC UA Part 8.
// The top 2 bits of the 32-bit code encode severity: 00=Good, 01=Uncertain, 10/11=Bad.
// GOOD variants (e.g. GoodClamped, GoodLocalOverride) and UNCERTAIN codes carry usable
// data and must not be dropped by callers checking value validity.
func statusIsBad(code ua.StatusCode) bool {
	return uint32(code)>>30 >= 2
}

// getBytesFromValue returns the bytes and the tag type for a given OPC UA DataValue and NodeDef.
func (g *OPCUAConnection) getBytesFromValue(dataValue *ua.DataValue, nodeDef NodeDef) ([]byte, string) {
	variant := dataValue.Value
	if variant == nil || variant.Value() == nil {
		g.Log.Errorf("Variant is nil")
		return nil, ""
	}

	if statusIsBad(dataValue.Status) {
		g.Log.Warnf("Skipping node %s: bad status %v", nodeDef.NodeID.String(), dataValue.Status)
		return nil, ""
	}

	b := make([]byte, 0)

	var tagType string

	switch v := variant.Value().(type) {
	case float32:
		b = append(b, []byte(strconv.FormatFloat(float64(v), 'f', -1, 32))...)
		tagType = "number"
	case float64:
		b = append(b, []byte(strconv.FormatFloat(v, 'f', -1, 64))...)
		tagType = "number"
	case string:
		b = append(b, []byte(v)...)
		tagType = "string"
	case bool:
		b = append(b, []byte(strconv.FormatBool(v))...)
		tagType = "bool"
	case int:
		b = append(b, []byte(strconv.Itoa(v))...)
		tagType = "number"
	case int8:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
		tagType = "number"
	case int16:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
		tagType = "number"
	case int32:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
		tagType = "number"
	case int64:
		b = append(b, []byte(strconv.FormatInt(v, 10))...)
		tagType = "number"
	case uint:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
		tagType = "number"
	case uint8:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
		tagType = "number"
	case uint16:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
		tagType = "number"
	case uint32:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
		tagType = "number"
	case uint64:
		b = append(b, []byte(strconv.FormatUint(v, 10))...)
		tagType = "number"
	case *ua.ExtensionObject:
		if v == nil || v.Value == nil {
			// Unregistered type — skip (binary data already discarded by gopcua).
			// Note: a typed-nil *ua.ExtensionObject still matches this case, so guard v itself.
			// TypeID / TypeID.NodeID can also be nil for unknown extension types.
			typeIDStr := "<unknown>"
			if v != nil && v.TypeID != nil && v.TypeID.NodeID != nil {
				typeIDStr = v.TypeID.NodeID.String()
			}
			g.Log.Warnf("Skipping node %s: ExtensionObject type %s not decodable (custom UDT not registered)",
				nodeDef.NodeID.String(), typeIDStr)
			return nil, ""
		}
		// Type was registered and decoded — serialize the actual value
		jsonBytes, err := json.Marshal(v.Value)
		if err != nil {
			g.Log.Errorf("Error marshaling ExtensionObject value for node %s: %v", nodeDef.NodeID.String(), err)
			return nil, ""
		}
		b = append(b, jsonBytes...)
		tagType = "string"
	case []*ua.ExtensionObject:
		// Filter: collect only decoded Extension Objects
		var decoded []interface{}
		for _, eo := range v {
			if eo != nil && eo.Value != nil {
				decoded = append(decoded, eo.Value)
			}
		}
		if len(decoded) == 0 {
			g.Log.Warnf("Skipping node %s: array of %d ExtensionObjects, none decodable (custom UDTs not registered)",
				nodeDef.NodeID.String(), len(v))
			return nil, ""
		}
		jsonBytes, err := json.Marshal(decoded)
		if err != nil {
			g.Log.Errorf("Error marshaling ExtensionObject array for node %s: %v", nodeDef.NodeID.String(), err)
			return nil, ""
		}
		b = append(b, jsonBytes...)
		tagType = "string"
	default:
		// Convert unknown types to JSON
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			g.Log.Errorf("Error marshaling to JSON: %v", err)
			return nil, ""
		}
		b = append(b, jsonBytes...)
		tagType = "string"
	}

	if b == nil {
		g.Log.Errorf("Could not create benthos message as payload is empty for node %s: %v", nodeDef.NodeID.String(), b)
		return nil, ""
	}

	return b, tagType
}

// Read performs a synchronous read operation on the OPC UA server using the provided ReadRequest.
//
// This function sends a ReadRequest to the OPC UA server and handles the response. It manages
// specific session/transport error conditions by closing the current session and signaling that
// the client is no longer connected, prompting reconnection attempts if necessary.
//
// NOTE: Read only returns a non-nil error for session/transport failures. Per-result problems
// (e.g. StatusBadDataTypeIDUnknown on a single node, UNCERTAIN values, undecodable
// ExtensionObjects) are NOT surfaced as errors here — they are carried on each
// resp.Results[i].Status. Callers must iterate resp.Results and inspect DataValue.Status per
// entry (see statusIsBad and getBytesFromValue for the canonical BAD-severity filter used in
// this plugin). Failing to do so will silently propagate bad values downstream.
func (g *OPCUAConnection) Read(ctx context.Context, req *ua.ReadRequest) (*ua.ReadResponse, error) {
	resp, err := g.Client.Read(ctx, req)
	if err != nil {
		g.Log.Errorf("Read failed: %s", err)
		// if the error is StatusBadSessionIDInvalid, the session has been closed, and we need to reconnect.
		switch {
		case errors.Is(err, ua.StatusBadSessionIDInvalid):
			_ = g.Close(ctx)
			return nil, service.ErrNotConnected
		case errors.Is(err, ua.StatusBadCommunicationError):
			_ = g.Close(ctx)
			return nil, service.ErrNotConnected
		case errors.Is(err, ua.StatusBadConnectionClosed):
			_ = g.Close(ctx)
			return nil, service.ErrNotConnected
		case errors.Is(err, ua.StatusBadTimeout):
			_ = g.Close(ctx)
			return nil, service.ErrNotConnected
		case errors.Is(err, ua.StatusBadConnectionRejected):
			_ = g.Close(ctx)
			return nil, service.ErrNotConnected
		case errors.Is(err, ua.StatusBadServerNotConnected):
			_ = g.Close(ctx)
			return nil, service.ErrNotConnected
		}

		// return error and stop executing this function.
		return nil, err
	}

	return resp, nil
}
