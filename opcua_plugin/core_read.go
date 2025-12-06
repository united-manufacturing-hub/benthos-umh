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
	"fmt"
	"strconv"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Context key for capability probe flag
type contextKey string

const capabilityProbeKey contextKey = "capability_probe"

// WithCapabilityProbe returns a context marked for capability probe operations.
// Capability probe reads are expected to fail on servers that don't support optional features.
func WithCapabilityProbe(ctx context.Context) context.Context {
	return context.WithValue(ctx, capabilityProbeKey, true)
}

// isCapabilityProbe checks if context is marked for capability probe operations.
func isCapabilityProbe(ctx context.Context) bool {
	if val := ctx.Value(capabilityProbeKey); val != nil {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}

// getBytesFromValue returns the bytes and the tag type for a given OPC UA DataValue and NodeDef.
func (g *OPCUAConnection) getBytesFromValue(dataValue *ua.DataValue, nodeDef NodeDef) ([]byte, string) {
	variant := dataValue.Value
	if variant == nil {
		g.Log.Errorf("Variant is nil")
		return nil, ""
	}

	if !errors.Is(dataValue.Status, ua.StatusOK) {
		g.Log.Warnf("Received bad status %v for node %s", dataValue.Status, nodeDef.NodeID.String())
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
// specific error conditions by closing the current session and signaling that the client is
// no longer connected, prompting reconnection attempts if necessary. Successful reads return
// the ReadResponse, while errors are appropriately logged and propagated.
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

	if !errors.Is(resp.Results[0].Status, ua.StatusOK) {
		// Capability probes are expected to fail on servers without optional features
		if isCapabilityProbe(ctx) {
			g.Log.Debugf("Capability probe returned status: %v (expected for servers without this feature)", resp.Results[0].Status)
		} else {
			g.Log.Errorf("Status not OK: %v", resp.Results[0].Status)
		}
		return nil, fmt.Errorf("status not OK: %w", resp.Results[0].Status)
	}

	return resp, nil
}
