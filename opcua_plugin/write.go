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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// OPCUAOutput represents an OPC UA output plugin
type OPCUAOutput struct {
	*OPCUAConnection // Embed the shared connection configuration

	// Output-specific fields
	NodeMappings []NodeMapping

	// Handshake configuration
	HandshakeEnabled     bool
	ReadbackTimeoutMs    int
	MaxWriteAttempts     int
	TimeBetweenRetriesMs int
}

// NodeMapping defines how to map message fields to OPC UA nodes
type NodeMapping struct {
	NodeID    *service.InterpolatedString `json:"nodeId"`
	ValueFrom string                      `json:"valueFrom"`
	DataType  *service.InterpolatedString `json:"dataType"`
}

// opcuaOutputConfig creates and returns a configuration spec for the OPC UA output
func opcuaOutputConfig() *service.ConfigSpec {
	return OPCUAConnectionConfigSpec.
		Summary("OPC UA output plugin").
		Description("The OPC UA output plugin writes data to an OPC UA server and optionally verifies the write via a read-back handshake.").
		Field(service.NewObjectListField("nodeMappings",
			service.NewInterpolatedStringField("nodeId").
				Description("The OPC UA node ID to write to. Supports dynamic values through interpolation.").
				Example("ns=2;s=MyVariable").
				Example("ns=2;s=${! json(\"nodeId\") }"),
			service.NewStringField("valueFrom").
				Description("The field in the input message to get the value from.").
				Example("value"),
			service.NewInterpolatedStringField("dataType").
				Description("The OPC UA data type for the value. Supports dynamic values through interpolation.").
				Example("Int32").
				Example("${! json(\"dataType\") }")).
			Description("List of node mappings defining which message fields to write to which OPC UA nodes")).
		Field(service.NewObjectField("handshake",
			service.NewBoolField("enabled").
				Description("Whether to enable read-back verification after writing.").
				Default(true),
			service.NewIntField("readbackTimeoutMs").
				Description("How long to wait for the server to show the updated value.").
				Default(2000),
			service.NewIntField("maxWriteAttempts").
				Description("Number of write attempts if the server fails.").
				Default(1),
			service.NewIntField("timeBetweenRetriesMs").
				Description("Delay between write attempts.").
				Default(1000)).
			Description("Configuration for the read-back handshake.").
			Default(map[string]any{
				"enabled":              true,
				"readbackTimeoutMs":    2000,
				"maxWriteAttempts":     1,
				"timeBetweenRetriesMs": 1000,
			}))
}

// newOPCUAOutput creates a new OPC UA output based on the provided configuration
func newOPCUAOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, error) {
	// Parse the shared connection configuration
	conn, err := ParseConnectionConfig(conf, mgr)
	if err != nil {
		return nil, err
	}

	output := &OPCUAOutput{
		OPCUAConnection: conn,
	}

	// Parse node mappings
	nodeMappingsConf, err := conf.FieldObjectList("nodeMappings")
	if err != nil {
		return nil, err
	}

	// Validate that we have at least one node mapping
	if len(nodeMappingsConf) == 0 {
		return nil, fmt.Errorf("at least one node mapping is required in the 'nodeMappings' configuration. Please refer to the documentation for examples")
	}

	for i := 0; i < len(nodeMappingsConf); i++ {
		mapConf := nodeMappingsConf[i]

		// Get nodeId as InterpolatedString
		nodeIDInterp, err := mapConf.FieldInterpolatedString("nodeId")
		if err != nil {
			return nil, fmt.Errorf("nodeId is required in node mapping %d: %w", i, err)
		}
		
		// For validation during config parsing, we'll check if it's a static value
		// Dynamic values will be validated at runtime
		staticNodeID, exists := nodeIDInterp.Static()
		if exists && staticNodeID != "" {
			// Validate static nodeId format
			if _, err := ua.ParseNodeID(staticNodeID); err != nil {
				return nil, fmt.Errorf("invalid nodeId format in node mapping %d: %s. Expected format examples: 'ns=2;s=MyVariable', 'i=85', 'ns=3;i=1000'. Error: %v", i, staticNodeID, err)
			}
		}

		// Get valueFrom and validate it
		valueFrom, err := mapConf.FieldString("valueFrom")
		if err != nil {
			return nil, fmt.Errorf("valueFrom is required in node mapping %d: %w", i, err)
		}
		if valueFrom == "" {
			return nil, fmt.Errorf("valueFrom in node mapping %d cannot be empty. Please specify the message field to read the value from (e.g., 'value', 'data.temperature')", i)
		}

		// Get dataType as InterpolatedString
		dataTypeInterp, err := mapConf.FieldInterpolatedString("dataType")
		if err != nil {
			return nil, fmt.Errorf("dataType is required in node mapping %d: %w", i, err)
		}

		// For validation during config parsing, we'll check if it's a static value
		// Dynamic values will be validated at runtime
		staticDataType, exists := dataTypeInterp.Static()
		if exists && staticDataType != "" {
			// Validate static dataType is supported
			supportedTypes := []string{
				"Boolean", "SByte", "Byte", "Int16", "UInt16", "Int32", "UInt32",
				"Int64", "UInt64", "Float", "Double", "String", "DateTime",
			}
			validType := false
			for _, t := range supportedTypes {
				if staticDataType == t {
					validType = true
					break
				}
			}
			if !validType {
				return nil, fmt.Errorf("unsupported dataType '%s' in node mapping %d. Supported types are: %s",
					staticDataType, i, strings.Join(supportedTypes, ", "))
			}
		}

		output.NodeMappings = append(output.NodeMappings, NodeMapping{
			NodeID:    nodeIDInterp,
			ValueFrom: valueFrom,
			DataType:  dataTypeInterp,
		})
	}

	// Parse handshake configuration
	handshakeConf := conf.Namespace("handshake")

	if output.HandshakeEnabled, err = handshakeConf.FieldBool("enabled"); err != nil {
		return nil, err
	}
	if output.ReadbackTimeoutMs, err = handshakeConf.FieldInt("readbackTimeoutMs"); err != nil {
		return nil, err
	}
	if output.MaxWriteAttempts, err = handshakeConf.FieldInt("maxWriteAttempts"); err != nil {
		return nil, err
	}
	if output.TimeBetweenRetriesMs, err = handshakeConf.FieldInt("timeBetweenRetriesMs"); err != nil {
		return nil, err
	}

	return output, nil
}

// Connect establishes a connection to the OPC UA server
func (o *OPCUAOutput) Connect(ctx context.Context) error {
	if o.Client != nil {
		return nil
	}

	o.Log.Infof("Connecting to OPC UA server at %s", o.Endpoint)
	return o.connect(ctx)
}

// Write writes a message to the OPC UA server
func (o *OPCUAOutput) Write(ctx context.Context, msg *service.Message) error {
	// Establish connection if not connected
	if o.Client == nil {
		if err := o.Connect(ctx); err != nil {
			return err
		}
	}

	// Get message content as structured data
	content, err := msg.AsStructured()
	if err != nil {
		return fmt.Errorf("error getting message content: %w", err)
	}

	// Extract values and resolve node IDs from message
	type nodeWrite struct {
		nodeID   string
		value    interface{}
		dataType string
	}
	var writes []nodeWrite
	
	for _, mapping := range o.NodeMappings {
		// Resolve the interpolated node ID
		nodeID, err := mapping.NodeID.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to resolve nodeId: %w", err)
		}
		if nodeID == "" || nodeID == "null" {
			return fmt.Errorf("nodeId resolved to empty or null value")
		}
		
		// Resolve the interpolated data type
		dataType, err := mapping.DataType.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to resolve dataType: %w", err)
		}
		if dataType == "" || dataType == "null" {
			return fmt.Errorf("dataType resolved to empty or null value")
		}
		
		// Validate dataType at runtime
		supportedTypes := []string{
			"Boolean", "SByte", "Byte", "Int16", "UInt16", "Int32", "UInt32",
			"Int64", "UInt64", "Float", "Double", "String", "DateTime",
		}
		validType := false
		for _, t := range supportedTypes {
			if dataType == t {
				validType = true
				break
			}
		}
		if !validType {
			return fmt.Errorf("unsupported dataType '%s'. Supported types are: %s",
				dataType, strings.Join(supportedTypes, ", "))
		}
		
		// Try to get the value from the structured content
		if contentMap, ok := content.(map[string]interface{}); ok {
			if val, exists := contentMap[mapping.ValueFrom]; exists {
				writes = append(writes, nodeWrite{
					nodeID:   nodeID,
					value:    val,
					dataType: dataType,
				})
			} else {
				return fmt.Errorf("field %s not found in message", mapping.ValueFrom)
			}
		} else {
			return fmt.Errorf("message content is not a map")
		}
	}

	// Write values to OPC UA nodes
	for _, write := range writes {
		// Parse the node ID
		nodeID, err := ua.ParseNodeID(write.nodeID)
		if err != nil {
			return fmt.Errorf("invalid node ID %s: %w", write.nodeID, err)
		}

		// Convert value to OPC UA variant
		variant, err := o.convertToVariant(write.value, write.dataType)
		if err != nil {
			return fmt.Errorf("error converting value for node %s: %w", write.nodeID, err)
		}

		// Prepare write request
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

		// Implement retry logic
		var writeErr error
		for attempt := 1; attempt <= o.MaxWriteAttempts; attempt++ {
			// Write the value
			resp, err := o.Client.Write(ctx, req)
			if err != nil {
				writeErr = fmt.Errorf("error writing to node %s (attempt %d/%d): %w",
					write.nodeID, attempt, o.MaxWriteAttempts, err)
				o.Log.Warnf("%v", writeErr)
				if attempt < o.MaxWriteAttempts {
					time.Sleep(time.Duration(o.TimeBetweenRetriesMs) * time.Millisecond)
					continue
				}
				return writeErr
			}

			if resp.Results[0] != ua.StatusOK {
				writeErr = fmt.Errorf("write failed for node %s with status %v (attempt %d/%d)",
					write.nodeID, resp.Results[0], attempt, o.MaxWriteAttempts)
				o.Log.Warnf("%v", writeErr)
				if attempt < o.MaxWriteAttempts {
					time.Sleep(time.Duration(o.TimeBetweenRetriesMs) * time.Millisecond)
					continue
				}
				return writeErr
			}

			// If handshake is enabled, verify the write
			if o.HandshakeEnabled {
				if err := o.verifyWrite(ctx, nodeID, variant); err != nil {
					writeErr = fmt.Errorf("handshake verification failed for node %s (attempt %d/%d): %w",
						write.nodeID, attempt, o.MaxWriteAttempts, err)
					o.Log.Warnf("%v", writeErr)
					if attempt < o.MaxWriteAttempts {
						time.Sleep(time.Duration(o.TimeBetweenRetriesMs) * time.Millisecond)
						continue
					}
					return writeErr
				}
			}

			// If we get here, the write was successful
			if attempt > 1 {
				o.Log.Infof("Successfully wrote to node %s after %d attempts", write.nodeID, attempt)
			}
			writeErr = nil
			break
		}

		if writeErr != nil {
			return writeErr
		}
	}

	return nil
}

// convertToVariant converts a value to an OPC UA variant with a specified type
func (o *OPCUAOutput) convertToVariant(val interface{}, dataType string) (*ua.Variant, error) {
	// Handle json.Number first to convert it to a numeric type
	if jsonNum, ok := val.(json.Number); ok {
		// Try float64 first as it's the most flexible
		if floatVal, err := jsonNum.Float64(); err == nil {
			val = floatVal
		} else if intVal, err := jsonNum.Int64(); err == nil {
			// If it's not a float, try int64
			val = intVal
		} else {
			return nil, fmt.Errorf("value %v cannot be converted to a number", val)
		}
	}

	switch dataType {
	case "Boolean":
		// Convert to bool
		var boolVal bool
		switch v := val.(type) {
		case bool:
			boolVal = v
		case int:
			boolVal = v != 0
		case int64:
			boolVal = v != 0
		case float64:
			boolVal = v != 0
		case string:
			var err error
			boolVal, err = strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to Boolean: %w", val, err)
			}
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to Boolean", val, val)
		}
		return ua.NewVariant(boolVal)

	case "SByte":
		// Convert to int8
		var int8Val int8
		switch v := val.(type) {
		case int8:
			int8Val = v
		case int:
			if v < -128 || v > 127 {
				return nil, fmt.Errorf("value %v is out of range for SByte", val)
			}
			int8Val = int8(v)
		case int64:
			if v < -128 || v > 127 {
				return nil, fmt.Errorf("value %v is out of range for SByte", val)
			}
			int8Val = int8(v)
		case float64:
			if v < -128 || v > 127 {
				return nil, fmt.Errorf("value %v is out of range for SByte", val)
			}
			int8Val = int8(v)
		case string:
			intVal, err := strconv.ParseInt(v, 10, 8)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to SByte: %w", val, err)
			}
			int8Val = int8(intVal)
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to SByte", val, val)
		}
		return ua.NewVariant(int8Val)

	case "Byte":
		// Convert to uint8
		var uint8Val uint8
		switch v := val.(type) {
		case uint8:
			uint8Val = v
		case int:
			if v < 0 || v > 255 {
				return nil, fmt.Errorf("value %v is out of range for Byte", val)
			}
			uint8Val = uint8(v)
		case int64:
			if v < 0 || v > 255 {
				return nil, fmt.Errorf("value %v is out of range for Byte", val)
			}
			uint8Val = uint8(v)
		case float64:
			if v < 0 || v > 255 {
				return nil, fmt.Errorf("value %v is out of range for Byte", val)
			}
			uint8Val = uint8(v)
		case string:
			uintVal, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to Byte: %w", val, err)
			}
			uint8Val = uint8(uintVal)
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to Byte", val, val)
		}
		return ua.NewVariant(uint8Val)

	case "Int16":
		// Convert to int16
		var int16Val int16
		switch v := val.(type) {
		case int16:
			int16Val = v
		case int:
			if v < -32768 || v > 32767 {
				return nil, fmt.Errorf("value %v is out of range for Int16", val)
			}
			int16Val = int16(v)
		case int64:
			if v < -32768 || v > 32767 {
				return nil, fmt.Errorf("value %v is out of range for Int16", val)
			}
			int16Val = int16(v)
		case float64:
			if v < -32768 || v > 32767 {
				return nil, fmt.Errorf("value %v is out of range for Int16", val)
			}
			int16Val = int16(v)
		case string:
			intVal, err := strconv.ParseInt(v, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to Int16: %w", val, err)
			}
			int16Val = int16(intVal)
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to Int16", val, val)
		}
		return ua.NewVariant(int16Val)

	case "UInt16":
		// Convert to uint16
		var uint16Val uint16
		switch v := val.(type) {
		case uint16:
			uint16Val = v
		case int:
			if v < 0 || v > 65535 {
				return nil, fmt.Errorf("value %v is out of range for UInt16", val)
			}
			uint16Val = uint16(v)
		case int64:
			if v < 0 || v > 65535 {
				return nil, fmt.Errorf("value %v is out of range for UInt16", val)
			}
			uint16Val = uint16(v)
		case float64:
			if v < 0 || v > 65535 {
				return nil, fmt.Errorf("value %v is out of range for UInt16", val)
			}
			uint16Val = uint16(v)
		case string:
			uintVal, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to UInt16: %w", val, err)
			}
			uint16Val = uint16(uintVal)
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to UInt16", val, val)
		}
		return ua.NewVariant(uint16Val)

	case "Int32":
		// Convert to int32
		var int32Val int32
		switch v := val.(type) {
		case int32:
			int32Val = v
		case int:
			int32Val = int32(v)
		case int64:
			if v > math.MaxInt32 || v < math.MinInt32 {
				return nil, fmt.Errorf("value %v is out of range for Int32", val)
			}
			int32Val = int32(v)
		case float64:
			if v > math.MaxInt32 || v < math.MinInt32 {
				return nil, fmt.Errorf("value %v is out of range for Int32", val)
			}
			int32Val = int32(v)
		case string:
			intVal, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to Int32: %w", val, err)
			}
			int32Val = int32(intVal)
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to Int32", val, val)
		}
		return ua.NewVariant(int32Val)

	case "UInt32":
		// Convert to uint32
		var uint32Val uint32
		switch v := val.(type) {
		case uint32:
			uint32Val = v
		case int:
			if v < 0 {
				return nil, fmt.Errorf("value %v is out of range for UInt32", val)
			}
			uint32Val = uint32(v)
		case int64:
			if v < 0 || v > math.MaxUint32 {
				return nil, fmt.Errorf("value %v is out of range for UInt32", val)
			}
			uint32Val = uint32(v)
		case float64:
			if v < 0 || v > math.MaxUint32 {
				return nil, fmt.Errorf("value %v is out of range for UInt32", val)
			}
			uint32Val = uint32(v)
		case string:
			uintVal, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to UInt32: %w", val, err)
			}
			uint32Val = uint32(uintVal)
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to UInt32", val, val)
		}
		return ua.NewVariant(uint32Val)

	case "Int64":
		// Convert to int64
		var int64Val int64
		switch v := val.(type) {
		case int64:
			int64Val = v
		case int:
			int64Val = int64(v)
		case float64:
			if v > math.MaxInt64 || v < math.MinInt64 {
				return nil, fmt.Errorf("value %v is out of range for Int64", val)
			}
			int64Val = int64(v)
		case string:
			var err error
			int64Val, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to Int64: %w", val, err)
			}
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to Int64", val, val)
		}
		return ua.NewVariant(int64Val)

	case "UInt64":
		// Convert to uint64
		var uint64Val uint64
		switch v := val.(type) {
		case uint64:
			uint64Val = v
		case int:
			if v < 0 {
				return nil, fmt.Errorf("value %v is out of range for UInt64", val)
			}
			uint64Val = uint64(v)
		case int64:
			if v < 0 {
				return nil, fmt.Errorf("value %v is out of range for UInt64", val)
			}
			uint64Val = uint64(v)
		case float64:
			if v < 0 || v > math.MaxUint64 {
				return nil, fmt.Errorf("value %v is out of range for UInt64", val)
			}
			uint64Val = uint64(v)
		case string:
			var err error
			uint64Val, err = strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to UInt64: %w", val, err)
			}
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to UInt64", val, val)
		}
		return ua.NewVariant(uint64Val)

	case "Float":
		// Convert to float32
		var float32Val float32
		switch v := val.(type) {
		case float32:
			float32Val = v
		case int:
			float32Val = float32(v)
		case int64:
			float32Val = float32(v)
		case float64:
			if v > math.MaxFloat32 || v < -math.MaxFloat32 {
				return nil, fmt.Errorf("value %v is out of range for Float", val)
			}
			float32Val = float32(v)
		case string:
			float64Val, err := strconv.ParseFloat(v, 32)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to Float: %w", val, err)
			}
			float32Val = float32(float64Val)
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to Float", val, val)
		}
		return ua.NewVariant(float32Val)

	case "Double":
		// Convert to float64
		var float64Val float64
		switch v := val.(type) {
		case float64:
			float64Val = v
		case float32:
			float64Val = float64(v)
		case int:
			float64Val = float64(v)
		case int64:
			float64Val = float64(v)
		case string:
			var err error
			float64Val, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to Double: %w", val, err)
			}
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to Double", val, val)
		}
		return ua.NewVariant(float64Val)

	case "String":
		// Convert to string
		var stringVal string
		switch v := val.(type) {
		case string:
			stringVal = v
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
			stringVal = fmt.Sprintf("%v", v)
		case map[string]interface{}, []interface{}:
			// Handle JSON objects and arrays by converting them to JSON strings
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("value %v cannot be converted to JSON string: %w", val, err)
			}
			stringVal = string(jsonBytes)
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to String", val, val)
		}
		return ua.NewVariant(stringVal)

	case "DateTime":
		// Convert to time.Time
		var timeVal time.Time
		switch v := val.(type) {
		case time.Time:
			timeVal = v
		case string:
			// Try a few common formats
			formats := []string{
				time.RFC3339,
				time.RFC3339Nano,
				"2006-01-02T15:04:05",
				"2006-01-02 15:04:05",
				"2006-01-02",
			}
			var err error
			var parsed bool
			for _, format := range formats {
				timeVal, err = time.Parse(format, v)
				if err == nil {
					parsed = true
					break
				}
			}
			if !parsed {
				return nil, fmt.Errorf("value %v cannot be converted to DateTime: not a recognized time format", val)
			}
		default:
			return nil, fmt.Errorf("value %v of type %T cannot be converted to DateTime", val, val)
		}
		return ua.NewVariant(timeVal)

	default:
		return nil, fmt.Errorf("unsupported data type: %s", dataType)
	}
}

// verifyWrite verifies that a write was successful by reading back the value
func (o *OPCUAOutput) verifyWrite(ctx context.Context, nodeID *ua.NodeID, expected *ua.Variant) error {
	// Create a timeout context for the read-back
	ctx, cancel := context.WithTimeout(ctx, time.Duration(o.ReadbackTimeoutMs)*time.Millisecond)
	defer cancel()

	// Create read request
	req := &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{
			{
				NodeID:      nodeID,
				AttributeID: ua.AttributeIDValue,
			},
		},
	}

	// Read the value
	resp, err := o.Client.Read(ctx, req)
	if err != nil {
		return fmt.Errorf("error reading back value: %w", err)
	}

	if resp.Results[0].Status != ua.StatusOK {
		return fmt.Errorf("read-back failed with status %v", resp.Results[0].Status)
	}

	// Compare the values
	if !variantEqual(resp.Results[0].Value, expected) {
		return fmt.Errorf("read-back value %v does not match written value %v", resp.Results[0].Value, expected)
	}

	return nil
}

// variantEqual compares two OPC UA variants for equality
func variantEqual(v1, v2 *ua.Variant) bool {
	// Check if either variant is nil
	if v1 == nil || v2 == nil {
		return v1 == v2
	}

	// Compare types first
	if v1.Type() != v2.Type() {
		return false
	}

	// Compare values
	return v1.Value() == v2.Value()
}

func init() {
	err := service.RegisterOutput(
		"opcua",
		opcuaOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			output, err := newOPCUAOutput(conf, mgr)
			if err != nil {
				return nil, 0, err
			}
			mgr.Logger().Infof("OPC UA output plugin by the United Manufacturing Hub. About us: www.umh.app")
			return output, 1, nil
		})
	if err != nil {
		panic(err)
	}
}
