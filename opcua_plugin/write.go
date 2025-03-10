package opcua_plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// OPCUAOutput represents an OPC UA output plugin
type OPCUAOutput struct {
	*OPCUAConnection // Embed the shared connection configuration

	// Output-specific fields
	NodeMappings    []NodeMapping
	ForcedDataTypes map[string]string

	// Handshake configuration
	HandshakeEnabled     bool
	ReadbackTimeoutMs    int
	MaxWriteAttempts     int
	TimeBetweenRetriesMs int
}

// NodeMapping defines how to map message fields to OPC UA nodes
type NodeMapping struct {
	NodeID    string `json:"nodeId"`
	ValueFrom string `json:"valueFrom"`
}

// opcuaOutputConfig creates and returns a configuration spec for the OPC UA output
func opcuaOutputConfig() *service.ConfigSpec {
	return OPCUAConnectionConfigSpec.
		Summary("OPC UA output plugin").
		Description("The OPC UA output plugin writes data to an OPC UA server and optionally verifies the write via a read-back handshake.").
		Field(service.NewObjectListField("nodeMappings",
			service.NewStringField("nodeId").
				Description("The OPC UA node ID to write to.").
				Example("ns=2;s=MyVariable"),
			service.NewStringField("valueFrom").
				Description("The field in the input message to get the value from.").
				Example("value")).
			Description("List of node mappings defining which message fields to write to which OPC UA nodes")).
		Field(service.NewObjectField("forcedDataTypes").
			Description("Optional map of node IDs to data types to force for the write.").
			Default(map[string]any{}).
			Advanced()).
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
		ForcedDataTypes: make(map[string]string),
	}

	// Parse node mappings
	nodeMappingsConf, err := conf.FieldObjectList("nodeMappings")
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(nodeMappingsConf); i++ {
		mapConf := nodeMappingsConf[i]

		nodeID, err := mapConf.FieldString("nodeId")
		if err != nil {
			return nil, err
		}

		valueFrom, err := mapConf.FieldString("valueFrom")
		if err != nil {
			return nil, err
		}

		output.NodeMappings = append(output.NodeMappings, NodeMapping{
			NodeID:    nodeID,
			ValueFrom: valueFrom,
		})
	}

	// Parse forced data types
	if conf.Contains("forcedDataTypes") {
		forcedTypes := map[string]string{}

		forcedTypesObj, err := conf.FieldAny("forcedDataTypes")
		if err != nil {
			return nil, err
		}

		if forcedMap, ok := forcedTypesObj.(map[string]any); ok {
			for key, val := range forcedMap {
				if strVal, ok := val.(string); ok {
					forcedTypes[key] = strVal
				}
			}
		}

		output.ForcedDataTypes = forcedTypes
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

	// Extract values from message
	values := make(map[string]interface{})
	for _, mapping := range o.NodeMappings {
		// Try to get the value from the structured content
		if contentMap, ok := content.(map[string]interface{}); ok {
			if val, exists := contentMap[mapping.ValueFrom]; exists {
				values[mapping.NodeID] = val
			} else {
				return fmt.Errorf("field %s not found in message", mapping.ValueFrom)
			}
		} else {
			return fmt.Errorf("message content is not a map")
		}
	}

	// Write values to OPC UA nodes
	for _, mapping := range o.NodeMappings {
		val := values[mapping.NodeID]
		if val == nil {
			return fmt.Errorf("value for node %s is nil", mapping.NodeID)
		}

		// Parse the node ID
		nodeID, err := ua.ParseNodeID(mapping.NodeID)
		if err != nil {
			return fmt.Errorf("invalid node ID %s: %w", mapping.NodeID, err)
		}

		// Convert value to OPC UA variant
		variant, err := o.convertToVariant(val, mapping.NodeID)
		if err != nil {
			return fmt.Errorf("error converting value for node %s: %w", mapping.NodeID, err)
		}

		// Write the value
		req := &ua.WriteRequest{
			NodesToWrite: []*ua.WriteValue{
				{
					NodeID:      nodeID,
					AttributeID: ua.AttributeIDValue,
					Value: &ua.DataValue{
						Value: variant,
					},
				},
			},
		}

		resp, err := o.Client.Write(ctx, req)
		if err != nil {
			return fmt.Errorf("error writing to node %s: %w", mapping.NodeID, err)
		}

		if resp.Results[0] != ua.StatusOK {
			return fmt.Errorf("write failed for node %s with status %v", mapping.NodeID, resp.Results[0])
		}

		// If handshake is enabled, verify the write
		if o.HandshakeEnabled {
			if err := o.verifyWrite(ctx, nodeID, variant); err != nil {
				return fmt.Errorf("handshake verification failed for node %s: %w", mapping.NodeID, err)
			}
		}
	}

	return nil
}

// Close closes the connection to the OPC UA server
func (o *OPCUAOutput) Close(ctx context.Context) error {
	if o.Client == nil {
		return nil
	}

	o.Log.Infof("Closing connection to OPC UA server")
	return o.Close(ctx)
}

// convertToVariant converts a value to an OPC UA variant
func (o *OPCUAOutput) convertToVariant(val interface{}, nodeID string) (*ua.Variant, error) {
	// Check if we have a forced data type for this node
	if forcedType, ok := o.ForcedDataTypes[nodeID]; ok {
		switch forcedType {
		case "Boolean":
			boolVal, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf("value %v cannot be converted to Boolean", val)
			}
			variant, err := ua.NewVariant(boolVal)
			if err != nil {
				return nil, err
			}
			return variant, nil
		case "Int32":
			intVal, ok := val.(int32)
			if !ok {
				return nil, fmt.Errorf("value %v cannot be converted to Int32", val)
			}
			variant, err := ua.NewVariant(intVal)
			if err != nil {
				return nil, err
			}
			return variant, nil
		case "Float":
			floatVal, ok := val.(float32)
			if !ok {
				return nil, fmt.Errorf("value %v cannot be converted to Float", val)
			}
			variant, err := ua.NewVariant(floatVal)
			if err != nil {
				return nil, err
			}
			return variant, nil
		case "Double":
			doubleVal, ok := val.(float64)
			if !ok {
				return nil, fmt.Errorf("value %v cannot be converted to Double", val)
			}
			variant, err := ua.NewVariant(doubleVal)
			if err != nil {
				return nil, err
			}
			return variant, nil
		case "String":
			strVal, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("value %v cannot be converted to String", val)
			}
			variant, err := ua.NewVariant(strVal)
			if err != nil {
				return nil, err
			}
			return variant, nil
		default:
			return nil, fmt.Errorf("unsupported forced data type: %s", forcedType)
		}
	}

	// If no forced type, try to infer the type
	switch v := val.(type) {
	case bool:
		variant, err := ua.NewVariant(v)
		if err != nil {
			return nil, err
		}
		return variant, nil
	case int32:
		variant, err := ua.NewVariant(v)
		if err != nil {
			return nil, err
		}
		return variant, nil
	case float32:
		variant, err := ua.NewVariant(v)
		if err != nil {
			return nil, err
		}
		return variant, nil
	case float64:
		variant, err := ua.NewVariant(v)
		if err != nil {
			return nil, err
		}
		return variant, nil
	case string:
		variant, err := ua.NewVariant(v)
		if err != nil {
			return nil, err
		}
		return variant, nil
	default:
		return nil, fmt.Errorf("unsupported value type: %T", val)
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
