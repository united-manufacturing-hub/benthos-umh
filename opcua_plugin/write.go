package opcua_plugin

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/gopcua/opcua"
)

// OPCUAOutput represents an OPC UA output plugin
type OPCUAOutput struct {
	// Connection-related fields (similar to OPCUAInput)
	Endpoint                     string
	Username                     string
	Password                     string
	SecurityMode                 string
	SecurityPolicy               string
	ClientCertificate            string
	ServerCertificateFingerprint string
	Client                       *opcua.Client
	Log                          *service.Logger

	// Output-specific fields
	NodeMappings    []NodeMapping
	ForcedDataTypes map[string]string

	// Handshake configuration
	HandshakeEnabled     bool
	ReadbackTimeoutMs    int
	MaxWriteAttempts     int
	TimeBetweenRetriesMs int

	// Reconnection settings
	AutoReconnect              bool
	ReconnectIntervalInSeconds int
}

// NodeMapping defines how to map message fields to OPC UA nodes
type NodeMapping struct {
	NodeID    string `json:"nodeId"`
	ValueFrom string `json:"valueFrom"`
}

// TODO: double check if all paramters from opcuaInput are also implemented here
// sessionTimeout, autoReconnect, reconnectIntervalInSeconds are missing
// nodeIDs, subscribeEnabled, useHeartbeat, pollRate are missing but not needed for output
// insecure, directConnect are missing but deprecated anyway
// opcuaOutputConfig creates and returns a configuration spec for the OPC UA output
func opcuaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Writes data to an OPC UA server.").
		Description("The OPC UA output plugin writes data to an OPC UA server and optionally verifies the write via a read-back handshake.").
		Field(service.NewStringField("endpoint").
			Description("The OPC UA server endpoint to connect to.").
			Example("opc.tcp://localhost:4840")).
		Field(service.NewStringField("username").
			Description("The username for authentication.").
			Default("").
			Advanced()).
		Field(service.NewStringField("password").
			Description("The password for authentication.").
			Default("").
			Secret().
			Advanced()).
		Field(service.NewStringField("securityMode").
			Description("The security mode to use. Options: None, Sign, SignAndEncrypt").
			Default("").
			Advanced()).
		Field(service.NewStringField("securityPolicy").
			Description("The security policy to use. Options: None, Basic128Rsa15, Basic256, Basic256Sha256").
			Default("").
			Advanced()).
		Field(service.NewStringField("clientCertificate").
			Description("The client certificate to use, base64-encoded.").
			Default("").
			Advanced()).
		Field(service.NewStringField("serverCertificateFingerprint").
			Description("The server certificate fingerprint to verify, SHA3-512 hash.").
			Default("").
			Advanced()).
		Field(service.NewObjectListField("nodeMappings",
			service.NewObjectField("",
				service.NewStringField("nodeId").
					Description("The OPC UA node ID to write to.").
					Example("ns=2;s=MyVariable"),
				service.NewStringField("valueFrom").
					Description("The field in the input message to get the value from.").
					Example("value"),
			),
		).Description("Mapping of message fields to OPC UA nodes")).
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
				Default(1000),
		).
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
	output := &OPCUAOutput{
		Log:             mgr.Logger(),
		ForcedDataTypes: make(map[string]string),
	}

	// Parse endpoint
	var err error
	if output.Endpoint, err = conf.FieldString("endpoint"); err != nil {
		return nil, err
	}

	// Parse authentication
	if output.Username, err = conf.FieldString("username"); err != nil {
		return nil, err
	}
	if output.Password, err = conf.FieldString("password"); err != nil {
		return nil, err
	}

	// Parse security settings
	if output.SecurityMode, err = conf.FieldString("securityMode"); err != nil {
		return nil, err
	}
	if output.SecurityPolicy, err = conf.FieldString("securityPolicy"); err != nil {
		return nil, err
	}
	if output.ClientCertificate, err = conf.FieldString("clientCertificate"); err != nil {
		return nil, err
	}
	if output.ServerCertificateFingerprint, err = conf.FieldString("serverCertificateFingerprint"); err != nil {
		return nil, err
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

	// Parse reconnection settings
	reconnectConf := conf.Namespace("reconnect")

	if output.AutoReconnect, err = reconnectConf.FieldBool("enabled"); err != nil {
		return nil, err
	}
	if output.ReconnectIntervalInSeconds, err = reconnectConf.FieldInt("intervalInSeconds"); err != nil {
		return nil, err
	}

	return output, nil
}

// Connect establishes a connection to the OPC UA server
func (o *OPCUAOutput) Connect(ctx context.Context) error {
	if o.Client != nil {
		return nil
	}

	// For now, just log a message for testing
	o.Log.Infof("Connecting to OPC UA server at %s", o.Endpoint)

	// TODO: Implement actual connection logic, reusing code from OPCUAInput

	return nil
}

// Write writes a message to the OPC UA server
func (o *OPCUAOutput) Write(ctx context.Context, msg *service.Message) error {
	// Establish connection if not connected
	if o.Client == nil {
		if err := o.Connect(ctx); err != nil {
			return err
		}
	}

	// For now, just log a message for testing
	o.Log.Infof("Writing message to OPC UA server")

	// TODO: Implement actual write logic with handshake verification

	return nil
}

// Close closes the connection to the OPC UA server
func (o *OPCUAOutput) Close(ctx context.Context) error {
	if o.Client == nil {
		return nil
	}

	// For now, just log a message for testing
	o.Log.Infof("Closing connection to OPC UA server")

	// TODO: Implement actual close logic

	return nil
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
