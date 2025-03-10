package opcua_plugin

import (
	"crypto/tls"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Session and connection constants
const SessionTimeout = 5 * time.Second

var OPCUAConnectionConfigSpec = service.NewConfigSpec().
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
	Field(service.NewIntField("sessionTimeout").
		Description("The duration in milliseconds that a OPC UA session will last. Is used to ensure that older failed sessions will timeout and that we will not get a TooManySession error.").
		Default(10000).
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
	Field(service.NewBoolField("insecure").
		Description("Set to true to bypass secure connections, useful in case of SSL or certificate issues. Default is secure (false).").
		Default(false).
		Advanced()).
	Field(service.NewBoolField("directConnect").
		Description("Set this to true to directly connect to an OPC UA endpoint. This can be necessary in cases where the OPC UA server does not allow 'endpoint discovery'. This requires having the full endpoint name in endpoint, and securityMode and securityPolicy set.").
		Default(false).
		Advanced()).
	Field(service.NewBoolField("autoReconnect").
		Description("Set to true to automatically reconnect to the OPC UA server when the connection is lost.").
		Default(false).
		Advanced()).
	Field(service.NewIntField("reconnectIntervalInSeconds").
		Description("The interval in seconds at which to reconnect to the OPC UA server when the connection is lost. This is only used if `autoReconnect` is set to true.").
		Default(5).
		Advanced())

// OPCUAConnection represents the common connection configuration for OPC UA plugins
type OPCUAConnection struct {
	Endpoint                     string
	Username                     string
	Password                     string
	SecurityMode                 string
	SecurityPolicy               string
	ClientCertificate            string
	ServerCertificateFingerprint string
	Insecure                     bool
	DirectConnect                bool
	AutoReconnect                bool
	ReconnectIntervalInSeconds   int
	SessionTimeout               int
	Log                          *service.Logger
	Client                       *opcua.Client
	CachedTLSCertificate         *tls.Certificate // certificate
	ServerCertificates           map[*ua.EndpointDescription]string
}

// ParseConnectionConfig parses the common connection configuration from a ParsedConfig
func ParseConnectionConfig(conf *service.ParsedConfig, mgr *service.Resources) (*OPCUAConnection, error) {
	conn := &OPCUAConnection{
		Log: mgr.Logger(),
	}

	var err error
	if conn.Endpoint, err = conf.FieldString("endpoint"); err != nil {
		return nil, err
	}
	if conn.Username, err = conf.FieldString("username"); err != nil {
		return nil, err
	}
	if conn.Password, err = conf.FieldString("password"); err != nil {
		return nil, err
	}
	if conn.SecurityMode, err = conf.FieldString("securityMode"); err != nil {
		return nil, err
	}
	if conn.SecurityPolicy, err = conf.FieldString("securityPolicy"); err != nil {
		return nil, err
	}
	if conn.ClientCertificate, err = conf.FieldString("clientCertificate"); err != nil {
		return nil, err
	}
	if conn.ServerCertificateFingerprint, err = conf.FieldString("serverCertificateFingerprint"); err != nil {
		return nil, err
	}
	if conn.Insecure, err = conf.FieldBool("insecure"); err != nil {
		return nil, err
	}
	if conn.DirectConnect, err = conf.FieldBool("directConnect"); err != nil {
		return nil, err
	}
	if conn.AutoReconnect, err = conf.FieldBool("autoReconnect"); err != nil {
		return nil, err
	}
	if conn.ReconnectIntervalInSeconds, err = conf.FieldInt("reconnectIntervalInSeconds"); err != nil {
		return nil, err
	}
	if conn.SessionTimeout, err = conf.FieldInt("sessionTimeout"); err != nil {
		return nil, err
	}

	return conn, nil
}
