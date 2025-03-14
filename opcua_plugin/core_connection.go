package opcua_plugin

import (
	"context"
	"crypto/tls"
	"sync"
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
	Field(service.NewStringField("userCertificate").
		Description("User certificate in base64 encoded format of either PEM or DER.").
		Default("").
		Advanced()).
	Field(service.NewStringField("userPrivateKey").
		Description("User private key in base64 format of PEM for user certificate based authentication.").
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
	UserCertificate              string
	UserPrivateKey               string
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

	// Custom cleanup function for write or read plugin, e.g., unsubscribe from a subscription
	cleanup_func func(context.Context)

	// for browsing
	browseCancel    context.CancelFunc
	browseWaitGroup sync.WaitGroup
	visited         sync.Map
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
	if conn.UserCertificate, err = conf.FieldString("userCertificate"); err != nil {
		return nil, err
	}
	if conn.UserPrivateKey, err = conf.FieldString("userPrivateKey"); err != nil {
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

	conn.browseWaitGroup = sync.WaitGroup{}

	return conn, nil
}

// cleanupBrowsing ensures the browsing goroutine is properly stopped and cleaned up
func (g *OPCUAConnection) cleanupBrowsing() {
	if g.browseCancel != nil {
		g.browseCancel()
		g.browseCancel = nil

		g.Log.Infof("Waiting for browsing subroutine to finish...")
		g.browseWaitGroup.Wait()
		g.Log.Infof("Browsing subroutine finished")
	}
}

// closeConnection handles the actual connection closure
func (g *OPCUAConnection) closeConnection(ctx context.Context) {
	// Call the custom cleanup function if set
	if g.cleanup_func != nil {
		g.cleanup_func(ctx)
	}

	if g.Client != nil {
		if err := g.Client.Close(ctx); err != nil {
			g.Log.Infof("Error closing OPC UA client: %v", err)
		}
		g.Client = nil
	}

}

// Close terminates the OPC UA connection with error logging
func (g *OPCUAConnection) Close(ctx context.Context) error {
	g.Log.Errorf("Initiating closure of OPC UA client...")
	g.cleanupBrowsing()
	g.closeConnection(ctx)
	g.Log.Infof("OPC UA client closed successfully.")
	return nil
}

// CloseExpected terminates the OPC UA connection without error logging
func (g *OPCUAConnection) CloseExpected(ctx context.Context) {
	g.Log.Infof("Initiating expected closure of OPC UA client...")
	g.cleanupBrowsing()
	g.closeConnection(ctx)
	g.Log.Infof("OPC UA client closed successfully.")
}
