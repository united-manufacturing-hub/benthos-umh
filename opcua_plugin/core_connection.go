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
		Examples("opc.tcp://localhost:4840", "opc.tcp://192.168.1.100:4840", "opc.tcp://10.0.0.50:4840", "opc.tcp://plc.local:4840")).
	Field(service.NewStringField("username").
		Description("The username for authentication.").
		Default("").
		Optional().
		Examples("", "admin", "opcuser").
		Advanced()).
	Field(service.NewStringField("password").
		Description("The password for authentication.").
		Default("").
		Optional().
		Examples("", "password123").
		Secret().
		Advanced()).
	Field(service.NewIntField("sessionTimeout").
		Description("The duration in milliseconds that a OPC UA session will last. Is used to ensure that older failed sessions will timeout and that we will not get a TooManySession error.").
		Default(10000).
		Examples(10000, 30000, 60000).
		Optional().
		Advanced()).
	Field(service.NewStringField("securityMode").
		Description("The security mode to use. Options: None, Sign, SignAndEncrypt").
		Default("").
		Examples("", "None", "Sign", "SignAndEncrypt").
		Optional().
		Advanced()).
	Field(service.NewStringField("securityPolicy").
		Description("The security policy to use. Options: None, Basic128Rsa15, Basic256, Basic256Sha256").
		Default("").
		Examples("", "None", "Basic256", "Basic256Sha256").
		Optional().
		Advanced()).
	Field(service.NewStringField("clientCertificate").
		Description("The client certificate to use, base64-encoded.").
		Default("").
		Optional().
		Advanced()).
	Field(service.NewStringField("serverCertificateFingerprint").
		Description("The server certificate fingerprint to verify, SHA3-512 hash.").
		Default("").
		Optional().
		Advanced()).
	Field(service.NewStringField("userCertificate").
		Description("User certificate in base64 encoded format of either PEM or DER.").
		Default("").
		Optional().
		Advanced()).
	Field(service.NewStringField("userPrivateKey").
		Description("User private key in base64 format of PEM for user certificate based authentication.").
		Default("").
		Optional().
		Advanced()).
	Field(service.NewBoolField("insecure").
		Description("Set to true to bypass secure connections, useful in case of SSL or certificate issues. Default is secure (false).").
		Default(false).
		Examples(false, true).
		Optional().
		Advanced()).
	Field(service.NewBoolField("directConnect").
		Description("Set this to true to directly connect to an OPC UA endpoint. This can be necessary in cases where the OPC UA server does not allow 'endpoint discovery'. This requires having the full endpoint name in endpoint, and securityMode and securityPolicy set.").
		Default(false).
		Examples(false, true).
		Optional().
		Advanced()).
	Field(service.NewBoolField("autoReconnect").
		Description("Set to true to automatically reconnect to the OPC UA server when the connection is lost.").
		Default(false).
		Examples(false, true).
		Optional().
		Advanced()).
	Field(service.NewIntField("reconnectIntervalInSeconds").
		Description("The interval in seconds at which to reconnect to the OPC UA server when the connection is lost. This is only used if `autoReconnect` is set to true.").
		Default(5).
		Examples(5, 10, 30).
		Optional().
		Advanced()).
	Field(service.NewStringField("profile").
		Description("Server profile for performance tuning. Leave empty for automatic detection (recommended). Manual options: auto, high-performance, ignition, kepware, siemens-s7-1200, siemens-s7-1500, prosys.").
		Default("").
		Optional().
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
	Profile                      string // Manual profile override (optional)
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
	if conn.Profile, err = conf.FieldString("profile"); err != nil {
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
