// Copyright 2023 UMH Systems GmbH
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
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/errors"
	"github.com/gopcua/opcua/ua"
)

const SessionTimeout = 5 * time.Second
const SubscribeTimeoutContext = 10 * time.Second

var OPCUAConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from OPC-UA servers. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Field(service.NewStringField("endpoint").Description("Address of the OPC-UA server to connect with.")).
	Field(service.NewStringField("username").Description("Username for server access. If not set, no username is used.").Default("")).
	Field(service.NewStringField("password").Description("Password for server access. If not set, no password is used.").Default("")).
	Field(service.NewStringField("sessionTimeout").Description("The duration in milliseconds that a OPC UA session will last. Is used to ensure that older failed sessions will timeout and that we will not get a TooManySession error.").Default(10000)).
	Field(service.NewStringListField("nodeIDs").Description("List of OPC-UA node IDs to begin browsing.")).
	Field(service.NewStringField("securityMode").Description("Security mode to use. If not set, a reasonable security mode will be set depending on the discovered endpoints.").Default("")).
	Field(service.NewStringField("securityPolicy").Description("The security policy to use.  If not set, a reasonable security policy will be set depending on the discovered endpoints.").Default("")).
	Field(service.NewBoolField("insecure").Description("Set to true to bypass secure connections, useful in case of SSL or certificate issues. Default is secure (false).").Default(false)).
	Field(service.NewBoolField("subscribeEnabled").Description("Set to true to subscribe to OPC UA nodes instead of fetching them every seconds. Default is pulling messages every second (false).").Default(false)).
	Field(service.NewBoolField("directConnect").Description("Set this to true to directly connect to an OPC UA endpoint. This can be necessary in cases where the OPC UA server does not allow 'endpoint discovery'. This requires having the full endpoint name in endpoint, and securityMode and securityPolicy set. Defaults to 'false'").Default(false)).
	Field(service.NewBoolField("useHeartbeat").Description("Set to true to provide an extra message with the servers timestamp as a heartbeat").Default(false))

func ParseNodeIDs(incomingNodes []string) []*ua.NodeID {

	// Parse all nodeIDs to validate them.
	// loop through all nodeIDs, parse them and put them into a slice
	var parsedNodeIDs []*ua.NodeID

	for _, incomingNodeId := range incomingNodes {
		parsedNodeID, err := ua.ParseNodeID(incomingNodeId)
		if err != nil {
			return nil
		}

		parsedNodeIDs = append(parsedNodeIDs, parsedNodeID)
	}

	return parsedNodeIDs
}

func newOPCUAInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	endpoint, err := conf.FieldString("endpoint")
	if err != nil {
		return nil, err
	}

	securityMode, err := conf.FieldString("securityMode")
	if err != nil {
		return nil, err
	}

	securityPolicy, err := conf.FieldString("securityPolicy")
	if err != nil {
		return nil, err
	}

	username, err := conf.FieldString("username")
	if err != nil {
		return nil, err
	}

	password, err := conf.FieldString("password")
	if err != nil {
		return nil, err
	}

	insecure, err := conf.FieldBool("insecure")
	if err != nil {
		return nil, err
	}

	subscribeEnabled, err := conf.FieldBool("subscribeEnabled")
	if err != nil {
		return nil, err
	}

	nodeIDs, err := conf.FieldStringList("nodeIDs")
	if err != nil {
		return nil, err
	}

	sessionTimeout, err := conf.FieldInt("sessionTimeout")
	if err != nil {
		return nil, err
	}

	directConnect, err := conf.FieldBool("directConnect")
	if err != nil {
		return nil, err
	}

	useHeartbeat, err := conf.FieldBool("useHeartbeat")
	if err != nil {
		return nil, err
	}

	// fail if no nodeIDs are provided
	if len(nodeIDs) == 0 {
		return nil, errors.New("no nodeIDs provided")
	}

	parsedNodeIDs := ParseNodeIDs(nodeIDs)

	m := &OPCUAInput{
		Endpoint:                     endpoint,
		Username:                     username,
		Password:                     password,
		NodeIDs:                      parsedNodeIDs,
		Log:                          mgr.Logger(),
		SecurityMode:                 securityMode,
		SecurityPolicy:               securityPolicy,
		Insecure:                     insecure,
		SubscribeEnabled:             subscribeEnabled,
		SessionTimeout:               sessionTimeout,
		DirectConnect:                directConnect,
		UseHeartbeat:                 useHeartbeat,
		LastHeartbeatMessageReceived: atomic.Uint32{},
		LastMessageReceived:          atomic.Uint32{},
		HeartbeatManualSubscribed:    false,
		HeartbeatNodeId:              ua.NewNumericNodeID(0, 2258), // 2258 is the nodeID for CurrentTime, only in tests this is different
	}

	return service.AutoRetryNacksBatched(m), nil
}

func init() {

	err := service.RegisterBatchInput(
		"opcua", OPCUAConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			mgr.Logger().Infof("Created & maintained by the United Manufacturing Hub. About us: www.umh.app")
			return newOPCUAInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type OPCUAInput struct {
	Endpoint       string
	Username       string
	Password       string
	NodeIDs        []*ua.NodeID
	NodeList       []NodeDef
	SecurityMode   string
	SecurityPolicy string
	Insecure       bool
	Client         *opcua.Client
	Log            *service.Logger
	// this is required for subscription
	SubscribeEnabled             bool
	SubNotifyChan                chan *opcua.PublishNotificationData
	SessionTimeout               int
	DirectConnect                bool
	UseHeartbeat                 bool
	LastHeartbeatMessageReceived atomic.Uint32
	LastMessageReceived          atomic.Uint32
	HeartbeatManualSubscribed    bool
	HeartbeatNodeId              *ua.NodeID
	Subscription                 *opcua.Subscription
	ServerInfo                   ServerInfo
}

// Connect establishes a connection to the OPC UA server.
// It handles various configurations such as security policies, authentication methods,
// and endpoint selections. The function attempts to connect using user-specified
// settings first and iterates through available endpoints and security combinations
// if necessary. Upon successful connection, it retrieves server information
// and initiates browsing and subscription of nodes.
func (g *OPCUAInput) Connect(ctx context.Context) (err error) {

	if g.Client != nil {
		return nil
	}

	defer func() {
		if err != nil {
			g.Log.Warnf("Connect failed with %v, waiting 5 seconds before retrying to prevent overloading the server", err)
			time.Sleep(5 * time.Second)
		}
	}()

	err = g.connect(ctx)
	if err != nil {
		return err
	}

	g.Log.Infof("Connected to %s", g.Endpoint)

	// Get OPC UA server information
	serverInfo, err := g.GetOPCUAServerInformation(ctx)
	if err != nil {
		g.Log.Infof("Failed to get OPC UA server information: %s", err)
	} else {
		g.Log.Infof("OPC UA Server Information: %v+", serverInfo)
		g.ServerInfo = serverInfo
	}

	// Create a subscription channel if needed
	if g.SubscribeEnabled {
		g.SubNotifyChan = make(chan *opcua.PublishNotificationData, 10000)
	}
	// Browse and subscribe to the nodes if needed
	// BrowseAndSubscribeIfNeeded has to be run synchronously since the field g.NodeList
	// is set inside this function.And g.NodeList is required to be set mandatorily for further func calls like ReadBatch or ReadBatchSubscribe
	g.Log.Infof("Please note that browsing large node trees can take some time")
	if err := g.BrowseAndSubscribeIfNeeded(ctx); err != nil {
		g.Log.Errorf("Failed to subscribe: %v", err)
		_ = g.Close(ctx)
	}
	// Set the heartbeat after browsing, as browsing might take some time
	g.LastHeartbeatMessageReceived.Store(uint32(time.Now().Unix()))

	return nil
}

// ReadBatch retrieves a batch of messages from the OPC UA server.
// It either subscribes to node updates or performs a pull-based read based on the configuration.
// The function updates heartbeat information and monitors the connection's health.
// If no messages or heartbeats are received within the expected timeframe, it closes the connection.
func (g *OPCUAInput) ReadBatch(ctx context.Context) (msgs service.MessageBatch, ackFunc service.AckFunc, err error) {
	if g.SubscribeEnabled {
		// Wait for maximum 10 seconds for a response from the subscription channel
		// So that this never gets stuck
		ctxSubscribe, cancel := context.WithTimeout(ctx, SubscribeTimeoutContext)
		defer cancel()

		msgs, ackFunc, err = g.ReadBatchSubscribe(ctxSubscribe)

	} else {
		msgs, ackFunc, err = g.ReadBatchPull(ctx)
	}

	// Heartbeat logic
	msgs = g.updateHeartbeatInMessageBatch(msgs)

	// if the last heartbeat message was received more than 10 seconds ago, close the connection
	// benthos will automatically reconnect
	if g.UseHeartbeat && g.LastHeartbeatMessageReceived.Load() < uint32(time.Now().Unix()-10) && g.LastHeartbeatMessageReceived.Load() != 0 {
		if g.LastMessageReceived.Load() < uint32(time.Now().Unix()-10) {
			g.Log.Error("No messages received (including heartbeat) for over 10 seconds. Closing connection.")
			_ = g.Close(ctx)
			return nil, nil, service.ErrNotConnected
		} else {
			if g.ServerInfo.ManufacturerName == "Prosys OPC Ltd." {
				g.Log.Info("No heartbeat message (ServerTime) received for over 10 seconds. This is normal for your Prosys OPC UA server. Other messages are being received; continuing operations. ")
			} else {
				g.Log.Warn("No heartbeat message (ServerTime) received for over 10 seconds. Other messages are being received; continuing operations.")
			}
		}
	}

	// If context deadline exceeded, print it as debug and ignore it. We don't want to show this to the user.
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		g.Log.Debugf("ReadBatch context.DeadlineExceeded")
		return nil, nil, nil
	}

	return
}

// closeRaw closes the OPC UA client and handles subscription cleanup if necessary.
// It performs the closure without logging any high-level messages, allowing
// higher-level functions to manage logging based on context.
func (g *OPCUAInput) closeRaw(ctx context.Context) {
	if g.Client != nil {
		// Unsubscribe from the subscription
		if g.SubscribeEnabled && g.Subscription != nil {
			g.Log.Infof("Unsubscribing from OPC UA subscription...")
			if err := g.Subscription.Cancel(ctx); err != nil {
				g.Log.Infof("Failed to unsubscribe from OPC UA subscription: %v", err)
			}
			g.Subscription = nil
		}

		// Attempt to close the OPC UA client
		if err := g.Client.Close(ctx); err != nil {
			g.Log.Infof("Error closing OPC UA client: %v", err)
		}

		g.Client = nil
	}

	// Reset the heartbeat
	g.LastHeartbeatMessageReceived.Store(uint32(0))
	g.LastMessageReceived.Store(uint32(0))

	return
}

// Close terminates the OPC UA connection and logs the closure process.
// It logs an informational message when starting and successfully closing the client.
// If an error occurs during closure, it logs the error.
func (g *OPCUAInput) Close(ctx context.Context) error {
	g.Log.Errorf("Initiating closure of OPC UA client...")
	g.closeRaw(ctx)
	g.Log.Infof("OPC UA client closed successfully.")

	return nil
}

// CloseExpected terminates the OPC UA connection without logging errors.
// This function is intended for scenarios where closing the client is expected
// and should not be treated as an error (e.g., when attempting multiple client connections).
func (g *OPCUAInput) CloseExpected(ctx context.Context) {
	g.Log.Infof("Initiating expected closure of OPC UA client...")
	g.closeRaw(ctx)
	g.Log.Infof("OPC UA client closed successfully.")

	return
}
