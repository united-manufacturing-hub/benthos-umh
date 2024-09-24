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
const SubscribeTimeoutContext = 3 * time.Second

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
	parsedNodeIDs := make([]*ua.NodeID, len(incomingNodes))

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
func (g *OPCUAInput) Connect(ctx context.Context) error {

	if g.Client != nil {
		return nil
	}

	var c *opcua.Client
	var endpoints []*ua.EndpointDescription
	var err error

	defer func() {
		if err != nil {
			g.Log.Warnf("Connect failed with %v, waiting 5 seconds before retrying to prevent overloading the server", err)
			time.Sleep(5 * time.Second)
		}
	}()

	// Step 1: Retrieve all available endpoints from the OPC UA server
	// Iterate through DiscoveryURLs until we receive a list of all working endpoints including their potential security modes, etc.

	endpoints, err = g.FetchAllEndpoints(ctx)
	if err != nil {
		g.Log.Infof("Failed to fetch any endpoint: %s", err)
		g.Log.Infof("Trying to connect to the endpoint as a last resort measure")
	} else {
		g.Log.Infof("Fetched %d endpoints", len(endpoints))
		// Step 2 (optional): Log details of each discovered endpoint for debugging
		g.LogEndpoints(endpoints)
	}

	// Step 3: Determine the authentication method to use.
	// Default to Anonymous if neither username nor password is provided.
	selectedAuthentication := ua.UserTokenTypeAnonymous
	if g.Username != "" && g.Password != "" {
		// Use UsernamePassword authentication if both username and password are available.
		selectedAuthentication = ua.UserTokenTypeUserName
	}

	// Step 4: Check if the user has specified a very concrete endpoint, security policy, and security mode
	// Connect to this endpoint directly
	// If connection fails, then return an error
	if g.DirectConnect || len(endpoints) == 0 {
		g.Log.Infof("Directly connecting to the endpoint %s", g.Endpoint)

		// Create a new endpoint description
		// It will never be used directly by the OPC UA library, but we need it for our internal helper functions
		// such as GetOPCUAClientOptions
		securityMode := ua.MessageSecurityModeNone
		if g.SecurityMode != "" {
			securityMode = ua.MessageSecurityModeFromString(g.SecurityMode)
		}

		securityPolicyURI := ua.SecurityPolicyURINone
		if g.SecurityPolicy != "" {
			securityPolicyURI = "http://opcfoundation.org/UA/SecurityPolicy#" + g.SecurityPolicy
		}

		directEndpoint := &ua.EndpointDescription{
			EndpointURL:       g.Endpoint,
			SecurityMode:      securityMode,
			SecurityPolicyURI: securityPolicyURI,
		}

		// Prepare authentication and encryption
		opts, err := g.GetOPCUAClientOptions(directEndpoint, selectedAuthentication)
		if err != nil {
			g.Log.Errorf("Failed to get OPC UA client options: %s", err)
			return err
		}

		c, err = opcua.NewClient(directEndpoint.EndpointURL, opts...)
		if err != nil {
			g.Log.Errorf("Failed to create a new client: %v", err)
			return err
		}

		// Connect to the selected endpoint
		if err := c.Connect(ctx); err != nil {
			_ = g.Close(ctx)
			g.Log.Errorf("Failed to connect: %v", err)
			return err
		}

	} else if g.SecurityMode != "" && g.SecurityPolicy != "" {
		foundEndpoint, err := g.getEndpointIfExists(endpoints, selectedAuthentication, g.SecurityMode, g.SecurityPolicy)
		if err != nil {
			g.Log.Errorf("Failed to get endpoint: %s", err)
			return err
		}

		if foundEndpoint == nil {
			g.Log.Errorf("No suitable endpoint found")
			return errors.New("no suitable endpoint found")
		}

		opts, err := g.GetOPCUAClientOptions(foundEndpoint, selectedAuthentication)
		if err != nil {
			g.Log.Errorf("Failed to get OPC UA client options: %s", err)
			return err
		}

		c, err = opcua.NewClient(foundEndpoint.EndpointURL, opts...)
		if err != nil {
			g.Log.Errorf("Failed to create a new client: %v", err)
			return err
		}

		// Connect to the selected endpoint
		if err := c.Connect(ctx); err != nil {
			_ = g.Close(ctx)
			g.Log.Errorf("Failed to connect: %v", err)
			return err
		}

	} else {
		// Step 5: If the user has not specified a very concrete endpoint, security policy, and security mode
		// Iterate through all available endpoints and try to connect to them

		// Order the endpoints based on the expected success of the connection
		orderedEndpoints := g.orderEndpoints(endpoints, selectedAuthentication)

		for _, currentEndpoint := range orderedEndpoints {

			opts, err := g.GetOPCUAClientOptions(currentEndpoint, selectedAuthentication)
			if err != nil {
				g.Log.Errorf("Failed to get OPC UA client options: %s", err)
				return err
			}

			c, err = opcua.NewClient(currentEndpoint.EndpointURL, opts...)
			if err != nil {
				g.Log.Errorf("Failed to create a new client")
				return err
			}

			// Connect to the endpoint
			// If connection fails, then continue to the next endpoint
			// Connect to the selected endpoint
			if err := c.Connect(ctx); err != nil {
				g.CloseExpected(ctx)

				g.Log.Infof("Failed to connect, but continue anyway: %v", err)

				if errors.Is(err, ua.StatusBadUserAccessDenied) || errors.Is(err, ua.StatusBadTooManySessions) {
					var timeout time.Duration
					// Adding a sleep to prevent immediate re-connect
					// In the case of ua.StatusBadUserAccessDenied, the session is for some reason not properly closed on the server
					// If we were to re-connect, we could overload the server with too many sessions as the default timeout is 10 seconds
					if g.SessionTimeout > 0 {
						timeout = time.Duration(g.SessionTimeout * int(time.Millisecond))
					} else {
						timeout = SessionTimeout
					}
					g.Log.Errorf("Encountered unrecoverable error. Waiting before trying to re-connect to prevent overloading the server: %v with timeout %v", err, timeout)
					time.Sleep(timeout)
					return err
				} else if errors.Is(err, ua.StatusBadTimeout) {
					g.Log.Infof("Selected endpoint timed out. Selecting next one: %v", currentEndpoint)
					continue
				}

				continue
			} else {
				break
			}
		}

		// If no connection was successful, return an error
		if c == nil {
			g.Log.Errorf("Failed to connect to any endpoint")
			return errors.New("failed to connect to any endpoint")
		}
	}

	g.Log.Infof("Connected to %s", g.Endpoint)
	g.Client = c

	// Get OPC UA server information
	serverInfo, err := g.GetOPCUAServerInformation(ctx)
	if err != nil {
		g.Log.Infof("Failed to get OPC UA server information: %s", err)
	} else {
		g.Log.Infof("OPC UA Server Information: %v+", serverInfo)
		g.ServerInfo = serverInfo
	}

	// Browse and subscribe to the nodes if needed
	// Do this asynchronously so that the first messages can already arrive
	go func() {
		g.Log.Infof("Please note that browsing large node trees can take some time")
		if err := g.BrowseAndSubscribeIfNeeded(ctx); err != nil {
			g.Log.Errorf("Failed to subscribe: %v", err)
			_ = g.Close(ctx)
		}
		// Set the heartbeat after browsing, as browsing might take some time
		g.LastHeartbeatMessageReceived.Store(uint32(time.Now().Unix()))
	}()

	// Create a subscription channel if needed
	if g.SubscribeEnabled {
		g.SubNotifyChan = make(chan *opcua.PublishNotificationData, 10000)
	}

	return nil
}

// ReadBatch retrieves a batch of messages from the OPC UA server.
// It either subscribes to node updates or performs a pull-based read based on the configuration.
// The function updates heartbeat information and monitors the connection's health.
// If no messages or heartbeats are received within the expected timeframe, it closes the connection.
func (g *OPCUAInput) ReadBatch(ctx context.Context) (msgs service.MessageBatch, ackFunc service.AckFunc, err error) {
	if g.SubscribeEnabled {
		// Wait for maximum 3 seconds for a response from the subscription channel
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
