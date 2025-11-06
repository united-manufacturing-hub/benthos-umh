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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/errors"
	"github.com/gopcua/opcua/ua"
)

const (
	SubscribeTimeoutContext = 3 * time.Second
	DefaultPollRate         = 1000
	DefaultQueueSize        = 10
	DefaultSamplingInterval = 0.0
)

var OPCUAConfigSpec = OPCUAConnectionConfigSpec.
	Summary("OPC UA input plugin").
	Description("The OPC UA input plugin reads data from an OPC UA server and sends it to Benthos.").
	Field(service.NewStringListField("nodeIDs").
		Description("List of OPC-UA node IDs to begin browsing.")).
	Field(service.NewBoolField("subscribeEnabled").
		Description("Set to true to subscribe to OPC UA nodes instead of fetching them every seconds. Default is pulling messages every second (false).").
		Default(false)).
	Field(service.NewBoolField("useHeartbeat").
		Description("Set to true to provide an extra message with the servers timestamp as a heartbeat").
		Default(false)).
	Field(service.NewIntField("pollRate").
		Description("The rate in milliseconds at which to poll the OPC UA server when not using subscriptions. Defaults to 1000ms (1 second).").
		Default(DefaultPollRate)).
	Field(service.NewIntField("queueSize").
		Description("The size of the queue, which will get filled from the OPC UA server when requesting its data via subscription").Default(DefaultQueueSize)).
	Field(service.NewFloatField("samplingInterval").Description("The interval for sampling on the OPC UA server - notice 0.0 will get you updates as fast as possible").Default(DefaultSamplingInterval))

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
	// Parse the shared connection configuration
	conn, err := ParseConnectionConfig(conf, mgr)
	if err != nil {
		return nil, err
	}

	// Parse input-specific fields
	nodeIDs, err := conf.FieldStringList("nodeIDs")
	if err != nil {
		return nil, err
	}

	subscribeEnabled, err := conf.FieldBool("subscribeEnabled")
	if err != nil {
		return nil, err
	}

	useHeartbeat, err := conf.FieldBool("useHeartbeat")
	if err != nil {
		return nil, err
	}

	pollRate, err := conf.FieldInt("pollRate")
	if err != nil {
		return nil, err
	}

	queueSize, err := conf.FieldInt("queueSize")
	if err != nil {
		return nil, err
	}

	samplingInterval, err := conf.FieldFloat("samplingInterval")
	if err != nil {
		return nil, err
	}

	// Deadband configuration: Hardcoded for duplicate suppression
	// Product decision: Users configure thresholds in UMH downsampler, not OPC UA layer
	// threshold=0 suppresses only exact duplicate values (e.g., 123.0 → 123.0)
	const deadbandType = "absolute"
	const deadbandValue = 0.0

	// fail if no nodeIDs are provided
	if len(nodeIDs) == 0 {
		return nil, errors.New("no nodeIDs provided")
	}

	parsedNodeIDs := ParseNodeIDs(nodeIDs)

	m := &OPCUAInput{
		OPCUAConnection:              conn,
		NodeIDs:                      parsedNodeIDs,
		SubscribeEnabled:             subscribeEnabled,
		UseHeartbeat:                 useHeartbeat,
		LastHeartbeatMessageReceived: atomic.Uint32{},
		LastMessageReceived:          atomic.Uint32{},
		HeartbeatManualSubscribed:    false,
		HeartbeatNodeId:              ua.NewNumericNodeID(0, 2258), // 2258 is the nodeID for CurrentTime, only in tests this is different
		PollRate:                     pollRate,
		QueueSize:                    uint32(queueSize),
		SamplingInterval:             samplingInterval,
		DeadbandType:                 deadbandType,
		DeadbandValue:                deadbandValue,
	}

	m.cleanup_func = func(ctx context.Context) {
		m.unsubscribeAndResetHeartbeat(ctx)
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

// ServerCapabilities holds OPC UA server capability flags
// ServerCapabilities represents OPC UA server capabilities including operation limits and feature support.
// Operation limits come from ns=0;i=11704 (OperationLimits) - many servers (S7-1200/1500) don't expose these.
// Deadband support is queried separately via AggregateConfiguration node.
type ServerCapabilities struct {
	// Operation limits from ServerCapabilities.OperationLimits
	MaxNodesPerBrowse           uint32
	MaxMonitoredItemsPerCall    uint32
	MaxNodesPerRead             uint32
	MaxNodesPerWrite            uint32
	MaxBrowseContinuationPoints uint32

	// Feature support flags
	SupportsPercentDeadband  bool
	SupportsAbsoluteDeadband bool
}

type OPCUAInput struct {
	*OPCUAConnection // Embed the shared connection configuration

	// Input-specific fields
	NodeIDs                      []*ua.NodeID
	NodeList                     []NodeDef
	SubscribeEnabled             bool
	SubNotifyChan                chan *opcua.PublishNotificationData
	UseHeartbeat                 bool
	LastHeartbeatMessageReceived atomic.Uint32
	LastMessageReceived          atomic.Uint32
	HeartbeatManualSubscribed    bool
	HeartbeatNodeId              *ua.NodeID
	Subscription                 *opcua.Subscription
	ServerInfo                   ServerInfo
	ServerProfile                ServerProfile
	PollRate                     int
	QueueSize                    uint32
	SamplingInterval             float64
	DeadbandType                 string
	DeadbandValue                float64
	ServerCapabilities           *ServerCapabilities
}

// unsubscribeAndResetHeartbeat unsubscribes from the OPC UA subscription and resets the heartbeat
// It is used as a cleanup function for the OPC UA connection
func (g *OPCUAInput) unsubscribeAndResetHeartbeat(ctx context.Context) {
	if g.SubscribeEnabled && g.Subscription != nil {
		g.Log.Infof("Unsubscribing from OPC UA subscription...")
		if err := g.Subscription.Cancel(ctx); err != nil {
			g.Log.Infof("Failed to unsubscribe from OPC UA subscription: %v", err)
		}
		g.Subscription = nil
	}

	g.LastHeartbeatMessageReceived.Store(uint32(0))
	g.LastMessageReceived.Store(uint32(0))
}

// startBrowsing initiates the browsing process and handles errors
func (g *OPCUAInput) startBrowsing(ctx context.Context) {
	browseCtx, cancel := context.WithCancel(ctx)
	g.browseCancel = cancel
	g.browseWaitGroup.Add(1)

	go func() {
		defer g.browseWaitGroup.Done()
		g.Log.Infof("Please note that browsing large node trees can take some time")

		if err := g.BrowseAndSubscribeIfNeeded(browseCtx); err != nil {
			g.Log.Errorf("Failed to subscribe: %v", err)
			g.Close(ctx) // Safe to call Close here as we're in a separate goroutine
			return
		}

		g.LastHeartbeatMessageReceived.Store(uint32(time.Now().Unix()))
	}()
}

// Connect establishes a connection to the OPC UA server
func (g *OPCUAInput) Connect(ctx context.Context) error {
	var err error

	if g.Client != nil {
		return nil
	}

	defer func() {
		if err != nil {
			g.Log.Warnf("Connect failed with %v, waiting 5 seconds before retrying to prevent overloading the server", err)
			time.Sleep(5 * time.Second)
		}
	}()

	if err = g.connect(ctx); err != nil {
		return err
	}

	g.Log.Infof("Connected to %s", g.Endpoint)

	// Get OPC UA server information
	if serverInfo, err := g.GetOPCUAServerInformation(ctx); err != nil {
		g.Log.Infof("Failed to get OPC UA server information: %s", err)
	} else {
		g.Log.Infof("OPC UA Server Information: %v+", serverInfo)
		g.ServerInfo = serverInfo

		// Detect and store server profile
		g.ServerProfile = DetectServerProfile(&g.ServerInfo)
		g.Log.With("profile", g.ServerProfile.Name).
			With("manufacturer", g.ServerInfo.ManufacturerName).
			With("product", g.ServerInfo.ProductName).
			Info("Detected OPC UA server profile")
	}

	// Query server capabilities for deadband support (only if subscriptions enabled)
	if g.SubscribeEnabled {
		caps, err := g.queryServerCapabilities(ctx)
		if err != nil {
			g.Log.Warnf("Failed to query server capabilities: %v, assuming basic support", err)
			caps = &ServerCapabilities{
				SupportsAbsoluteDeadband: true,  // Most servers support this
				SupportsPercentDeadband:  false, // Conservative assumption
			}
		}
		g.ServerCapabilities = caps

		// Log operation limits (Phase 1: logging only, no behavior changes)
		g.logServerCapabilities(caps)

		// Adjust deadband type based on server capabilities
		originalType := g.DeadbandType
		g.DeadbandType = adjustDeadbandType(g.DeadbandType, caps)
		if g.DeadbandType != originalType {
			g.Log.Warnf("Server does not support %s deadband, falling back to %s",
				originalType, g.DeadbandType)
		}
	}

	// Create a subscription channel if needed
	if g.SubscribeEnabled {
		g.SubNotifyChan = make(chan *opcua.PublishNotificationData, MaxTagsToBrowse)
	}

	g.startBrowsing(ctx)
	return nil
}

// ReadBatch retrieves a batch of messages from the OPC UA server.
// It either subscribes to node updates or performs a pull-based read based on the configuration.
// The function updates heartbeat information and monitors the connection's health.
// If no messages or heartbeats are received within the expected timeframe, it closes the connection.
func (g *OPCUAInput) ReadBatch(ctx context.Context) (msgs service.MessageBatch, ackFunc service.AckFunc, err error) {
	if len(g.NodeList) == 0 {
		g.Log.Debug("ReadBatch is called with empty nodelists. returning early from ReadBatch")
		return nil, nil, nil
	}

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

	return msgs, ackFunc, err
}

// ReadBatchPull performs a batch read of all OPC UA nodes in the NodeList using a pull method.
//
// This function constructs a ReadRequest encompassing all nodes to be read and sends it to the OPC UA server.
// It processes the response by converting each DataValue into a Benthos message using `createMessageFromValue`.
// The function introduces a brief pause before returning the messages to ensure system stability.
func (g *OPCUAInput) ReadBatchPull(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if g.Client == nil {
		return nil, nil, errors.New("client is nil")
	}

	// Add validation for PollRate
	// benthos will set a default value if not set, and this is the fallback option for tests
	if g.PollRate <= 0 {
		g.PollRate = DefaultPollRate
	}

	// Read all values in NodeList and return each of them as a message with the node's path as the metadata

	// Create first a list of all the values to read
	var nodesToRead []*ua.ReadValueID

	for _, node := range g.NodeList {
		nodesToRead = append(nodesToRead, &ua.ReadValueID{
			NodeID: node.NodeID,
		})
	}

	if len(g.NodeList) > 100 {
		g.Log.Warnf("Reading more than 100 nodes with pull method. The request might fail as it can take too much time. Recommendation: use subscribeEnabled: true instead for better performance")
	}

	req := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := g.Read(ctx, req)
	if err != nil {
		g.Log.Errorf("Read failed: %s", err)
		return nil, nil, err
	}

	// Add nil check for response
	if resp == nil {
		g.Log.Errorf("Received nil response from Read")
		return nil, nil, errors.New("received nil response from Read")
	}

	// Add nil check for Results
	if resp.Results == nil {
		g.Log.Errorf("Received nil Results in response")
		return nil, nil, errors.New("received nil Results in response")
	}

	// Check if Results length matches NodeList length
	if len(resp.Results) != len(g.NodeList) {
		g.Log.Errorf("Results length (%d) does not match NodeList length (%d)", len(resp.Results), len(g.NodeList))
		return nil, nil, fmt.Errorf("results length (%d) does not match NodeList length (%d)", len(resp.Results), len(g.NodeList))
	}

	// Create a message with the node's path as the metadata
	msgs := service.MessageBatch{}

	for i, node := range g.NodeList {
		// Check if index is within bounds (redundant after length check, but good practice)
		if i >= len(resp.Results) {
			g.Log.Errorf("Index %d out of bounds for Results length %d", i, len(resp.Results))
			continue
		}

		value := resp.Results[i]
		if value == nil || value.Value == nil {
			g.Log.Debugf("Received nil in item structure on node %s. This can occur when subscribing to an OPC UA folder and may be ignored.", node.NodeID.String())
			continue
		}
		message := g.createMessageFromValue(value, node)
		if message != nil {
			msgs = append(msgs, message)
		}
	}

	// Wait for the configured poll rate before returning a message.
	time.Sleep(time.Duration(g.PollRate) * time.Millisecond)

	return msgs, func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

// ReadBatchSubscribe handles batch reads of OPC UA nodes using the subscription mechanism.
//
// This function listens for subscription notifications on `SubNotifyChan`. Upon receiving data changes,
// it converts each monitored item's value into a Benthos message using `createMessageFromValue`. It also
// manages context cancellations and timeouts, ensuring that subscription operations are gracefully
// terminated when needed.
func (g *OPCUAInput) ReadBatchSubscribe(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	var res *opcua.PublishNotificationData

	if ctx == nil || ctx.Done() == nil {
		return nil, nil, errors.New("emptyCtx is invalid for ReadBatchSubscribe")
	}
	select {
	case res = <-g.SubNotifyChan:
		// Received a result, check for error
		if res.Error != nil {
			g.Log.Errorf("ReadBatchSubscribe error: %s", res.Error)
			return nil, nil, res.Error
		}

		if g.NodeList == nil {
			g.Log.Errorf("nodelist is nil")
			return nil, nil, errors.New("nodelist empty")
		}

		// Create a message with the node's path as the metadata
		msgs := service.MessageBatch{}

		switch x := res.Value.(type) {
		case *ua.DataChangeNotification:
			for _, item := range x.MonitoredItems {
				if item == nil || item.Value == nil || item.Value.Value == nil {
					g.Log.Debugf("Received nil in item structure. This can occur when subscribing to an OPC UA folder and may be ignored.")
					continue
				}

				// now get the handle id, which is the position in g.Nodelist
				// see also NewMonitoredItemCreateRequestWithDefaults call in other functions
				handleID := item.ClientHandle

				if uint32(len(g.NodeList)) >= handleID {
					message := g.createMessageFromValue(item.Value, g.NodeList[handleID])
					if message != nil {
						msgs = append(msgs, message)
					}
				}
			}
		default:
			g.Log.Errorf("Unknown publish result %T", res.Value)
		}

		return msgs, func(ctx context.Context, err error) error {
			// Nacks are retried automatically when we use service.AutoRetryNacks
			return nil
		}, nil

	case <-ctx.Done():
		// Check why the context was done
		err := ctx.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			g.Log.Warnf("Subscribe timeout: this will happen if the server does not send any data updates within %v", SubscribeTimeoutContext)
		} else if errors.Is(err, context.Canceled) {
			g.Log.Warnf("Subscribe canceled: operation was manually canceled")
		} else {
			g.Log.Warnf("Subscribe stopped due to context error: %v", err)
		}
		return nil, nil, err
	}
}

// createMessageFromValue constructs a Benthos message from a given OPC UA DataValue and NodeDef.
//
// This function translates the OPC UA DataValue into a format suitable for Benthos, embedding relevant
// metadata derived from the NodeDef. It handles various data types, ensuring that the message payload
// accurately represents the OPC UA node's current value. Additionally, it marks heartbeat messages
// when applicable, facilitating heartbeat monitoring within the system.
func (g *OPCUAInput) createMessageFromValue(dataValue *ua.DataValue, nodeDef NodeDef) *service.Message {
	b, tagType := g.getBytesFromValue(dataValue, nodeDef)
	message := service.NewMessage(b)

	// New ones
	message.MetaSet("opcua_source_timestamp", dataValue.SourceTimestamp.Format("2006-01-02T15:04:05.000000Z07:00"))
	message.MetaSet("opcua_server_timestamp", dataValue.ServerTimestamp.Format("2006-01-02T15:04:05.000000Z07:00"))
	message.MetaSet("opcua_attr_nodeid", nodeDef.NodeID.String())
	message.MetaSet("opcua_attr_nodeclass", nodeDef.NodeClass.String())
	message.MetaSet("opcua_attr_browsename", nodeDef.BrowseName)
	message.MetaSet("opcua_attr_description", nodeDef.Description)
	message.MetaSet("opcua_attr_accesslevel", nodeDef.AccessLevel.String())
	message.MetaSet("opcua_attr_datatype", nodeDef.DataType)
	message.MetaSet("opcua_attr_statuscode", dataValue.Status.Error())

	tagName := sanitize(nodeDef.BrowseName)

	// Tag Group
	tagGroup := nodeDef.Path
	// remove nodeDef.BrowseName from tagGroup
	tagGroup = strings.Replace(tagGroup, nodeDef.BrowseName, "", 1)
	// remove trailing dot
	tagGroup = strings.TrimSuffix(tagGroup, ".")

	// if the node is the CurrentTime node, mark is as a heartbeat message
	if g.HeartbeatNodeId != nil && nodeDef.NodeID.Namespace() == g.HeartbeatNodeId.Namespace() && nodeDef.NodeID.IntID() == g.HeartbeatNodeId.IntID() && g.UseHeartbeat {
		message.MetaSet("opcua_heartbeat_message", "true")
	}

	if tagGroup == "" {
		tagGroup = tagName
	}

	message.MetaSet("opcua_tag_group", tagGroup)
	message.MetaSet("opcua_tag_name", tagName)

	// if the tag group is the same as the tag name ("root"), we don't want to have a tag path
	if tagGroup == tagName {
		message.MetaSet("opcua_tag_path", "")
	} else {
		message.MetaSet("opcua_tag_path", tagGroup)
	}

	message.MetaSet("opcua_tag_type", tagType)

	return message
}

// queryServerCapabilities reads server capability information
// to determine which deadband types are supported.
func (o *OPCUAInput) queryServerCapabilities(ctx context.Context) (*ServerCapabilities, error) {
	caps := &ServerCapabilities{
		SupportsAbsoluteDeadband: true, // All OPC UA servers support absolute
	}

	// Query deadband support via AggregateConfiguration
	// NodeID 2340 = Server_ServerCapabilities_AggregateConfiguration (OPC UA Part 5)
	aggConfigNodeID := ua.NewNumericNodeID(0, 2340)
	req := &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{
			{NodeID: aggConfigNodeID, AttributeID: ua.AttributeIDNodeClass},
		},
	}

	resp, err := o.Client.Read(ctx, req)
	if err == nil && len(resp.Results) > 0 && resp.Results[0].Status == ua.StatusOK {
		caps.SupportsPercentDeadband = true
	}

	// Query operation limits (Phase 1: logging only)
	if opLimits, err := o.queryOperationLimits(ctx); err != nil {
		o.Log.Debugf("OperationLimits not available: %s (this is normal for many PLCs)", err)
	} else if opLimits != nil {
		// Merge operation limits into capabilities
		caps.MaxNodesPerBrowse = opLimits.MaxNodesPerBrowse
		caps.MaxMonitoredItemsPerCall = opLimits.MaxMonitoredItemsPerCall
		caps.MaxNodesPerRead = opLimits.MaxNodesPerRead
		caps.MaxNodesPerWrite = opLimits.MaxNodesPerWrite
		caps.MaxBrowseContinuationPoints = opLimits.MaxBrowseContinuationPoints
	}

	return caps, nil
}

// adjustDeadbandType adjusts requested deadband type based on server capabilities.
// Implements fallback strategy: percent → absolute → none
func adjustDeadbandType(requested string, caps *ServerCapabilities) string {
	switch requested {
	case "none":
		return "none"

	case "absolute":
		if caps.SupportsAbsoluteDeadband {
			return "absolute"
		}
		// Absolute not supported - fallback to none
		return "none"

	case "percent":
		if caps.SupportsPercentDeadband {
			return "percent"
		}
		// Percent not supported - fallback to absolute
		if caps.SupportsAbsoluteDeadband {
			return "absolute"
		}
		// Neither supported - fallback to none
		return "none"

	default:
		return "none"
	}
}
