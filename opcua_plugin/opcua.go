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
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"golang.org/x/exp/slices"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/errors"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

const SessionTimeout = 5 * time.Second

const SubscribeTimeoutContext = 3 * time.Second

type NodeDef struct {
	NodeID       *ua.NodeID
	NodeClass    ua.NodeClass
	BrowseName   string
	Description  string
	AccessLevel  ua.AccessLevelType
	DataType     string
	ParentNodeID string // custom, not an official opcua attribute
	Path         string // custom, not an official opcua attribute
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

func sanitize(s string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	return re.ReplaceAllString(s, "_")
}

func browse(ctx context.Context, n *opcua.Node, path string, level int, logger *service.Logger, parentNodeId string, nodeChan chan NodeDef, errChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()

	logger.Debugf("node:%s path:%q level:%d parentNodeId:%s\n", n, path, level, parentNodeId)
	if level > 10 {
		return
	}

	attrs, err := n.Attributes(ctx, ua.AttributeIDNodeClass, ua.AttributeIDBrowseName, ua.AttributeIDDescription, ua.AttributeIDAccessLevel, ua.AttributeIDDataType)
	if err != nil {
		errChan <- err
		return
	}

	browseName, err := n.BrowseName(ctx)
	if err != nil {
		errChan <- err
		return
	}

	var newPath string
	if path == "" {
		newPath = sanitize(browseName.Name)
	} else {
		newPath = path + "." + sanitize(browseName.Name)
	}

	var def = NodeDef{
		NodeID: n.ID,
		Path:   newPath,
	}

	switch err := attrs[0].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[0].Value == nil {
			errChan <- errors.New("node class is nil")
			return
		} else {
			def.NodeClass = ua.NodeClass(attrs[0].Value.Int())
		}
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	default:
		errChan <- err
		return
	}

	switch err := attrs[1].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[1].Value == nil {
			errChan <- errors.New("browse name is nil")
			return
		} else {
			def.BrowseName = attrs[1].Value.String()
		}
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	default:
		errChan <- err
		return
	}

	switch err := attrs[2].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[2].Value == nil {
			def.Description = "" // this can happen for example in Kepware v6, where the description is OPCUAType_Null
		} else {
			def.Description = attrs[2].Value.String()
		}
	case errors.Is(err, ua.StatusBadAttributeIDInvalid):
		// ignore
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	default:
		errChan <- err
		return
	}

	switch err := attrs[3].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[3].Value == nil {
			errChan <- errors.New("access level is nil")
			return
		} else {
			def.AccessLevel = ua.AccessLevelType(attrs[3].Value.Int())
		}
	case errors.Is(err, ua.StatusBadAttributeIDInvalid):
		// ignore
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	default:
		errChan <- err
		return
	}

	switch err := attrs[4].Status; {
	case errors.Is(err, ua.StatusOK):
		if attrs[4].Value == nil {
			errChan <- errors.New("data type is nil")
			return
		} else {
			switch v := attrs[4].Value.NodeID().IntID(); v {
			case id.DateTime:
				def.DataType = "time.Time"
			case id.Boolean:
				def.DataType = "bool"
			case id.SByte:
				def.DataType = "int8"
			case id.Int16:
				def.DataType = "int16"
			case id.Int32:
				def.DataType = "int32"
			case id.Byte:
				def.DataType = "byte"
			case id.UInt16:
				def.DataType = "uint16"
			case id.UInt32:
				def.DataType = "uint32"
			case id.UtcTime:
				def.DataType = "time.Time"
			case id.String:
				def.DataType = "string"
			case id.Float:
				def.DataType = "float32"
			case id.Double:
				def.DataType = "float64"
			default:
				def.DataType = attrs[4].Value.NodeID().String()
			}
		}

	case errors.Is(err, ua.StatusBadAttributeIDInvalid):
		// ignore
	case errors.Is(err, ua.StatusBadSecurityModeInsufficient):
		return
	default:
		errChan <- err
		return
	}

	logger.Debugf("%d: def.Path:%s def.NodeClass:%s\n", level, def.Path, def.NodeClass)
	def.ParentNodeID = parentNodeId

	hasNodeReferencedComponents := func() bool {
		refs, err := n.ReferencedNodes(ctx, id.HasComponent, ua.BrowseDirectionForward, ua.NodeClassAll, true)
		if err != nil || len(refs) == 0 {
			return false
		}
		return true
	}

	browseChildren := func(refType uint32) error {
		refs, err := n.ReferencedNodes(ctx, refType, ua.BrowseDirectionForward, ua.NodeClassAll, true)
		if err != nil {
			return errors.Errorf("References: %d: %s", refType, err)
		}
		logger.Debugf("found %d child refs\n", len(refs))
		for _, rn := range refs {
			wg.Add(1)
			go browse(ctx, rn, def.Path, level+1, logger, parentNodeId, nodeChan, errChan, wg)
		}
		return nil
	}

	// If a node has a Variable class, it probably means that it is a tag
	// Normally, there is no need to browse further. However, structs will be a variable on the top level,
	// but it then will have HasComponent references to its children
	if def.NodeClass == ua.NodeClassVariable {

		if hasNodeReferencedComponents() {
			if err := browseChildren(id.HasComponent); err != nil {
				errChan <- err
				return
			}
			return
		}

		def.Path = join(path, def.BrowseName)
		nodeChan <- def
		return
	}

	// If a node has an Object class, it probably means that it is a folder
	// Therefore, browse its children
	if def.NodeClass == ua.NodeClassObject {
		// To determine if an Object is a folder, we need to check different references
		// Add here all references that should be checked

		if err := browseChildren(id.HasComponent); err != nil {
			errChan <- err
			return
		}
		if err := browseChildren(id.Organizes); err != nil {
			errChan <- err
			return
		}
		if err := browseChildren(id.FolderType); err != nil {
			errChan <- err
			return
		}
		if err := browseChildren(id.HasNotifier); err != nil {
			errChan <- err
			return
		}
		// For hasProperty it makes sense to show it very close to the tag itself, e.g., use the tagName as tagGroup and then the properties as subparts of it
		/*
			if err := browseChildren(id.HasProperty); err != nil {
				return nil, err
			}
		*/
	}
	return
}

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

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

// UpdateNodePaths updates the node paths to use the nodeID instead of the browseName
// if the browseName is not unique
func UpdateNodePaths(nodes []NodeDef) {
	for i, node := range nodes {
		for j, otherNode := range nodes {
			if i == j {
				continue
			}
			if node.Path == otherNode.Path {
				// update only the last element of the path, after the last dot
				nodePathSplit := strings.Split(node.Path, ".")
				nodePath := strings.Join(nodePathSplit[:len(nodePathSplit)-1], ".")
				nodePath = nodePath + "." + sanitize(node.NodeID.String())
				nodes[i].Path = nodePath

				otherNodePathSplit := strings.Split(otherNode.Path, ".")
				otherNodePath := strings.Join(otherNodePathSplit[:len(otherNodePathSplit)-1], ".")
				otherNodePath = otherNodePath + "." + sanitize(otherNode.NodeID.String())
				nodes[j].Path = otherNodePath
			}
		}
	}
}

// createMessageFromValue creates a benthos messages from a given variant and nodeID
// theoretically nodeID can be extracted from variant, but not in all cases (e.g., when subscribing), so it is left to the calling function
func (g *OPCUAInput) createMessageFromValue(dataValue *ua.DataValue, nodeDef NodeDef) *service.Message {
	variant := dataValue.Value
	if variant == nil {
		g.Log.Errorf("Variant is nil")
		return nil
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
			return nil
		}
		b = append(b, jsonBytes...)
		tagType = "string"
	}

	if b == nil {
		g.Log.Errorf("Could not create benthos message as payload is empty for node %s: %v", nodeDef.NodeID.String(), b)
		return nil
	}

	message := service.NewMessage(b)

	// Deprecated
	message.MetaSet("opcua_path", sanitize(nodeDef.NodeID.String()))
	message.MetaSet("opcua_tag_path", sanitize(nodeDef.BrowseName))
	message.MetaSet("opcua_parent_path", sanitize(nodeDef.ParentNodeID))

	// New ones
	message.MetaSet("opcua_source_timestamp", dataValue.SourceTimestamp.Format("2006-01-02T15:04:05.000000Z07:00"))
	message.MetaSet("opcua_server_timestamp", dataValue.ServerTimestamp.Format("2006-01-02T15:04:05.000000Z07:00"))
	message.MetaSet("opcua_attr_nodeid", nodeDef.NodeID.String())
	message.MetaSet("opcua_attr_nodeclass", nodeDef.NodeClass.String())
	message.MetaSet("opcua_attr_browsename", nodeDef.BrowseName)
	message.MetaSet("opcua_attr_description", nodeDef.Description)
	message.MetaSet("opcua_attr_accesslevel", nodeDef.AccessLevel.String())
	message.MetaSet("opcua_attr_datatype", nodeDef.DataType)

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

	message.MetaSet("opcua_tag_type", tagType)

	return message
}

func (g *OPCUAInput) Read(ctx context.Context, req *ua.ReadRequest) (*ua.ReadResponse, error) {
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
		g.Log.Errorf("Status not OK: %v", resp.Results[0].Status)
		return nil, fmt.Errorf("status not OK: %v", resp.Results[0].Status)
	}

	return resp, nil
}

func (g *OPCUAInput) ReadBatchPull(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if g.Client == nil {
		return nil, nil, errors.New("client is nil")
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
	}

	// Create a message with the node's path as the metadata
	msgs := service.MessageBatch{}

	for i, node := range g.NodeList {
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

	// Wait for a second before returning a message.
	time.Sleep(time.Second)

	return msgs, func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

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
	if g.UseHeartbeat && g.LastHeartbeatMessageReceived.Load() < uint32(time.Now().Unix()-10) {
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

// updateHeartbeatInMessageBatch processes the heartbeat message in a batch of messages.
// If UseHeartbeat is enabled, it searches for a heartbeat message in the batch.
// When found, it updates the LastHeartbeatMessageReceived timestamp.
// If HeartbeatManualSubscribed is true (so if the user specifically subscribed to the heartbeat node), it duplicates the heartbeat message and appends it to the batch.
// Otherwise, it renames the existing heartbeat message.
// The function returns the modified message batch.
func (g *OPCUAInput) updateHeartbeatInMessageBatch(msgs service.MessageBatch) service.MessageBatch {
	if !g.UseHeartbeat {
		return msgs
	}

	if len(msgs) != 0 {
		g.LastMessageReceived.Store(uint32(time.Now().Unix()))
	}

	idx := slices.IndexFunc(msgs, func(msg *service.Message) bool {
		_, exists := msg.MetaGet("opcua_heartbeat_message")
		return exists
	})

	if idx == -1 {
		return msgs
	}

	g.LastHeartbeatMessageReceived.Store(uint32(time.Now().Unix()))

	if g.HeartbeatManualSubscribed {
		g.Log.Debugf("Got heartbeat message. Duplicating it to a new message.")
		newMsg := msgs[idx].DeepCopy()
		newMsg.MetaSet("opcua_tag_group", "heartbeat")
		newMsg.MetaSet("opcua_tag_name", "CurrentTime")
		newMsg.MetaSet("opcua_heartbeat_message", "")
		return append(msgs, newMsg)
	} else {
		g.Log.Debugf("Got heartbeat message. Renaming it.")
		msgs[idx].MetaSet("opcua_tag_group", "heartbeat")
		msgs[idx].MetaSet("opcua_tag_name", "CurrentTime")
		msgs[idx].MetaSet("opcua_heartbeat_message", "")
		return msgs
	}
}

func (g *OPCUAInput) Close(ctx context.Context) error {
	g.Log.Errorf("Closing OPC UA client...")
	if g.Client != nil {
		// Unsubscribe from the subscription
		if g.SubscribeEnabled && g.Subscription != nil {
			g.Log.Infof("Unsubscribing from subscription...")
			if err := g.Subscription.Cancel(ctx); err != nil {
				g.Log.Errorf("Failed to unsubscribe from subscription: %v", err)
			}
			g.Subscription = nil
		}

		_ = g.Client.Close(ctx)
		g.Client = nil
	}
	g.Log.Infof("OPC UA client closed!")

	return nil
}

func (g *OPCUAInput) logCertificateInfo(certBytes []byte) {
	g.Log.Infof("  Server certificate:")

	// Decode the certificate from base64 to DER format
	block, _ := pem.Decode(certBytes)
	if block == nil {
		g.Log.Errorf("Failed to decode certificate")
		return
	}

	// Parse the DER-format certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		g.Log.Errorf("Failed to parse certificate: " + err.Error())
		return
	}

	// Log the details
	g.Log.Infof("    Not Before: %v", cert.NotBefore)
	g.Log.Infof("    Not After: %v", cert.NotAfter)
	g.Log.Infof("    DNS Names: %v", cert.DNSNames)
	g.Log.Infof("    IP Addresses: %v", cert.IPAddresses)
	g.Log.Infof("    URIs: %v", cert.URIs)
}

// Connect connects to the OPC UA server
// Because there are a lot of methods, security policies, potential endpoints, etc., this function is using the following behaviour
// If the user selected a very concrete endpoint, security policy, and security mode, it will try to connect to this endpoint.
// Otherwise, it will iterate until it finds a working combination
// At the end, it will output the recommended connection details ala
// "We have iterated various combinations, and found this one here working. To speed it up, please speicfy in your configuration"
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
		g.Log.Warnf("Failed to fetch any endpoint: %s", err)
		g.Log.Warnf("Trying to connect to the endpoint as a last resort measure")
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
				_ = g.Close(ctx)

				g.Log.Infof("Failed to connect" + err.Error())

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
					g.Log.Warnf("Selected endpoint timed out. Selecting next one: %v", currentEndpoint)
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
		g.Log.Warnf("Failed to get OPC UA server information: %s", err)
	} else {
		g.Log.Infof("OPC UA Server Information: %v+", serverInfo)
		g.ServerInfo = serverInfo
	}

	g.Log.Infof("Please note that browsing large node trees can take some time")

	// Browse and subscribe to the nodes if needed
	if err := g.BrowseAndSubscribeIfNeeded(ctx); err != nil {
		return err
	}

	// Set the heartbeat after browsing, as browsing might take some time
	g.LastHeartbeatMessageReceived.Store(uint32(time.Now().Unix()))
	return nil
}

type ServerInfo struct {
	ManufacturerName string
	ProductName      string
	SoftwareVersion  string
}

// GetOPCUAServerInformation retrieves the server information from the OPC UA server
// It is available as i=2295
func (g *OPCUAInput) GetOPCUAServerInformation(ctx context.Context) (ServerInfo, error) {

	if g.Client == nil {
		return ServerInfo{}, errors.New("client is nil")
	}
	// Fetch ManufacturerName node from i=2263
	manufacturerNameNodeID := ua.NewNumericNodeID(0, 2263)
	productNameNodeID := ua.NewNumericNodeID(0, 2261)
	softwareVersionNodeID := ua.NewNumericNodeID(0, 2264)

	nodeChan := make(chan NodeDef, 3)
	var wg sync.WaitGroup
	errChan := make(chan error, 3)

	wg.Add(3)
	go browse(ctx, g.Client.Node(manufacturerNameNodeID), "", 0, g.Log, manufacturerNameNodeID.String(), nodeChan, errChan, &wg)
	go browse(ctx, g.Client.Node(productNameNodeID), "", 0, g.Log, productNameNodeID.String(), nodeChan, errChan, &wg)
	go browse(ctx, g.Client.Node(softwareVersionNodeID), "", 0, g.Log, softwareVersionNodeID.String(), nodeChan, errChan, &wg)
	wg.Wait()

	close(nodeChan)
	close(errChan)

	if len(errChan) > 0 {
		return ServerInfo{}, <-errChan
	}

	var nodeList []NodeDef
	for node := range nodeChan {
		nodeList = append(nodeList, node)
	}

	if len(nodeList) != 3 {
		g.Log.Warn("Could not find OPC UA Server Information")
		return ServerInfo{}, errors.New("could not find OPC UA Server Information")
	}

	var nodesToRead []*ua.ReadValueID
	for _, node := range nodeList {
		nodesToRead = append(nodesToRead, &ua.ReadValueID{
			NodeID: node.NodeID,
		})
	}

	req := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := g.Read(ctx, req)
	if err != nil {
		g.Log.Errorf("Read failed: %s", err)
		return ServerInfo{}, err
	}

	if len(resp.Results) != 3 {
		g.Log.Errorf("Expected 3 results, got %d", len(resp.Results))
		return ServerInfo{}, errors.New("expected 3 results")
	}

	serverInfo := ServerInfo{}

	for i, node := range nodeList {
		value := resp.Results[i]
		if value == nil || value.Value == nil {
			g.Log.Debugf("Received nil in item structure for OPC UA Server Information")
		}

		message := g.createMessageFromValue(value, node)
		if message != nil {
			messageBytes, err := message.AsBytes()
			if err != nil {
				return ServerInfo{}, err
			}

			if node.NodeID.IntID() == 2263 {
				serverInfo.ManufacturerName = string(messageBytes)
			} else if node.NodeID.IntID() == 2261 {
				serverInfo.ProductName = string(messageBytes)
			} else if node.NodeID.IntID() == 2264 {
				serverInfo.SoftwareVersion = string(messageBytes)
			}
		}
	}
	return serverInfo, nil
}

func (g *OPCUAInput) BrowseAndSubscribeIfNeeded(ctx context.Context) error {

	// Create a slice to store the detected nodes
	nodeList := make([]NodeDef, 0)

	// Create a channel to store the detected nodes
	nodeChan := make(chan NodeDef, 100_000)

	// Create a WaitGroup to synchronize goroutines
	var wg sync.WaitGroup

	// For collecting errors from goroutines
	errChan := make(chan error, len(g.NodeIDs))

	// Start goroutines for each nodeID
	for _, nodeID := range g.NodeIDs {
		if nodeID == nil {
			continue
		}

		// Log the nodeID being browsed
		g.Log.Debugf("Browsing nodeID: %s", nodeID.String())

		// Start a goroutine for browsing
		wg.Add(1)
		go browse(ctx, g.Client.Node(nodeID), "", 0, g.Log, nodeID.String(), nodeChan, errChan, &wg)
	}

	// close nodeChan and errChan once all browsing is done
	wg.Wait()
	close(nodeChan)
	close(errChan)

	// Read nodes from nodeChan and process them
	for node := range nodeChan {
		// Add the node to nodeList
		nodeList = append(nodeList, node)
	}

	UpdateNodePaths(nodeList)

	// Check for any errors collected during browsing
	if len(errChan) > 0 {
		// Return the first error encountered
		return <-errChan
	}

	// Now add i=2258 to the nodeList, which is the CurrentTime node, which is used for heartbeats
	// This is only added if the heartbeat is enabled
	// instead of i=2258 the g.HeartbeatNodeId is used, which can be different in tests
	if g.UseHeartbeat {

		// Check if the node is already in the list
		for _, node := range nodeList {
			if node.NodeID.Namespace() == g.HeartbeatNodeId.Namespace() && node.NodeID.IntID() == g.HeartbeatNodeId.IntID() {
				g.HeartbeatManualSubscribed = true
				break
			}
		}

		// If the node is not in the list, add it
		if !g.HeartbeatManualSubscribed {
			heartbeatNodeID := g.HeartbeatNodeId

			// Copied and pasted from above, just for one node
			nodeHeartbeatChan := make(chan NodeDef, 1)
			var wgHeartbeat sync.WaitGroup
			errChanHeartbeat := make(chan error, 1)

			wgHeartbeat.Add(1)
			go browse(ctx, g.Client.Node(heartbeatNodeID), "", 0, g.Log, heartbeatNodeID.String(), nodeHeartbeatChan, errChanHeartbeat, &wgHeartbeat)

			wgHeartbeat.Wait()
			close(nodeHeartbeatChan)
			close(errChanHeartbeat)
			for node := range nodeHeartbeatChan {
				nodeList = append(nodeList, node)
			}
			UpdateNodePaths(nodeList)
			if len(errChanHeartbeat) > 0 {
				return <-errChanHeartbeat
			}
		}
	}

	b, err := json.Marshal(nodeList)
	if err != nil {
		g.Log.Errorf("Unmarshalling failed: %s", err)
		_ = g.Close(ctx) // ensure that if something fails here, the connection is always safely closed
		return err
	}

	g.Log.Infof("Detected nodes: %s", b)

	g.NodeList = nodeList

	// If subscription is enabled, start subscribing to the nodes
	if g.SubscribeEnabled {
		g.Log.Infof("Subscription is enabled, therefore start subscribing to the selected notes...")

		g.SubNotifyChan = make(chan *opcua.PublishNotificationData, 10000)

		g.Subscription, err = g.Client.Subscribe(ctx, &opcua.SubscriptionParameters{
			Interval: opcua.DefaultSubscriptionInterval,
		}, g.SubNotifyChan)
		if err != nil {
			g.Log.Errorf("Subscribing failed: %s", err)
			_ = g.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}

		monitoredNodes, err := g.MonitorBatched(ctx, nodeList)
		if err != nil {
			g.Log.Errorf("Monitoring failed: %s", err)
			_ = g.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}

		g.Log.Infof("Subscribed to %d nodes!", monitoredNodes)

	}

	return nil
}

// MonitorBatched splits the nodes into manageable batches and starts monitoring them.
// This approach prevents the server from returning BadTcpMessageTooLarge by avoiding oversized monitoring requests.
// It returns the total number of nodes that were successfully monitored or an error if monitoring fails.
func (g *OPCUAInput) MonitorBatched(ctx context.Context, nodes []NodeDef) (int, error) {
	const maxBatchSize = 500
	totalMonitored := 0
	totalNodes := len(nodes)

	if len(nodes) == 0 {
		g.Log.Errorf("Did not subscribe to any nodes. This can happen if the nodes that are selected are incompatible with this benthos version. Aborting...")
		return 0, fmt.Errorf("no valid nodes selected")
	}

	g.Log.Infof("Starting to monitor %d nodes in batches of %d", totalNodes, maxBatchSize)

	for startIdx := 0; startIdx < totalNodes; startIdx += maxBatchSize {
		endIdx := startIdx + maxBatchSize
		if endIdx > totalNodes {
			endIdx = totalNodes
		}

		batch := nodes[startIdx:endIdx]
		g.Log.Infof("Creating monitor for nodes %d to %d", startIdx, endIdx-1)

		monitoredRequests := make([]*ua.MonitoredItemCreateRequest, 0, len(batch))

		for pos, nodeDef := range batch {
			request := opcua.NewMonitoredItemCreateRequestWithDefaults(
				nodeDef.NodeID,
				ua.AttributeIDValue,
				uint32(startIdx+pos),
			)
			monitoredRequests = append(monitoredRequests, request)
		}

		response, err := g.Subscription.Monitor(ctx, ua.TimestampsToReturnBoth, monitoredRequests...)
		if err != nil {
			g.Log.Errorf("Failed to monitor batch %d-%d: %v", startIdx, endIdx-1, err)
			if closeErr := g.Close(ctx); closeErr != nil {
				g.Log.Errorf("Failed to close OPC UA connection: %v", closeErr)
			}
			return totalMonitored, fmt.Errorf("monitoring failed for batch %d-%d: %w", startIdx, endIdx-1, err)
		}

		if response == nil {
			g.Log.Error("Received nil response from Monitor call")
			if closeErr := g.Close(ctx); closeErr != nil {
				g.Log.Errorf("Failed to close OPC UA connection: %v", closeErr)
			}
			return totalMonitored, errors.New("received nil response from Monitor")
		}

		for i, result := range response.Results {
			if !errors.Is(result.StatusCode, ua.StatusOK) {
				failedNode := batch[i].NodeID.String()
				g.Log.Errorf("Failed to monitor node %s: %v", failedNode, result.StatusCode)
				// Depending on requirements, you might choose to continue monitoring other nodes
				// instead of aborting. Here, we abort on the first failure.
				if closeErr := g.Close(ctx); closeErr != nil {
					g.Log.Errorf("Failed to close OPC UA connection: %v", closeErr)
				}
				return totalMonitored, fmt.Errorf("monitoring failed for node %s: %v", failedNode, result.StatusCode)
			}
		}

		monitoredNodes := len(response.Results)
		totalMonitored += monitoredNodes
		g.Log.Infof("Successfully monitored %d nodes in current batch", monitoredNodes)
	}

	g.Log.Infof("Monitoring completed. Total nodes monitored: %d/%d", totalMonitored, totalNodes)
	return totalMonitored, nil
}

func (g *OPCUAInput) GetOPCUAClientOptions(selectedEndpoint *ua.EndpointDescription, selectedAuthentication ua.UserTokenType) (opts []opcua.Option, err error) {
	opts = append(opts, opcua.SecurityFromEndpoint(selectedEndpoint, selectedAuthentication))

	// Set additional options based on the authentication method
	switch selectedAuthentication {
	case ua.UserTokenTypeAnonymous:
		g.Log.Infof("Using anonymous login")
	case ua.UserTokenTypeUserName:
		g.Log.Infof("Using username/password login")
		opts = append(opts, opcua.AuthUsername(g.Username, g.Password))
	}

	// Generate certificates if Basic256Sha256
	if selectedEndpoint.SecurityPolicyURI == ua.SecurityPolicyURIBasic256Sha256 {
		randomStr := randomString(8) // Generates an 8-character random string
		clientName := "urn:benthos-umh:client-" + randomStr
		certPEM, keyPEM, err := GenerateCert(clientName, 2048, 24*time.Hour*365*10)
		if err != nil {
			g.Log.Errorf("Failed to generate certificate: %v", err)
			return nil, err
		}

		// Convert PEM to X509 Certificate and RSA PrivateKey for in-memory use.
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			g.Log.Errorf("Failed to parse certificate: %v", err)
			return nil, err
		}

		pk, ok := cert.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			g.Log.Errorf("Invalid private key type")
			return nil, err
		}

		// Append the certificate and private key to the client options
		opts = append(opts, opcua.PrivateKey(pk), opcua.Certificate(cert.Certificate[0]))
	}

	opts = append(opts, opcua.SessionName("benthos-umh"))
	if g.SessionTimeout > 0 {
		opts = append(opts, opcua.SessionTimeout(time.Duration(g.SessionTimeout*int(time.Millisecond)))) // set the session timeout to prevent having to many connections
	} else {
		opts = append(opts, opcua.SessionTimeout(SessionTimeout))
	}
	opts = append(opts, opcua.ApplicationName("benthos-umh"))
	//opts = append(opts, opcua.ApplicationURI("urn:benthos-umh"))
	//opts = append(opts, opcua.ProductURI("urn:benthos-umh"))

	// Fine-Tune Buffer
	//opts = append(opts, opcua.MaxMessageSize(2*1024*1024))    // 2MB
	//opts = append(opts, opcua.ReceiveBufferSize(2*1024*1024)) // 2MB
	//opts = append(opts, opcua.SendBufferSize(2*1024*1024))    // 2MB
	return opts, nil
}

func (g *OPCUAInput) LogEndpoint(endpoint *ua.EndpointDescription) {
	g.Log.Infof("  EndpointURL: %s", endpoint.EndpointURL)
	g.Log.Infof("  SecurityMode: %v", endpoint.SecurityMode)
	g.Log.Infof("  SecurityPolicyURI: %s", endpoint.SecurityPolicyURI)
	g.Log.Infof("  TransportProfileURI: %s", endpoint.TransportProfileURI)
	g.Log.Infof("  SecurityLevel: %d", endpoint.SecurityLevel)

	// If Server is not nil, log its details
	if endpoint.Server != nil {
		g.Log.Infof("  Server ApplicationURI: %s", endpoint.Server.ApplicationURI)
		g.Log.Infof("  Server ProductURI: %s", endpoint.Server.ProductURI)
		g.Log.Infof("  Server ApplicationName: %s", endpoint.Server.ApplicationName.Text)
		g.Log.Infof("  Server ApplicationType: %v", endpoint.Server.ApplicationType)
		g.Log.Infof("  Server GatewayServerURI: %s", endpoint.Server.GatewayServerURI)
		g.Log.Infof("  Server DiscoveryProfileURI: %s", endpoint.Server.DiscoveryProfileURI)
		g.Log.Infof("  Server DiscoveryURLs: %v", endpoint.Server.DiscoveryURLs)
	}

	// Output the certificate
	if len(endpoint.ServerCertificate) > 0 {
		// Convert to PEM format first, then log the certificate information
		pemCert := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: endpoint.ServerCertificate,
		})
		g.logCertificateInfo(pemCert)
	}

	// Loop through UserIdentityTokens
	for j, token := range endpoint.UserIdentityTokens {
		g.Log.Infof("  UserIdentityToken %d:", j+1)
		g.Log.Infof("    PolicyID: %s", token.PolicyID)
		g.Log.Infof("    TokenType: %v", token.TokenType)
		g.Log.Infof("    IssuedTokenType: %s", token.IssuedTokenType)
		g.Log.Infof("    IssuerEndpointURL: %s", token.IssuerEndpointURL)
	}
}

func (g *OPCUAInput) LogEndpoints(endpoints []*ua.EndpointDescription) {
	for i, endpoint := range endpoints {
		g.Log.Infof("Endpoint %d:", i+1)
		g.LogEndpoint(endpoint)
	}
}

// FetchAllEndpoints retrieves all possible endpoints from an OPC UA server,
// handling cases where only a Discovery URL is provided
// and substituting server endpoints with user-specified ones if necessary.
// This is important for addressing issues with servers that return only their DNS name.
func (g *OPCUAInput) FetchAllEndpoints(ctx context.Context) ([]*ua.EndpointDescription, error) {
	g.Log.Infof("Querying OPC UA server at: %s", g.Endpoint)

	endpoints, err := opcua.GetEndpoints(ctx, g.Endpoint)
	if err != nil {
		g.Log.Errorf("Error fetching endpoints from: %s, error: %s", g.Endpoint, err)
		return nil, err
	}

	g.Log.Infof("Retrieved %d initial endpoint(s).", len(endpoints))

	// If only one endpoint is found, further discovery is attempted using the Discovery URL.
	if len(endpoints) == 1 {
		return g.handleSingleEndpointDiscovery(ctx, endpoints[0])
	}

	adjustedEndpoints, err := g.ReplaceHostInEndpoints(endpoints, g.Endpoint)
	if err != nil {
		g.Log.Errorf("Failed to adjust endpoint hosts: %s", err)
		return nil, err
	}

	// For multiple endpoints, adjust their hosts as per user specification.
	return adjustedEndpoints, nil
}

// handleSingleEndpointDiscovery processes a single discovered endpoint by attempting to discover more endpoints using its Discovery URL and applying user-specified host adjustments.
func (g *OPCUAInput) handleSingleEndpointDiscovery(ctx context.Context, endpoint *ua.EndpointDescription) ([]*ua.EndpointDescription, error) {
	if endpoint == nil || endpoint.Server == nil || len(endpoint.Server.DiscoveryURLs) == 0 {
		if endpoint != nil && endpoint.Server != nil && len(endpoint.Server.DiscoveryURLs) == 0 { // This is the edge case when there is no discovery URL
			g.Log.Warnf("No discovery URL. This is the endpoint: %v", endpoint)
			g.LogEndpoint(endpoint)

			// Adjust the hosts of the endpoint that has no discovery URL
			updatedURL, err := g.ReplaceHostInEndpointURL(endpoint.EndpointURL, g.Endpoint)
			if err != nil {
				return nil, err
			}

			// Update the endpoint URL with the new host.
			endpoint.EndpointURL = updatedURL
			var updatedEndpoints []*ua.EndpointDescription
			updatedEndpoints = append(updatedEndpoints, endpoint)

			return updatedEndpoints, nil

		} else {
			g.Log.Errorf("Invalid endpoint configuration")
		}
		return nil, errors.New("invalid endpoint configuration")
	}

	discoveryURL := endpoint.Server.DiscoveryURLs[0]
	g.Log.Infof("Using discovery URL for further discovery: %s", discoveryURL)

	updatedURL, err := g.ReplaceHostInEndpointURL(discoveryURL, g.Endpoint)
	if err != nil {
		g.Log.Errorf("Failed to adjust endpoint URL: %s", err)
		return nil, err
	}

	moreEndpoints, err := opcua.GetEndpoints(ctx, updatedURL)
	if err != nil {
		return nil, err
	}

	// Adjust the hosts of the newly discovered endpoints.
	adjustedEndpoints, err := g.ReplaceHostInEndpoints(moreEndpoints, g.Endpoint)
	if err != nil {
		g.Log.Errorf("Failed to adjust endpoint hosts: %s", err)
		return nil, err
	}

	return adjustedEndpoints, nil
}

// ReplaceHostInEndpoints updates each endpoint's URL to use a specified host, aiming to mitigate potential connectivity issues due to DNS name discrepancies.
func (g *OPCUAInput) ReplaceHostInEndpoints(endpoints []*ua.EndpointDescription, newHost string) ([]*ua.EndpointDescription, error) {
	var updatedEndpoints []*ua.EndpointDescription

	for _, endpoint := range endpoints {
		updatedURL, err := g.ReplaceHostInEndpointURL(endpoint.EndpointURL, newHost)
		if err != nil {
			return nil, err
		}

		// Update the endpoint URL with the new host.
		endpoint.EndpointURL = updatedURL
		updatedEndpoints = append(updatedEndpoints, endpoint)
	}

	return updatedEndpoints, nil
}

// ReplaceHostInEndpointURL constructs a new endpoint URL by replacing the existing host with a new host, preserving the original path and query parameters.
func (g *OPCUAInput) ReplaceHostInEndpointURL(endpointURL, newHost string) (string, error) {

	// Remove the "opc.tcp://" prefix to simplify parsing.
	newHost = strings.TrimPrefix(newHost, "opc.tcp://")

	// Remove the "opc.tcp://" prefix to simplify parsing.
	withoutPrefix := strings.TrimPrefix(endpointURL, "opc.tcp://")

	// Identify the first slash ("/") to separate the host from the path.
	slashIndex := strings.Index(withoutPrefix, "/")

	if slashIndex == -1 {
		g.Log.Warnf("Endpoint URL does not contain a path: %s", endpointURL)
		// Assume entire URL is a host and directly replace with newHost, retaining the "opc.tcp://" prefix.
		return "opc.tcp://" + newHost, nil
	}

	// Reconstruct the endpoint URL with the new host and the original path/query.
	newURL := fmt.Sprintf("opc.tcp://%s%s", newHost, withoutPrefix[slashIndex:])
	g.Log.Infof("Updated endpoint URL to: %s", newURL)

	return newURL, nil
}
