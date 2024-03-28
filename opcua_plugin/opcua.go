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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/errors"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

type NodeDef struct {
	NodeID       *ua.NodeID
	NodeClass    ua.NodeClass
	BrowseName   string
	Description  string
	AccessLevel  ua.AccessLevelType
	ParentNodeID string
	Path         string
	DataType     string
	Writable     bool
	Unit         string
	Scale        string
	Min          string
	Max          string
}

func (n NodeDef) Records() []string {
	return []string{n.BrowseName, n.DataType, n.NodeID.String(), n.Unit, n.Scale, n.Min, n.Max, strconv.FormatBool(n.Writable), n.Description}
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

func browse(ctx context.Context, n *opcua.Node, path string, level int, logger *service.Logger, parentNodeId string) ([]NodeDef, error) {
	logger.Debugf("node:%s path:%q level:%d parentNodeId:%s\n", n, path, level, parentNodeId)
	if level > 10 {
		return nil, nil
	}

	attrs, err := n.Attributes(ctx, ua.AttributeIDNodeClass, ua.AttributeIDBrowseName, ua.AttributeIDDescription, ua.AttributeIDAccessLevel, ua.AttributeIDDataType)
	if err != nil {
		return nil, err
	}

	var def = NodeDef{
		NodeID: n.ID,
	}

	switch err := attrs[0].Status; err {
	case ua.StatusOK:
		def.NodeClass = ua.NodeClass(attrs[0].Value.Int())
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	switch err := attrs[1].Status; err {
	case ua.StatusOK:
		def.BrowseName = attrs[1].Value.String()
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	switch err := attrs[2].Status; err {
	case ua.StatusOK:
		def.Description = attrs[2].Value.String()
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	switch err := attrs[3].Status; err {
	case ua.StatusOK:
		def.AccessLevel = ua.AccessLevelType(attrs[3].Value.Int())
		def.Writable = def.AccessLevel&ua.AccessLevelTypeCurrentWrite == ua.AccessLevelTypeCurrentWrite
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	switch err := attrs[4].Status; err {
	case ua.StatusOK:
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
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	logger.Debugf("%d: def.Path:%s def.NodeClass:%s\n", level, def.Path, def.NodeClass)
	def.ParentNodeID = parentNodeId

	var nodes []NodeDef
	// If a node has a Variable class, it probably means that it is a tag
	// Therefore, no need to browse further
	if def.NodeClass == ua.NodeClassVariable {
		def.Path = join(path, def.BrowseName)
		nodes = append(nodes, def)
		return nodes, nil
	}

	browseChildren := func(refType uint32) error {
		refs, err := n.ReferencedNodes(ctx, refType, ua.BrowseDirectionForward, ua.NodeClassAll, true)
		if err != nil {
			return errors.Errorf("References: %d: %s", refType, err)
		}
		logger.Debugf("found %d child refs\n", len(refs))
		for _, rn := range refs {
			children, err := browse(ctx, rn, def.Path, level+1, logger, parentNodeId)
			if err != nil {
				return errors.Errorf("browse children: %s", err)
			}
			nodes = append(nodes, children...)
		}
		return nil
	}

	// If a node has an Object class, it probably means that it is a folder
	// Therefore, browse its children
	if def.NodeClass == ua.NodeClassObject {
		// To determine if an Object is a folder, we need to check different references
		// Add here all references that should be checked

		if err := browseChildren(id.HasComponent); err != nil {
			return nil, err
		}
		if err := browseChildren(id.Organizes); err != nil {
			return nil, err
		}
		if err := browseChildren(id.FolderType); err != nil {
			return nil, err
		}
		// For hasProperty it makes sense to show it very close to the tag itself, e.g., use the tagName as tagGroup and then the properties as subparts of it
		/*
			if err := browseChildren(id.HasProperty); err != nil {
				return nil, err
			}
		*/
	}
	return nodes, nil
}

//------------------------------------------------------------------------------

var OPCUAConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from OPC-UA servers. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Field(service.NewStringField("endpoint").Description("Address of the OPC-UA server to connect with.")).
	Field(service.NewStringField("username").Description("Username for server access. If not set, no username is used.").Default("")).
	Field(service.NewStringField("password").Description("Password for server access. If not set, no password is used.").Default("")).
	Field(service.NewStringListField("nodeIDs").Description("List of OPC-UA node IDs to begin browsing.")).
	Field(service.NewStringField("securityMode").Description("Security mode to use. If not set, a reasonable security mode will be set depending on the discovered endpoints.").Default("")).
	Field(service.NewStringField("securityPolicy").Description("The security policy to use.  If not set, a reasonable security policy will be set depending on the discovered endpoints.").Default("")).
	Field(service.NewBoolField("insecure").Description("Set to true to bypass secure connections, useful in case of SSL or certificate issues. Default is secure (false).").Default(false)).
	Field(service.NewBoolField("subscribeEnabled").Description("Set to true to subscribe to OPC-UA nodes instead of fetching them every seconds. Default is pulling messages every second (false).").Default(false))

func ParseNodeIDs(incomingNodes []string) []*ua.NodeID {

	// Parse all nodeIDs to validate them.
	// loop through all nodeIDs, parse them and put them into a slice
	parsedNodeIDs := make([]*ua.NodeID, len(incomingNodes))

	for _, id := range incomingNodes {
		parsedNodeID, err := ua.ParseNodeID(id)
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

	// fail if no nodeIDs are provided
	if len(nodeIDs) == 0 {
		return nil, errors.New("no nodeIDs provided")
	}

	parsedNodeIDs := ParseNodeIDs(nodeIDs)

	m := &OPCUAInput{
		Endpoint:         endpoint,
		Username:         username,
		Password:         password,
		NodeIDs:          parsedNodeIDs,
		Log:              mgr.Logger(),
		SecurityMode:     securityMode,
		SecurityPolicy:   securityPolicy,
		Insecure:         insecure,
		SubscribeEnabled: subscribeEnabled,
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
	SubscribeEnabled bool
	SubNotifyChan    chan *opcua.PublishNotificationData
}

// updateNodePaths updates the node paths to use the nodeID instead of the browseName
// if the browseName is not unique
func updateNodePaths(nodes []NodeDef) {
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
// theoretically nodeID can be extracted from variant, but not in all cases (e.g., when subscribing), so it it left to the calling function
func (g *OPCUAInput) createMessageFromValue(variant *ua.Variant, nodeDef NodeDef) *service.Message {
	if variant == nil {
		g.Log.Errorf("Variant is nil")
		return nil
	}

	b := make([]byte, 0)

	switch v := variant.Value().(type) {
	case float32:
		b = append(b, []byte(strconv.FormatFloat(float64(v), 'f', -1, 32))...)
	case float64:
		b = append(b, []byte(strconv.FormatFloat(v, 'f', -1, 64))...)
	case string:
		b = append(b, []byte(string(v))...)
	case bool:
		b = append(b, []byte(strconv.FormatBool(v))...)
	case int:
		b = append(b, []byte(strconv.Itoa(v))...)
	case int8:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int16:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int32:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int64:
		b = append(b, []byte(strconv.FormatInt(v, 10))...)
	case uint:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint8:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint16:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint32:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint64:
		b = append(b, []byte(strconv.FormatUint(v, 10))...)
	default:
		// Convert unknown types to JSON
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			g.Log.Errorf("Error marshaling to JSON: %v", err)
			return nil
		}
		b = append(b, jsonBytes...)
	}

	if b == nil {
		g.Log.Errorf("Could not create benthos message as payload is empty for node %s: %v", nodeDef.NodeID.String(), b)
		return nil
	}

	message := service.NewMessage(b)

	message.MetaSet("opcua_path", sanitize(nodeDef.NodeID.String()))
	message.MetaSet("opcua_tag_path", sanitize(nodeDef.Path))
	message.MetaSet("opcua_parent_path", sanitize(nodeDef.ParentNodeID))

	op, _ := message.MetaGet("opcua_path")
	pp, _ := message.MetaGet("opcua_parent_path")
	tp, _ := message.MetaGet("opcua_tag_path")
	g.Log.Debugf("Created message with opcua_path: %s", op)
	g.Log.Debugf("Created message with opcua_parent_path: %s", pp)
	g.Log.Debugf("Created message with opcua_tag_path: %s", tp)

	return message
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

	resp, err := g.Client.Read(ctx, req)
	if err != nil {
		g.Log.Errorf("Read failed: %s", err)
		// if the error is StatusBadSessionIDInvalid, the session has been closed
		// and we need to reconnect.
		switch err {
		case ua.StatusBadSessionIDInvalid:
			g.Client.Close(ctx)
			g.Client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadCommunicationError:
			g.Client.Close(ctx)
			g.Client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadConnectionClosed:
			g.Client.Close(ctx)
			g.Client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadTimeout:
			g.Client.Close(ctx)
			g.Client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadConnectionRejected:
			g.Client.Close(ctx)
			g.Client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadServerNotConnected:
			g.Client.Close(ctx)
			g.Client = nil
			return nil, nil, service.ErrNotConnected
		}

		// return error and stop executing this function.
		return nil, nil, err
	}

	if resp.Results[0].Status != ua.StatusOK {
		g.Log.Errorf("Status not OK: %v", resp.Results[0].Status)
	}

	// Create a message with the node's path as the metadata
	msgs := service.MessageBatch{}

	for i, node := range g.NodeList {
		value := resp.Results[i].Value
		if value == nil {
			g.Log.Errorf("Received nil from node: %s", node.NodeID.String())
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
					g.Log.Errorf("Received nil in item structure")
					continue
				}

				// now get the handle id, which is the position in g.Nodelist
				// see also NewMonitoredItemCreateRequestWithDefaults call in other functions
				handleID := item.ClientHandle

				if uint32(len(g.NodeList)) >= handleID {
					message := g.createMessageFromValue(item.Value.Value, g.NodeList[handleID])
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

	case _, ok := <-ctx.Done():
		if !ok {
			g.Log.Errorf("timeout channel was closed")
		} else {
			// Timeout occurred
			g.Log.Error("Timeout waiting for response from g.subNotifyChan")
		}
		return nil, nil, errors.New("timeout waiting for response")
	}
}

func (g *OPCUAInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if g.SubscribeEnabled {
		return g.ReadBatchSubscribe(ctx)
	}
	return g.ReadBatchPull(ctx)
}

func (g *OPCUAInput) Close(ctx context.Context) error {
	if g.Client != nil {
		g.Client.Close(ctx)
		g.Client = nil
	}

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
		g.Log.Errorf("Failed to parse certificate:", err)
		return
	}

	// Log the details
	g.Log.Infof("    Not Before:", cert.NotBefore)
	g.Log.Infof("    Not After:", cert.NotAfter)
	g.Log.Infof("    DNS Names:", cert.DNSNames)
	g.Log.Infof("    IP Addresses:", cert.IPAddresses)
	g.Log.Infof("    URIs:", cert.URIs)
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

	// Step 1: Retrieve all available endpoints from the OPC UA server
	// Iterate through DiscoveryURLs until we receive a list of all working endpoints including their potential security modes, etc.

	endpoints, err = g.FetchAllEndpoints(ctx)
	if err != nil {
		g.Log.Errorf("Failed to fetch an endpoint: %s", err)
		return err
	}

	// Step 2: Log details of each discovered endpoint for debugging
	g.LogEndpoints(endpoints)

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

	if g.SecurityMode != "" && g.SecurityPolicy != "" {
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
			g.Log.Errorf("Failed to create a new client")
			return err
		}

		// Connect to the selected endpoint
		if err := c.Connect(ctx); err != nil {
			g.Log.Errorf("Failed to connect", err)
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
				g.Log.Infof("Failed to connect", err)
				continue
			}
		}

		// If no connection was successful, return an error
		if c == nil {
			g.Log.Errorf("Failed to connect to any endpoint")
			return errors.New("failed to connect to any endpoint")
		}
	}

	g.Log.Infof("Connected to %s", g.Endpoint)
	g.Log.Infof("Please note that browsing large node trees can take a long time (around 5 nodes per second)")

	g.Client = c

	// Browse and subscribe to the nodes if needed
	if err := g.BrowseAndSubscribeIfNeeded(ctx); err != nil {
		return err
	}

	return nil
}

func (g *OPCUAInput) BrowseAndSubscribeIfNeeded(ctx context.Context) error {

	// Create a slice to store the detected nodes
	nodeList := make([]NodeDef, 0)

	// Print all nodeIDs that are being browsed
	for _, id := range g.NodeIDs {
		if id == nil {
			continue
		}

		// Print id
		g.Log.Debugf("Browsing nodeID: %s", id.String())

		// Browse the OPC-UA server's node tree and print the results.
		nodes, err := browse(ctx, g.Client.Node(id), "", 0, g.Log, id.String())
		if err != nil {
			g.Log.Errorf("Browsing failed: %s")
			g.Client.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}

		updateNodePaths(nodes)

		// Add the nodes to the nodeList
		nodeList = append(nodeList, nodes...)
	}

	b, err := json.Marshal(nodeList)
	if err != nil {
		g.Log.Errorf("Unmarshalling failed: %s")
		g.Client.Close(ctx) // ensure that if something fails here, the connection is always safely closed
		return err
	}

	g.Log.Infof("Detected nodes: %s", b)

	g.NodeList = nodeList

	// If subscription is enabled, start subscribing to the nodes
	if g.SubscribeEnabled {
		g.Log.Infof("Subscription is enabled, therefore start subscribing to the selected notes...")

		g.SubNotifyChan = make(chan *opcua.PublishNotificationData, 100)

		sub, err := g.Client.Subscribe(ctx, &opcua.SubscriptionParameters{
			Interval: opcua.DefaultSubscriptionInterval,
		}, g.SubNotifyChan)
		if err != nil {
			g.Log.Errorf("Subscribing failed: %s")
			g.Client.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}

		monitoredRequests := make([]*ua.MonitoredItemCreateRequest, 0, len(nodeList))

		for pos, id := range nodeList {
			miCreateRequest := opcua.NewMonitoredItemCreateRequestWithDefaults(id.NodeID, ua.AttributeIDValue, uint32(pos))
			monitoredRequests = append(monitoredRequests, miCreateRequest)
		}

		if len(nodeList) == 0 {
			g.Log.Errorf("Did not subscribe to any nodes. This can happen if the nodes that are selected are incompatible with this benthos version. Aborting...")
			return fmt.Errorf("no valid nodes selected")
		}

		res, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, monitoredRequests...)
		if err != nil {
			g.Log.Errorf("Monitoring failed: %s")
			g.Client.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}
		if res == nil {
			g.Log.Errorf("Expected res to not be nil, if there is no error")
			g.Client.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return fmt.Errorf("expected res to be not nil")
		}

		// Assuming you want to check the status code of each result
		for _, result := range res.Results {
			if !errors.Is(result.StatusCode, ua.StatusOK) {
				g.Log.Errorf("Monitoring failed with status code: %v", result.StatusCode)
				g.Client.Close(ctx) // ensure that if something fails here, the connection is always safely closed
				return fmt.Errorf("monitoring failed for node, status code: %v", result.StatusCode)
			}
		}

		g.Log.Infof("Subscribed to %d nodes!", len(res.Results))

	}

	return nil
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

	return opts, nil
}

func (g *OPCUAInput) LogEndpoints(endpoints []*ua.EndpointDescription) {
	for i, endpoint := range endpoints {
		g.Log.Infof("Endpoint %d:", i+1)
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
		g.Log.Errorf("Invalid or incomplete single endpoint data.")
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

// replaceHostInEndpointURL constructs a new endpoint URL by replacing the existing host with a new host, preserving the original path and query parameters.
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
