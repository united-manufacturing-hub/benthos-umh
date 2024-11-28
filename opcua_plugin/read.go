package opcua_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// createMessageFromValue constructs a Benthos message from a given OPC UA DataValue and NodeDef.
//
// This function translates the OPC UA DataValue into a format suitable for Benthos, embedding relevant
// metadata derived from the NodeDef. It handles various data types, ensuring that the message payload
// accurately represents the OPC UA node's current value. Additionally, it marks heartbeat messages
// when applicable, facilitating heartbeat monitoring within the system.
func (g *OPCUAInput) createMessageFromValue(dataValue *ua.DataValue, nodeDef NodeDef) *service.Message {
	variant := dataValue.Value
	if variant == nil {
		g.Log.Errorf("Variant is nil")
		return nil
	}

	if !errors.Is(dataValue.Status, ua.StatusOK) {
		g.Log.Warnf("Received bad status %v for node %s", dataValue.Status, nodeDef.NodeID.String())
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

// Read performs a synchronous read operation on the OPC UA server using the provided ReadRequest.
//
// This function sends a ReadRequest to the OPC UA server and handles the response. It manages
// specific error conditions by closing the current session and signaling that the client is
// no longer connected, prompting reconnection attempts if necessary. Successful reads return
// the ReadResponse, while errors are appropriately logged and propagated.
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

// ReadBatchPull performs a batch read of all OPC UA nodes in the NodeList using a pull method.
//
// This function constructs a ReadRequest encompassing all nodes to be read and sends it to the OPC UA server.
// It processes the response by converting each DataValue into a Benthos message using `createMessageFromValue`.
// The function introduces a brief pause before returning the messages to ensure system stability.
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
