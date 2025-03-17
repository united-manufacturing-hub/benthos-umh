package opcua_plugin

import (
	"golang.org/x/exp/slices"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

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
