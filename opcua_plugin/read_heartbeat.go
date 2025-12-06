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
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/exp/slices"
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
	}
	g.Log.Debugf("Got heartbeat message. Renaming it.")
	msgs[idx].MetaSet("opcua_tag_group", "heartbeat")
	msgs[idx].MetaSet("opcua_tag_name", "CurrentTime")
	msgs[idx].MetaSet("opcua_heartbeat_message", "")
	return msgs
}
