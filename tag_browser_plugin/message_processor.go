package tag_browser_plugin

import (
	"encoding/hex"
	"github.com/cespare/xxhash/v2"
	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// MessageToUNSInfoAndEvent first extracts the topic from the message, then the event data and finally the UNS info.
// It also sets the eventTag if necessary and generates the UnsTreeId, which is required by the frontend.
// Finally it appends the "raw" kafka message part, including the raw message and its headers
func MessageToUNSInfoAndEvent(message *service.Message) (*tagbrowserpluginprotobuf.UnsInfo, *tagbrowserpluginprotobuf.EventTableEntry, *string, error) {
	topic, err := extractTopicFromMessage(message)
	if err != nil {
		return nil, nil, nil, err
	}
	// Extract Event Data
	event, eventTag, err := messageToEvent(message)
	if err != nil {
		return nil, nil, nil, err
	}

	// Extract UNS Data
	unsInfo, err := topicToUNSInfo(topic)
	if err != nil {
		return nil, nil, nil, err
	}

	// If the message is a time-series entry, we can enrich the unsInfo
	if event.IsTimeseries {
		unsInfo.EventTag = wrapperspb.String(*eventTag)
	}

	// We now have everything to calculate the uns_tree_id
	unsTreeId := HashUNSTableEntry(unsInfo)
	event.UnsTreeId = unsTreeId

	// Finally, we populate the RawKafkaMsg field in the event
	event.RawKafkaMsg, err = messageToRawKafkaMsg(message)
	if err != nil {
		return nil, nil, nil, err
	}

	return unsInfo, event, &unsTreeId, nil
}

// HashUNSTableEntry generates an xxHash from the Enterprise, Site, ...
func HashUNSTableEntry(info *tagbrowserpluginprotobuf.UnsInfo) string {
	hasher := xxhash.New()
	_, _ = hasher.Write([]byte(info.Enterprise))
	// GetValue returns either the contained data, or "" if no value is set
	_, _ = hasher.Write([]byte(info.Site.GetValue()))
	_, _ = hasher.Write([]byte(info.Area.GetValue()))
	_, _ = hasher.Write([]byte(info.Line.GetValue()))
	_, _ = hasher.Write([]byte(info.WorkCell.GetValue()))
	_, _ = hasher.Write([]byte(info.OriginId.GetValue()))
	_, _ = hasher.Write([]byte(info.Schema))
	_, _ = hasher.Write([]byte(info.EventGroup.GetValue()))
	_, _ = hasher.Write([]byte(info.EventTag.GetValue()))
	return hex.EncodeToString(hasher.Sum(nil))
}
