package topic_browser_plugin

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// bufferMessage handles the buffering of a processed message and its topic information.
// This function manages both the ring buffer storage and the message buffer for ACK control.
func (t *TopicBrowserProcessor) bufferMessage(msg *service.Message, event *EventTableEntry, topicInfo *TopicInfo, unsTreeId string) error {
	t.bufferMutex.Lock()
	defer t.bufferMutex.Unlock()

	// Safety check: prevent unbounded growth
	if len(t.messageBuffer) >= t.maxBufferSize {
		return errors.New("buffer full - dropping message")
	}

	// Extract topic string for ring buffer
	topic, err := t.extractTopicFromMessage(msg)
	if err != nil {
		return err
	}

	// Buffer original message (for ACK)
	t.messageBuffer = append(t.messageBuffer, msg)

	// Add to per-topic ring buffer
	t.addEventToTopicBuffer(topic, event)

	// Update fullTopicMap with cumulative metadata and emit complete state once per interval
	// This maintains the authoritative topic state with merged metadata across all messages
	cumulativeMetadata := t.mergeTopicHeaders(unsTreeId, []*TopicInfo{topicInfo})
	// Update topic info with cumulative metadata before storing
	topicInfoWithCumulative := *topicInfo // shallow copy
	topicInfoWithCumulative.Metadata = cumulativeMetadata
	t.updateTopicCache(unsTreeId, cumulativeMetadata)

	// Update full topic map (authoritative state) with cumulative metadata
	t.fullTopicMap[unsTreeId] = &topicInfoWithCumulative

	return nil
}
