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

	// Track topic changes using cumulative metadata
	cumulativeMetadata := t.mergeTopicHeaders(unsTreeId, []*TopicInfo{topicInfo})
	if t.shouldReportTopic(unsTreeId, cumulativeMetadata) {
		// Update topic info with cumulative metadata before storing
		topicInfoWithCumulative := *topicInfo // shallow copy
		topicInfoWithCumulative.Metadata = cumulativeMetadata
		t.pendingTopicChanges[unsTreeId] = &topicInfoWithCumulative
		t.updateTopicCache(unsTreeId, cumulativeMetadata)
	}

	// Update full topic map (authoritative state) with cumulative metadata
	topicInfoWithCumulative := *topicInfo // shallow copy
	topicInfoWithCumulative.Metadata = cumulativeMetadata
	t.fullTopicMap[unsTreeId] = &topicInfoWithCumulative

	return nil
}

// Helper methods for the old immediate processing workflow (kept for compatibility)

// initializeUnsBundle creates a new empty UNS bundle structure.
// The bundle is pre-allocated with initial capacity to avoid reallocations during processing.
// This structure will hold both the topic metadata map and the event entries.
func (t *TopicBrowserProcessor) initializeUnsBundle() *UnsBundle {
	return &UnsBundle{
		UnsMap: &TopicMap{
			Entries: make(map[string]*TopicInfo),
		},
		Events: &EventTable{
			Entries: make([]*EventTableEntry, 0, 1),
		},
	}
}

// processMessageBatch handles the conversion of raw messages into structured UNS data.
//
// # MESSAGE PROCESSING CONTRACT
//
// This function implements the individual message processing logic with the following guarantees:
//
// ## Processing Behavior:
//   - Processes each message independently (failure of one doesn't affect others)
//   - Extracts umh_topic metadata as the primary topic identifier
//   - Converts topic string to hierarchical TopicInfo structure
//   - Processes message payload into TimeSeriesPayload or RelationalPayload
//   - Generates UNS Tree ID via xxHash for efficient topic identification
//
// ## Error Handling Strategy:
//   - Individual message errors are logged with context but processing continues
//   - messages_failed metric is incremented for each failed message
//   - Failed messages do not appear in the final events array
//   - Non-fatal errors (e.g., malformed JSON) are handled gracefully
//
// ## Data Grouping Logic:
//   - Messages are grouped by UNS Tree ID for efficient metadata merging
//   - Multiple messages for the same topic have their headers merged
//   - Last-write-wins strategy for conflicting header values
//   - Each successful message produces exactly one EventTableEntry
//
// ## Header Management:
//   - Raw Kafka headers are preserved in EventTableEntry.RawKafkaMsg
//   - Headers are copied to TopicInfo.Metadata for search/filter functionality
//   - Bridge routing information extracted from "processed-by" header
//   - Kafka timestamp extracted for ProducedAtMs field
//
// For each message in the batch:
// - Extracts topic information and event data
// - Groups topic infos by UNS tree ID for efficient metadata merging
// - Updates metrics for monitoring and debugging
//
// Args:
//   - batch: Input message batch to process
//   - unsBundle: Output bundle to populate with events
//
// Returns:
//   - map[string][]*TopicInfo: Topics grouped by UNS Tree ID for cache comparison
//   - error: Fatal error that prevents further processing (nil for individual message failures)
func (t *TopicBrowserProcessor) processMessageBatch(
	batch service.MessageBatch,
	unsBundle *UnsBundle,
) (map[string][]*TopicInfo, error) {
	topicInfos := make(map[string][]*TopicInfo)

	for _, message := range batch {
		topicInfo, eventTableEntry, unsTreeId, err := MessageToUNSInfoAndEvent(message)
		if err != nil {
			t.logger.Errorf("Error while processing message: %v", err)
			t.messagesFailed.Incr(1)
			continue
		}

		// Add event to bundle
		unsBundle.Events.Entries = append(unsBundle.Events.Entries, eventTableEntry)
		topicInfo.Metadata = eventTableEntry.RawKafkaMsg.Headers

		// Group topic infos by UNS tree ID
		if _, ok := topicInfos[*unsTreeId]; !ok {
			topicInfos[*unsTreeId] = make([]*TopicInfo, 0, 1)
		}
		topicInfos[*unsTreeId] = append(topicInfos[*unsTreeId], topicInfo)
		t.messagesProcessed.Incr(1)
	}

	return topicInfos, nil
}
