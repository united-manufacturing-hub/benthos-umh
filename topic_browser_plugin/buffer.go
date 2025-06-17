package topic_browser_plugin

import (
	"errors"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Buffer management functions for TopicBrowserProcessor
// The topicRingBuffer struct is defined in topic_browser_plugin.go

// addEventToTopicBuffer adds an event to the per-topic ring buffer.
// If the buffer is full, it overwrites the oldest event and increments the overwritten metric.
func (t *TopicBrowserProcessor) addEventToTopicBuffer(topic string, event *EventTableEntry) {
	buffer := t.getOrCreateTopicBuffer(topic)

	// Add to ring buffer (overwrites oldest if full)
	buffer.events[buffer.head] = event
	buffer.head = (buffer.head + 1) % buffer.capacity

	if buffer.size < buffer.capacity {
		buffer.size++
	} else {
		// Buffer is full - we're overwriting the oldest event
		t.eventsOverwritten.Incr(1)
	}
}

// getOrCreateTopicBuffer returns the ring buffer for a topic, creating it if it doesn't exist.
func (t *TopicBrowserProcessor) getOrCreateTopicBuffer(topic string) *topicRingBuffer {
	if buffer, exists := t.topicBuffers[topic]; exists {
		return buffer
	}

	// Create new ring buffer for this topic
	buffer := &topicRingBuffer{
		events:   make([]*EventTableEntry, t.maxEventsPerTopic),
		head:     0,
		size:     0,
		capacity: t.maxEventsPerTopic,
	}
	t.topicBuffers[topic] = buffer
	return buffer
}

// getLatestEventsForTopic extracts all events from a topic's ring buffer in chronological order.
// Returns events from oldest to newest, preserving the correct time sequence.
func (t *TopicBrowserProcessor) getLatestEventsForTopic(topic string) []*EventTableEntry {
	buffer := t.topicBuffers[topic]
	if buffer == nil || buffer.size == 0 {
		return nil
	}

	// Extract events in chronological order (oldest to newest)
	events := make([]*EventTableEntry, buffer.size)
	for i := 0; i < buffer.size; i++ {
		idx := (buffer.head - buffer.size + i + buffer.capacity) % buffer.capacity
		events[i] = buffer.events[idx]
	}
	return events
}

// flushBufferAndACK handles the emission of accumulated data and ACK of buffered messages.
// This implements the delayed ACK pattern where messages are only ACKed after successful emission.
//
// # DELAYED ACK EMISSION ALGORITHM
//
// This function implements the core delayed ACK pattern:
//
// ## Emission Strategy:
//   - Collect all events from ring buffers across all topics
//   - Apply rate limiting (max events per topic per interval)
//   - Emit full topic tree when any topic metadata has changed
//   - Create single protobuf bundle with events + complete topic map
//
// ## Rate Limiting Logic:
//   - Extract latest N events per topic from ring buffers
//   - N = maxEventsPerTopic (configurable, default 10)
//   - Ring buffer automatically provides chronological ordering
//   - Oldest events naturally dropped when buffer is full
//
// ## Full Tree Emission:
//   - When any topic metadata changes, emit entire fullTopicMap
//   - Ensures downstream has complete topic state (no merge complexity)
//   - Consistent with existing shouldReportTopic change detection
//
// ## ACK Behavior:
//   - Only ACK messages after successful emission
//   - All buffered messages ACKed together (atomic operation)
//   - Failed emission = no ACK (messages will be retried)
//
// ## Buffer Management:
//   - Clear messageBuffer and pendingTopicChanges after emission
//   - Keep ring buffers intact for next interval (continuous buffering)
//   - Update lastEmitTime to reset interval timer
//
// Returns:
//   - []service.MessageBatch: [emission_message], [ack_batch] or nil on no data
//   - error: Emission failure (prevents ACK)
func (t *TopicBrowserProcessor) flushBufferAndACK() ([]service.MessageBatch, error) {
	t.bufferMutex.Lock()
	defer t.bufferMutex.Unlock()

	// Collect all events from ring buffers with rate limiting
	allEvents := make([]*EventTableEntry, 0)
	for topic := range t.topicBuffers {
		events := t.getLatestEventsForTopic(topic)
		// Apply rate limiting: max events per topic per interval
		if len(events) > t.maxEventsPerTopic {
			events = events[len(events)-t.maxEventsPerTopic:] // Keep latest N events
		}
		allEvents = append(allEvents, events...)
	}

	// Early return if no data to emit
	if len(allEvents) == 0 && len(t.pendingTopicChanges) == 0 {
		t.lastEmitTime = time.Now()
		// Still need to ACK any buffered messages even if no emission
		if len(t.messageBuffer) > 0 {
			ackBatch := make(service.MessageBatch, len(t.messageBuffer))
			copy(ackBatch, t.messageBuffer)
			t.clearBuffers()
			return []service.MessageBatch{ackBatch}, nil
		}
		return nil, nil
	}

	// Create UNS bundle
	unsBundle := &UnsBundle{
		UnsMap: &TopicMap{Entries: make(map[string]*TopicInfo)},
		Events: &EventTable{Entries: allEvents},
	}

	// Full tree emission: emit entire topic map when any topic changes
	if len(t.pendingTopicChanges) > 0 {
		for treeId, topic := range t.fullTopicMap {
			unsBundle.UnsMap.Entries[treeId] = topic
		}
	}

	// Serialize and compress bundle
	protoBytes, err := BundleToProtobufBytesWithCompression(unsBundle)
	if err != nil {
		return nil, err
	}

	// Create emission message
	emissionMsg := service.NewMessage(nil)
	emissionMsg.SetBytes(bytesToMessageWithStartEndBlocksAndTimestamp(protoBytes))

	// Create ACK batch from all buffered messages
	var ackBatch service.MessageBatch
	if len(t.messageBuffer) > 0 {
		ackBatch = make(service.MessageBatch, len(t.messageBuffer))
		copy(ackBatch, t.messageBuffer)
	}

	// Update metrics
	t.totalEventsEmitted.Incr(int64(len(allEvents)))
	t.emissionSize.Incr(int64(len(protoBytes)))

	// Clear buffers and update timestamp
	t.clearBuffers()
	t.lastEmitTime = time.Now()

	// Return emission + ACK batch
	if len(ackBatch) > 0 {
		return []service.MessageBatch{{emissionMsg}, ackBatch}, nil
	}
	return []service.MessageBatch{{emissionMsg}}, nil
}

// clearBuffers clears the message buffer and pending topic changes while preserving ring buffers.
// Ring buffers are kept intact to allow continuous buffering across emission intervals.
func (t *TopicBrowserProcessor) clearBuffers() {
	t.messageBuffer = nil
	t.pendingTopicChanges = make(map[string]*TopicInfo)
	// Note: topicBuffers (ring buffers) are kept intact for next interval
}

// extractTopicFromMessage extracts the topic string from message metadata.
// Returns the topic string or error if umh_topic metadata is missing.
func (t *TopicBrowserProcessor) extractTopicFromMessage(msg *service.Message) (string, error) {
	topic, exists := msg.MetaGet("umh_topic")
	if !exists {
		return "", errors.New("missing umh_topic metadata")
	}
	return topic, nil
}
