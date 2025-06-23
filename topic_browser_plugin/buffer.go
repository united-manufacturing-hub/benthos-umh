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
//
// THREAD SAFETY: This function assumes t.bufferMutex is already held by the caller.
// It MUST be called from within a mutex-protected section.
func (t *TopicBrowserProcessor) addEventToTopicBuffer(topic string, event *EventTableEntry) {
	buffer := t.getOrCreateTopicBuffer(topic)

	// Add to ring buffer (overwrites oldest if full)
	buffer.events[buffer.head] = event
	buffer.head = (buffer.head + 1) % buffer.capacity

	if buffer.size < buffer.capacity {
		buffer.size++
	} else {
		// Buffer is full - we're overwriting the oldest event
		if t.eventsOverwritten != nil {
			t.eventsOverwritten.Incr(1)
		}
	}
}

// getOrCreateTopicBuffer returns the ring buffer for a topic, creating it if it doesn't exist.
//
// THREAD SAFETY: This function assumes t.bufferMutex is already held by the caller.
// It MUST be called from within a mutex-protected section.
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
//
// THREAD SAFETY: This function assumes t.bufferMutex is already held by the caller.
// It MUST be called from within a mutex-protected section.
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
// This is the public interface that acquires the mutex and delegates to flushBufferAndACKLocked.
func (t *TopicBrowserProcessor) flushBufferAndACK() ([]service.MessageBatch, error) {
	t.bufferMutex.Lock()
	defer t.bufferMutex.Unlock()
	return t.flushBufferAndACKLocked()
}

// flushBufferAndACKLocked handles the emission of accumulated data and ACK of buffered messages.
// This implements the delayed ACK pattern where messages are only ACKed after successful emission.
//
// # DELAYED ACK EMISSION ALGORITHM
//
// This function implements the core delayed ACK pattern:
//
// ## Emission Strategy:
//   - Collect all events from ring buffers across all topics
//   - Apply rate limiting (max events per topic per interval)
//   - Always emit complete topic tree (full state in every bundle)
//   - Create single protobuf bundle with events + complete topic map
//
// ## Rate Limiting Logic:
//   - Extract latest N events per topic from ring buffers
//   - N = maxEventsPerTopic (configurable, default 10)
//   - Ring buffer automatically provides chronological ordering
//   - Oldest events naturally dropped when buffer is full
//
// ## Full Tree Emission:
//   - Always emit entire fullTopicMap in every bundle (regardless of changes)
//   - Ensures downstream has complete topic state (no merge complexity)
//   - Stateless for downstream consumers - no need to track partial updates
//   - Consistent behavior - every emission contains complete state
//
// ## ACK Behavior:
//   - Only ACK messages after successful emission
//   - All buffered messages ACKed together (atomic operation)
//   - Failed emission = no ACK (messages will be retried)
//
// ## Buffer Management:
//   - Clear messageBuffer and ring buffers after emission
//   - Reset ring buffer state to prevent duplicate emissions
//   - Update lastEmitTime to reset interval timer
//
// THREAD SAFETY: This function assumes t.bufferMutex is already held by the caller.
// It MUST be called from within a mutex-protected section.
//
// Returns:
//   - []service.MessageBatch: [emission_message, ack_batch] - delayed ACK pattern
//   - error: Emission failure (prevents ACK)
func (t *TopicBrowserProcessor) flushBufferAndACKLocked() ([]service.MessageBatch, error) {

	// Collect all events from ring buffers (already rate-limited by buffer size)
	allEvents := make([]*EventTableEntry, 0)
	for topic := range t.topicBuffers {
		events := t.getLatestEventsForTopic(topic)
		// Ring buffer already enforces max events per topic - no additional limiting needed
		allEvents = append(allEvents, events...)
	}

	// Early return if no data to emit
	if len(allEvents) == 0 && len(t.fullTopicMap) == 0 {
		t.lastEmitTime = time.Now()
		// ✅ FIX: Clear buffers and return nil - don't emit original messages
		t.clearBuffers()
		return nil, nil
	}

	// Create UNS bundle
	unsBundle := &UnsBundle{
		UnsMap: &TopicMap{Entries: make(map[string]*TopicInfo)},
		Events: &EventTable{Entries: allEvents},
	}

	// Always emit complete fullTopicMap (no conditional emission)
	for treeId, topic := range t.fullTopicMap {
		unsBundle.UnsMap.Entries[treeId] = topic
	}

	// Serialize and compress bundle
	protoBytes, err := BundleToProtobufBytes(unsBundle)
	if err != nil {
		return nil, err
	}

	// Create emission message
	emissionMsg := service.NewMessage(nil)
	emissionMsg.SetBytes(bytesToMessageWithStartEndBlocksAndTimestamp(protoBytes))

	// Update metrics
	if t.totalEventsEmitted != nil {
		t.totalEventsEmitted.Incr(int64(len(allEvents)))
	}
	if t.emissionSize != nil {
		t.emissionSize.Incr(int64(len(protoBytes)))
	}

	// ✅ FIX: Create ACK batch BEFORE clearing buffers
	// For proper delayed ACK: [emission_batch, ack_batch_of_original_messages]
	ackBatch := make(service.MessageBatch, len(t.messageBuffer))
	copy(ackBatch, t.messageBuffer)

	// Clear buffers and update timestamp AFTER creating ACK batch
	t.clearBuffers()
	t.lastEmitTime = time.Now()

	// Return both emission and ACK batches
	return []service.MessageBatch{{emissionMsg}, ackBatch}, nil
}

// clearBuffers clears both message buffer and ring buffers after successful emission.
// This prevents duplicate event emission across intervals.
//
// THREAD SAFETY: This function assumes t.bufferMutex is already held by the caller.
// It MUST be called from within a mutex-protected section.
func (t *TopicBrowserProcessor) clearBuffers() {
	t.messageBuffer = nil
	// Clear all ring buffers to prevent duplicate emissions
	for topic := range t.topicBuffers {
		buffer := t.topicBuffers[topic]
		buffer.size = 0
		buffer.head = 0
		// Keep the allocated slice but reset pointers to prevent memory leaks
		for i := range buffer.events {
			buffer.events[i] = nil
		}
	}
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
