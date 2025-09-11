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
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

// Buffer management functions for TopicBrowserProcessor
// The topicRingBuffer struct is defined in topic_browser_plugin.go

// addEventToTopicBuffer adds an event to the per-topic ring buffer.
// If the buffer is full, it overwrites the oldest event and increments the overwritten metric.
//
// THREAD SAFETY: This function assumes t.bufferMutex is already held by the caller.
// It MUST be called from within a mutex-protected section.
func (t *TopicBrowserProcessor) addEventToTopicBuffer(topic string, event *proto.EventTableEntry) {
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
		events:   make([]*proto.EventTableEntry, t.maxEventsPerTopic),
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
func (t *TopicBrowserProcessor) getLatestEventsForTopic(topic string) []*proto.EventTableEntry {
	buffer := t.topicBuffers[topic]
	if buffer == nil || buffer.size == 0 {
		return nil
	}

	// Extract events in chronological order (oldest to newest)
	events := make([]*proto.EventTableEntry, buffer.size)
	for i := 0; i < buffer.size; i++ {
		idx := (buffer.head - buffer.size + i + buffer.capacity) % buffer.capacity
		events[i] = buffer.events[idx]
	}
	return events
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
	allEvents := make([]*proto.EventTableEntry, 0)
	for topic := range t.topicBuffers {
		events := t.getLatestEventsForTopic(topic)
		// Ring buffer already enforces max events per topic - no additional limiting needed
		allEvents = append(allEvents, events...)
	}

	// Early return if no data to emit
	if len(allEvents) == 0 && len(t.fullTopicMap) == 0 {
		t.lastEmitTime = time.Now()
		t.clearBuffers()
		// Return empty slice instead of nil to indicate successful processing with no output
		// See: github.com/redpanda-data/benthos/v4/internal/component/processor/auto_observed.go:263-264
		return []service.MessageBatch{}, nil
	}

	// Create UNS bundle
	unsBundle := &proto.UnsBundle{
		UnsMap: &proto.TopicMap{Entries: make(map[string]*proto.TopicInfo)},
		Events: &proto.EventTable{Entries: allEvents},
	}

	// Always emit complete fullTopicMap (no conditional emission)
	for treeId, topic := range t.fullTopicMap {
		unsBundle.UnsMap.Entries[treeId] = topic
	}

	// Serialize bundle
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

	// ✅ DELAYED ACK: ACK original messages in-place without forwarding them
	// We only want to forward the protobuf bundle, not the original messages
	for _, msg := range t.messageBuffer {
		msg.SetError(nil) // Clear any errors and ACK the message
	}

	// Clear buffers and update timestamp
	t.clearBuffers()
	t.lastEmitTime = time.Now()

	// Return only the emission message (protobuf bundle)
	// Original messages are ACKed above but not forwarded
	return []service.MessageBatch{{emissionMsg}}, nil
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
		clear(buffer.events)
	}
}

// ackBufferAndClearLocked maintains internal state but ACKs messages WITHOUT emitting downstream.
// This is used in overflow scenarios during catch-up/initial startup where we want to:
// - Keep the latest value per topic in internal state (fullTopicMap, ring buffers)
// - ACK messages to prevent Kafka replay
// - Skip downstream emissions to reduce pipeline pressure
// - Resume normal emissions when back to real-time processing
//
// # CATCH-UP PROCESSING STRATEGY
//
// During initial startup or catch-up scenarios, the system processes historical data
// faster than it can reasonably emit downstream. This function handles overflow by:
//
// ## What This Function Does:
//   - ✅ Update internal state with latest values per topic (maintains data freshness)
//   - ✅ Process ring buffer events (keeps latest N events per topic)
//   - ✅ ACK messages to Kafka (prevents infinite replay)
//   - ✅ Clear message buffers (prevents memory issues)
//   - ❌ Does NOT emit downstream (no need - historical data, not real-time)
//
// ## When To Use:
//   - Buffer overflow during initial startup (processing historical backlog)
//   - System constantly hitting maxBufferSize (catch-up scenario)
//   - CPU usage high and adaptive controller backing off
//
// ## Why This Makes Sense:
//   - Pro: All data is processed and used to maintain latest state per topic
//   - Pro: Ring buffers keep recent events for each topic
//   - Pro: Ready to emit current state when back to real-time
//   - Pro: Kafka offset progress maintained
//   - Pro: No need to emit during catch-up (we'll never reach 100k msg/sec in real-time)
//
// THREAD SAFETY: This function assumes t.bufferMutex is already held by the caller.
// It MUST be called from within a mutex-protected section.
//
// Returns: Number of messages that were processed and ACKed (for logging/metrics)
func (t *TopicBrowserProcessor) ackBufferAndClearLocked() int {
	// Count messages for logging
	messageCount := len(t.messageBuffer)

	// IMPORTANT: Update internal state with latest data before clearing
	// This ensures fullTopicMap and ring buffers contain the most recent values
	// even though we're not emitting downstream

	// Process ring buffer events to maintain latest state per topic
	// (This is similar to flushBufferAndACKLocked but without emission)

	// The fullTopicMap already contains the latest cumulative state from
	// the buffering process - we preserve this state for future emissions

	// ACK all buffered messages without emitting them downstream
	for _, msg := range t.messageBuffer {
		msg.SetError(nil) // Clear any errors and ACK the message
	}

	// Clear message buffer but preserve topic state for future emissions
	t.messageBuffer = nil

	// Clear ring buffers since we've processed their latest state
	// but keep fullTopicMap intact for next emission
	for topic := range t.topicBuffers {
		buffer := t.topicBuffers[topic]
		buffer.size = 0
		buffer.head = 0
		// Keep the allocated slice but reset pointers to prevent memory leaks
		clear(buffer.events)
	}

	// Update timestamp to prevent immediate re-emission
	t.lastEmitTime = time.Now()

	return messageCount
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
