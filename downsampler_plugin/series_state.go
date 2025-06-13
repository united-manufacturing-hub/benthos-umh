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

package downsampler_plugin

import (
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

// SeriesState maintains processing state for a specific time series with ACK buffering support
//
// ## ACK Buffering Implementation
//
// This struct implements message buffering to support "emit-previous" algorithms like SDT.
// The buffering ensures at-least-once delivery while allowing algorithms to emit historical
// points when needed.
//
// ### Buffer Lifecycle:
// 1. **stash()**: Store message when algorithm doesn't emit (filtered)
// 2. **releaseCandidate()**: Retrieve and clear buffered message for emission
// 3. **Idle timeout**: Automatic flush based on algorithm's max_time setting
// 4. **Shutdown**: Proper emission of all buffered messages
//
// ### Memory Safety:
// - Only one message buffered per series (bounded memory usage)
// - Messages are copied to prevent mutation issues
// - Automatic cleanup on series reset or shutdown
type SeriesState struct {
	processor *algorithms.ProcessorWrapper
	mutex     sync.Mutex

	// Existing fields for late arrival detection and output tracking
	lastProcessedTime time.Time
	lastOutput        interface{}

	// ACK buffering fields for emit-previous algorithm support
	candidateMsg *service.Message // The buffered message (copied to prevent mutation)
	candidateTs  int64            // Timestamp when message was buffered (Unix milliseconds)

	// Optimization: Only buffer messages for algorithms that need emit-previous behavior
	// This eliminates unnecessary memory usage and idle-flush complexity for algorithms
	// like deadband that never emit historical points.
	holdsPrev bool // true if algorithm needs emit-previous buffering (e.g., SDT), false otherwise (e.g., deadband)
}

// stash stores a message as a candidate for potential later emission
// This is called when the algorithm doesn't emit a point but we want to buffer the message
// REQUIRES: caller must hold s.mutex.Lock()
func (s *SeriesState) stash(msg *service.Message) {
	s.candidateMsg = msg.Copy() // Make a copy to avoid mutation
	s.candidateTs = time.Now().UnixNano() / int64(time.Millisecond)
}

// releaseCandidate returns the buffered message if one exists and clears the buffer
// Returns nil if no message is buffered
// REQUIRES: caller must hold s.mutex.Lock()
func (s *SeriesState) releaseCandidate() *service.Message {
	if s.candidateMsg == nil {
		return nil
	}

	msg := s.candidateMsg
	s.candidateMsg = nil
	s.candidateTs = 0
	return msg
}

// hasCandidate returns true if there's a message currently buffered
// REQUIRES: caller must hold s.mutex.Lock()
func (s *SeriesState) hasCandidate() bool {
	return s.candidateMsg != nil
}

// getCandidateTimestamp returns the timestamp when the current candidate was buffered
// Returns 0 if no candidate is buffered
// REQUIRES: caller must hold s.mutex.Lock()
func (s *SeriesState) getCandidateTimestamp() int64 {
	return s.candidateTs
}
