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

// SeriesState holds the algorithm state of one time series and, for
// emit-previous algorithms such as swinging door, the one held candidate
// message. The candidate is ACKed when stashed and exists only in memory, so
// it is lost when:
//   - a newer sample replaces it;
//   - the max_time heartbeat or a late passthrough message emits the current
//     point instead (https://linear.app/united-manufacturing-hub/issue/ENG-8219);
//   - a boolean or string message on the series is emitted or stashed;
//   - a ds_* hint rebuilds the series' algorithm;
//   - a late message under late_policy drop replaces it (https://linear.app/united-manufacturing-hub/issue/ENG-8221);
//   - the idle flush cannot queue it, or Close queues it into shutdownBatch,
//     which is never read afterwards (https://linear.app/united-manufacturing-hub/issue/ENG-8218).
type SeriesState struct {
	processor *algorithms.ProcessorWrapper
	mutex     sync.Mutex

	// Existing fields for late arrival detection and output tracking
	lastProcessedTime time.Time

	// ACK buffering fields for emit-previous algorithm support
	candidateMsg *service.Message // The buffered message (copied to prevent mutation)
	candidateTs  int64            // Timestamp when message was buffered (Unix milliseconds)

	// Optimization: Only buffer messages for algorithms that need emit-previous behavior
	// This eliminates unnecessary memory usage and idle-flush complexity for algorithms
	// like deadband that never emit historical points.
	holdsPrev bool // true if algorithm needs emit-previous buffering (e.g., SDT), false otherwise (e.g., deadband)

	// Configuration tracking for parameter change detection
	// Used to detect when metadata overrides change parameters but keep the same algorithm
	lastConfig map[string]interface{} // Last effective configuration used for this series
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
