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

package algorithms

// SwingingDoorAlgorithm placeholder - actual implementation is in backup folder
// This is a minimal implementation to support the NeedsPreviousPoint optimization
// while SDT is deactivated.

// The actual SwingingDoorAlgorithm struct and methods should be copied from
// backup/swinging_door/swinging_door.go.backup when SDT is reactivated.

// SwingingDoorAlgorithm placeholder struct
type SwingingDoorAlgorithm struct {
	// Actual fields are in the backup implementation
}

// NeedsPreviousPoint returns true because Swinging Door Trending uses emit-previous logic.
//
// SDT maintains an envelope around trend lines and emits the PREVIOUS point when
// the envelope collapses (when a new point would violate the interpolation error threshold).
// This emit-previous behavior requires ACK buffering to ensure:
// - At-least-once delivery semantics
// - No message duplication
// - Proper handling of buffered messages that become historical emissions
//
// This method will be used by the actual SDT implementation when it's reactivated.
func (s *SwingingDoorAlgorithm) NeedsPreviousPoint() bool {
	return true
}
