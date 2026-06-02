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

package open_protocol_plugin

// Tag is one fanned-out measurement from a decoded result: a tag name and its
// scalar value (float64 | int | bool | string).
type Tag struct {
	Name  string
	Value any
}

// FanOut maps a decoded rev-1 LastTightening to its 18 tags, in a fixed order.
// Pure: no I/O, no clock. The order and set are invariant for rev 1.
func FanOut(lt LastTightening) []Tag {
	return []Tag{
		{"torque_actual", lt.TorqueActual},
		{"torque_target", lt.TorqueTarget},
		{"torque_min", lt.TorqueMin},
		{"torque_max", lt.TorqueMax},
		{"angle_actual", lt.AngleActual},
		{"angle_target", lt.AngleTarget},
		{"angle_min", lt.AngleMin},
		{"angle_max", lt.AngleMax},
		{"torque_status", lt.TorqueStatus},
		{"angle_status", lt.AngleStatus},
		{"tightening_ok", lt.TighteningOK},
		{"batch_status", lt.BatchStatus},
		{"tightening_id", lt.TighteningID},
		{"vin", lt.VIN},
		{"job_id", lt.JobID},
		{"pset_number", lt.PsetNumber},
		{"batch_counter", lt.BatchCounter},
		{"batch_size", lt.BatchSize},
	}
}
