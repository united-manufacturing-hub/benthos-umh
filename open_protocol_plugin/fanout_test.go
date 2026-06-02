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

package open_protocol_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

var _ = Describe("FanOut", func() {
	sample := op.LastTightening{
		TorqueActual: 50.12, TorqueTarget: 50.00, TorqueMin: 45.00, TorqueMax: 55.00,
		AngleActual: 720, AngleTarget: 700, AngleMin: 650, AngleMax: 760,
		TorqueStatus: 1, AngleStatus: 1, TighteningOK: true, BatchStatus: 1,
		TighteningID: 42, VIN: "WVWZZZ1KZAW000001",
		JobID: 3, PsetNumber: 7, BatchCounter: 5, BatchSize: 10,
	}

	It("emits exactly 18 tags in a fixed order", func() {
		tags := op.FanOut(sample)
		names := make([]string, len(tags))
		for i, t := range tags {
			names[i] = t.Name
		}
		Expect(names).To(Equal([]string{
			"torque_actual", "torque_target", "torque_min", "torque_max",
			"angle_actual", "angle_target", "angle_min", "angle_max",
			"torque_status", "angle_status", "tightening_ok", "batch_status",
			"tightening_id", "vin", "job_id", "pset_number", "batch_counter", "batch_size",
		}))
	})

	It("maps each value with the correct type", func() {
		byName := map[string]any{}
		for _, t := range op.FanOut(sample) {
			byName[t.Name] = t.Value
		}
		Expect(byName["torque_actual"]).To(BeNumerically("~", 50.12, 0.001))
		Expect(byName["angle_actual"]).To(Equal(720))
		Expect(byName["tightening_ok"]).To(BeTrue())
		Expect(byName["vin"]).To(Equal("WVWZZZ1KZAW000001"))
		Expect(byName["tightening_id"]).To(Equal(42))
	})
})
