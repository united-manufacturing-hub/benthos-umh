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

package stream_processor_plugin

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("State Coverage Tests", func() {
	var (
		state *ProcessorState
	)

	BeforeEach(func() {
		state = NewProcessorState()
	})

	Describe("GetVariableValue", func() {
		It("should return variable value for existing variable", func() {
			// Set a variable
			state.SetVariable("temp", 25.5, "test_source")

			// Get the value
			value, exists := state.GetVariableValue("temp")
			Expect(exists).To(BeTrue())
			Expect(value).To(BeNumerically("==", 25.5))
		})

		It("should return nil for non-existing variable", func() {
			value, exists := state.GetVariableValue("nonexistent")
			Expect(exists).To(BeFalse())
			Expect(value).To(BeNil())
		})
	})

	Describe("GetVariableNames", func() {
		It("should return all variable names", func() {
			// Set some variables
			state.SetVariable("temp", 25.5, "test_source")
			state.SetVariable("press", 100.0, "test_source")
			state.SetVariable("flow", 50.0, "test_source")

			names := state.GetVariableNames()
			Expect(names).To(ConsistOf("temp", "press", "flow"))
		})

		It("should return empty slice for no variables", func() {
			names := state.GetVariableNames()
			Expect(names).To(BeEmpty())
		})
	})

	Describe("GetStateVersion", func() {
		It("should return increasing version numbers", func() {
			initialVersion := state.GetStateVersion()

			// Setting a variable should increment version
			state.SetVariable("temp", 25.5, "test_source")
			newVersion := state.GetStateVersion()

			Expect(newVersion).To(BeNumerically(">", initialVersion))
		})
	})

	// Note: String method is on StateManager, not ProcessorState
	// This test is omitted as it would require testing StateManager instead
})
