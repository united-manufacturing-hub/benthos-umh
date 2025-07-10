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
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StateManager", func() {
	var (
		stateManager *StateManager
		config       *StreamProcessorConfig
	)

	BeforeEach(func() {
		config = &StreamProcessorConfig{
			Mode:        "timeseries",
			Model:       ModelConfig{Name: "test", Version: "v1"},
			OutputTopic: "test.output",
			Sources: map[string]string{
				"pressure":    "ia/raw/opcua/default/press",
				"temperature": "ia/raw/opcua/default/tF",
				"rpm":         "ia/raw/opcua/default/r",
			},
			Mapping: map[string]interface{}{
				"pressure":    "pressure * 0.001",
				"temperature": "temperature + 273.15",
				"rpm":         "rpm",
				"location":    "\"Factory-A\"",
			},
			StaticMappings: map[string]MappingInfo{
				"location": {
					VirtualPath:  "location",
					Expression:   "\"Factory-A\"",
					Type:         StaticMapping,
					Dependencies: []string{},
				},
			},
			DynamicMappings: map[string]MappingInfo{
				"pressure": {
					VirtualPath:  "pressure",
					Expression:   "pressure * 0.001",
					Type:         DynamicMapping,
					Dependencies: []string{"pressure"},
				},
				"temperature": {
					VirtualPath:  "temperature",
					Expression:   "temperature + 273.15",
					Type:         DynamicMapping,
					Dependencies: []string{"temperature"},
				},
				"rpm": {
					VirtualPath:  "rpm",
					Expression:   "rpm",
					Type:         DynamicMapping,
					Dependencies: []string{"rpm"},
				},
			},
		}

		stateManager = NewStateManager(config)
	})

	Context("ProcessorState Variable Management", func() {
		It("should set and get variables", func() {
			// Set a variable
			stateManager.GetState().SetVariable("pressure", 1000.0, "ia/raw/opcua/default/press")

			// Get the variable
			value, exists := stateManager.GetState().GetVariable("pressure")
			Expect(exists).To(BeTrue())
			Expect(value.Value).To(Equal(1000.0))
			Expect(value.Source).To(Equal("ia/raw/opcua/default/press"))
			Expect(value.Timestamp).To(BeTemporally("~", time.Now(), time.Second))
		})

		It("should handle non-existent variables", func() {
			value, exists := stateManager.GetState().GetVariable("nonexistent")
			Expect(exists).To(BeFalse())
			Expect(value).To(BeNil())
		})

		It("should check if variables exist", func() {
			// Non-existent variable
			Expect(stateManager.GetState().HasVariable("pressure")).To(BeFalse())

			// Set and check
			stateManager.GetState().SetVariable("pressure", 1000.0, "ia/raw/opcua/default/press")
			Expect(stateManager.GetState().HasVariable("pressure")).To(BeTrue())
		})

		It("should get all variables", func() {
			// Set multiple variables
			stateManager.GetState().SetVariable("pressure", 1000.0, "ia/raw/opcua/default/press")
			stateManager.GetState().SetVariable("temperature", 25.0, "ia/raw/opcua/default/tF")

			// Get all variables
			variables := stateManager.GetState().GetAllVariables()
			Expect(len(variables)).To(Equal(2))
			Expect(variables).To(HaveKey("pressure"))
			Expect(variables).To(HaveKey("temperature"))
		})

		It("should update existing variables", func() {
			// Set initial value
			stateManager.GetState().SetVariable("pressure", 1000.0, "ia/raw/opcua/default/press")

			initialTime := time.Now()
			time.Sleep(10 * time.Millisecond) // Small delay to ensure timestamp difference

			// Update value
			stateManager.GetState().SetVariable("pressure", 2000.0, "ia/raw/opcua/default/press")

			// Check updated value
			value, exists := stateManager.GetState().GetVariable("pressure")
			Expect(exists).To(BeTrue())
			Expect(value.Value).To(Equal(2000.0))
			Expect(value.Timestamp).To(BeTemporally(">", initialTime))
		})

		It("should provide variable context for JavaScript", func() {
			// Set some variables
			stateManager.GetState().SetVariable("pressure", 1000.0, "ia/raw/opcua/default/press")
			stateManager.GetState().SetVariable("temperature", 25.0, "ia/raw/opcua/default/tF")

			// Get variable context
			context := stateManager.GetState().GetVariableContext()
			Expect(len(context)).To(Equal(2))
			Expect(context["pressure"]).To(Equal(1000.0))
			Expect(context["temperature"]).To(Equal(25.0))
		})

		It("should handle variable removal", func() {
			// Set a variable
			stateManager.GetState().SetVariable("pressure", 1000.0, "ia/raw/opcua/default/press")
			Expect(stateManager.GetState().HasVariable("pressure")).To(BeTrue())

			// Remove variable
			removed := stateManager.GetState().RemoveVariable("pressure")
			Expect(removed).To(BeTrue())
			Expect(stateManager.GetState().HasVariable("pressure")).To(BeFalse())

			// Try to remove non-existent variable
			removed = stateManager.GetState().RemoveVariable("nonexistent")
			Expect(removed).To(BeFalse())
		})

		It("should clear all variables", func() {
			// Set multiple variables
			stateManager.GetState().SetVariable("pressure", 1000.0, "ia/raw/opcua/default/press")
			stateManager.GetState().SetVariable("temperature", 25.0, "ia/raw/opcua/default/tF")
			Expect(stateManager.GetState().GetVariableCount()).To(Equal(2))

			// Clear all variables
			stateManager.GetState().ClearVariables()
			Expect(stateManager.GetState().GetVariableCount()).To(Equal(0))
		})
	})

	Context("Source Resolution", func() {
		It("should resolve topic to variable name", func() {
			// Test existing mappings
			varName, exists := stateManager.ResolveVariableFromTopic("ia/raw/opcua/default/press")
			Expect(exists).To(BeTrue())
			Expect(varName).To(Equal("pressure"))

			varName, exists = stateManager.ResolveVariableFromTopic("ia/raw/opcua/default/tF")
			Expect(exists).To(BeTrue())
			Expect(varName).To(Equal("temperature"))

			varName, exists = stateManager.ResolveVariableFromTopic("ia/raw/opcua/default/r")
			Expect(exists).To(BeTrue())
			Expect(varName).To(Equal("rpm"))
		})

		It("should handle non-existent topics", func() {
			varName, exists := stateManager.ResolveVariableFromTopic("nonexistent/topic")
			Expect(exists).To(BeFalse())
			Expect(varName).To(BeEmpty())
		})

		It("should validate configured source topics", func() {
			// Test existing sources
			Expect(stateManager.ValidateTopicIsConfiguredSource("ia/raw/opcua/default/press")).To(BeTrue())
			Expect(stateManager.ValidateTopicIsConfiguredSource("ia/raw/opcua/default/tF")).To(BeTrue())
			Expect(stateManager.ValidateTopicIsConfiguredSource("ia/raw/opcua/default/r")).To(BeTrue())

			// Test non-existent source
			Expect(stateManager.ValidateTopicIsConfiguredSource("nonexistent/topic")).To(BeFalse())
		})

		It("should get all source topics", func() {
			topics := stateManager.GetSourceTopics()
			Expect(len(topics)).To(Equal(3))
			Expect(topics).To(ContainElements(
				"ia/raw/opcua/default/press",
				"ia/raw/opcua/default/tF",
				"ia/raw/opcua/default/r",
			))
		})

		It("should get variable info", func() {
			source, exists := stateManager.GetVariableInfo("pressure")
			Expect(exists).To(BeTrue())
			Expect(source).To(Equal("ia/raw/opcua/default/press"))

			source, exists = stateManager.GetVariableInfo("nonexistent")
			Expect(exists).To(BeFalse())
			Expect(source).To(BeEmpty())
		})
	})

	Context("Mapping Analysis", func() {
		It("should identify static mappings", func() {
			staticMappings := stateManager.GetStaticMappings()
			Expect(len(staticMappings)).To(Equal(1))
			Expect(staticMappings[0].VirtualPath).To(Equal("location"))
			Expect(staticMappings[0].Expression).To(Equal("\"Factory-A\""))
			Expect(staticMappings[0].Type).To(Equal(StaticMapping))
		})

		It("should identify dependent mappings", func() {
			dependentMappings := stateManager.GetDependentMappings("pressure")
			Expect(len(dependentMappings)).To(Equal(1))
			Expect(dependentMappings[0].VirtualPath).To(Equal("pressure"))
			Expect(dependentMappings[0].Expression).To(Equal("pressure * 0.001"))
			Expect(dependentMappings[0].Dependencies).To(ContainElement("pressure"))
		})

		It("should get executable mappings for a variable", func() {
			// Set the pressure variable
			stateManager.GetState().SetVariable("pressure", 1000.0, "ia/raw/opcua/default/press")

			// Get executable mappings
			executableMappings := stateManager.GetExecutableMappings("pressure")
			Expect(len(executableMappings)).To(BeNumerically(">=", 1))

			// Should include the pressure mapping
			found := false
			for _, mapping := range executableMappings {
				if mapping.VirtualPath == "pressure" {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue())
		})
	})

	Context("Thread Safety", func() {
		It("should handle concurrent access", func() {
			const numGoroutines = 50
			const numOperations = 10

			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			// Multiple goroutines setting and getting variables
			for i := 0; i < numGoroutines; i++ {
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						varName := fmt.Sprintf("var_%d_%d", id, j)
						value := float64(id*1000 + j)

						stateManager.GetState().SetVariable(varName, value, "test/source")

						retrievedValue, exists := stateManager.GetState().GetVariable(varName)
						Expect(exists).To(BeTrue())
						Expect(retrievedValue.Value).To(Equal(value))
					}
				}(i)
			}

			wg.Wait()

			// Verify all variables are present
			allVars := stateManager.GetState().GetAllVariables()
			Expect(len(allVars)).To(Equal(numGoroutines * numOperations))
		})
	})

	Context("Configuration Integration", func() {
		It("should initialize with correct source mappings", func() {
			// Test that the state manager was initialized with the correct source mappings
			varName, exists := stateManager.ResolveVariableFromTopic("ia/raw/opcua/default/press")
			Expect(exists).To(BeTrue())
			Expect(varName).To(Equal("pressure"))

			varName, exists = stateManager.ResolveVariableFromTopic("ia/raw/opcua/default/tF")
			Expect(exists).To(BeTrue())
			Expect(varName).To(Equal("temperature"))

			varName, exists = stateManager.ResolveVariableFromTopic("ia/raw/opcua/default/r")
			Expect(exists).To(BeTrue())
			Expect(varName).To(Equal("rpm"))
		})

		It("should have correct static and dynamic mappings", func() {
			staticMappings := stateManager.GetStaticMappings()

			// Static mappings should only contain location
			Expect(len(staticMappings)).To(Equal(1))
			Expect(staticMappings[0].VirtualPath).To(Equal("location"))

			// Dynamic mappings should contain the rest
			dependentMappings := stateManager.GetDependentMappings("pressure")
			Expect(len(dependentMappings)).To(Equal(1))
			Expect(dependentMappings[0].VirtualPath).To(Equal("pressure"))
		})
	})
})
