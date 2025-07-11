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

package stream_processor_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	config2 "github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_engine"
)

var _ = Describe("StreamProcessor", func() {

	Describe("Configuration Validation", func() {
		It("should reject invalid mode", func() {
			config := StreamProcessorConfig{
				Mode:        "invalid",
				Model:       ModelConfig{Name: "pump", Version: "v1"},
				OutputTopic: "test.topic",
				Sources:     map[string]string{"press": "test.source"},
			}

			err := config2.validateConfig(config)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsupported mode"))
		})

		It("should require model name", func() {
			config := StreamProcessorConfig{
				Mode:        "timeseries",
				Model:       ModelConfig{Name: "", Version: "v1"},
				OutputTopic: "test.topic",
				Sources:     map[string]string{"press": "test.source"},
			}

			err := config2.validateConfig(config)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("model name is required"))
		})

		It("should require model version", func() {
			config := StreamProcessorConfig{
				Mode:        "timeseries",
				Model:       ModelConfig{Name: "pump", Version: ""},
				OutputTopic: "test.topic",
				Sources:     map[string]string{"press": "test.source"},
			}

			err := config2.validateConfig(config)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("model version is required"))
		})

		It("should require output topic", func() {
			config := StreamProcessorConfig{
				Mode:        "timeseries",
				Model:       ModelConfig{Name: "pump", Version: "v1"},
				OutputTopic: "",
				Sources:     map[string]string{"press": "test.source"},
			}

			err := config2.validateConfig(config)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("output_topic is required"))
		})

		It("should require at least one source", func() {
			config := StreamProcessorConfig{
				Mode:        "timeseries",
				Model:       ModelConfig{Name: "pump", Version: "v1"},
				OutputTopic: "test.topic",
				Sources:     map[string]string{},
			}

			err := config2.validateConfig(config)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at least one source mapping is required"))
		})

		It("should accept valid configuration", func() {
			config := StreamProcessorConfig{
				Mode:        "timeseries",
				Model:       ModelConfig{Name: "pump", Version: "v1"},
				OutputTopic: "test.topic",
				Sources:     map[string]string{"press": "test.source"},
			}

			err := config2.validateConfig(config)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Describe("Static Mapping Detection", func() {
		var detector *js_engine.StaticDetector

		BeforeEach(func() {
			sources := map[string]string{
				"press": "topic1",
				"tF":    "topic2",
				"r":     "topic3",
			}
			detector = js_engine.NewStaticDetector(sources)
		})

		It("should identify static string constants", func() {
			analysis, err := detector.AnalyzeMapping(`"SN-P42-008"`)
			Expect(err).ToNot(HaveOccurred())
			Expect(analysis.Type).To(Equal(StaticMapping))
			Expect(analysis.Dependencies).To(BeEmpty())
		})

		It("should identify static numeric constants", func() {
			analysis, err := detector.AnalyzeMapping("42")
			Expect(err).ToNot(HaveOccurred())
			Expect(analysis.Type).To(Equal(StaticMapping))
			Expect(analysis.Dependencies).To(BeEmpty())
		})

		It("should identify static Date.now() calls", func() {
			analysis, err := detector.AnalyzeMapping("Date.now()")
			Expect(err).ToNot(HaveOccurred())
			Expect(analysis.Type).To(Equal(StaticMapping))
			Expect(analysis.Dependencies).To(BeEmpty())
		})

		It("should identify dynamic mapping with single variable", func() {
			analysis, err := detector.AnalyzeMapping("press + 4.00001")
			Expect(err).ToNot(HaveOccurred())
			Expect(analysis.Type).To(Equal(DynamicMapping))
			Expect(analysis.Dependencies).To(ContainElement("press"))
		})

		It("should identify dynamic mapping with multiple variables", func() {
			analysis, err := detector.AnalyzeMapping("press + tF * 2")
			Expect(err).ToNot(HaveOccurred())
			Expect(analysis.Type).To(Equal(DynamicMapping))
			Expect(analysis.Dependencies).To(ContainElements("press", "tF"))
		})

		It("should handle unknown variables as static", func() {
			analysis, err := detector.AnalyzeMapping("unknownVar + 5")
			Expect(err).ToNot(HaveOccurred())
			Expect(analysis.Type).To(Equal(StaticMapping))
			Expect(analysis.Dependencies).To(BeEmpty())
		})

		It("should reject invalid JavaScript", func() {
			_, err := detector.AnalyzeMapping("invalid javascript +++")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Mapping Flattening", func() {
		It("should flatten simple mappings", func() {
			input := map[string]interface{}{
				"pressure":     "press + 4",
				"temperature":  "tF * 2",
				"serialNumber": `"SN-123"`,
			}

			result := FlattenMappings(input)
			Expect(result).To(HaveKeyWithValue("pressure", "press + 4"))
			Expect(result).To(HaveKeyWithValue("temperature", "tF * 2"))
			Expect(result).To(HaveKeyWithValue("serialNumber", `"SN-123"`))
		})

		It("should flatten nested mappings", func() {
			input := map[string]interface{}{
				"motor": map[string]interface{}{
					"rpm":         "press / 4",
					"temperature": "tF + 273.15",
				},
				"serialNumber": `"SN-123"`,
			}

			result := FlattenMappings(input)
			Expect(result).To(HaveKeyWithValue("motor.rpm", "press / 4"))
			Expect(result).To(HaveKeyWithValue("motor.temperature", "tF + 273.15"))
			Expect(result).To(HaveKeyWithValue("serialNumber", `"SN-123"`))
		})

		It("should handle deeply nested mappings", func() {
			input := map[string]interface{}{
				"device": map[string]interface{}{
					"motor": map[string]interface{}{
						"axis": map[string]interface{}{
							"x": "press",
							"y": "tF",
						},
					},
				},
			}

			result := FlattenMappings(input)
			Expect(result).To(HaveKeyWithValue("device.motor.axis.x", "press"))
			Expect(result).To(HaveKeyWithValue("device.motor.axis.y", "tF"))
		})
	})

	Describe("Mapping Analysis Integration", func() {
		It("should analyze mappings correctly", func() {
			config := StreamProcessorConfig{
				Mode:        "timeseries",
				Model:       ModelConfig{Name: "pump", Version: "v1"},
				OutputTopic: "test.topic",
				Sources:     map[string]string{"press": "test.source"},
				Mapping: map[string]interface{}{
					"pressure":     "press + 4",
					"serialNumber": `"SN-123"`,
				},
			}

			err := js_engine.analyzeMappingsWithDetection(&config)
			Expect(err).ToNot(HaveOccurred())
			Expect(config.StaticMappings).To(HaveKey("serialNumber"))
			Expect(config.DynamicMappings).To(HaveKey("pressure"))
		})

		It("should handle empty mapping configuration", func() {
			config := StreamProcessorConfig{
				Mode:        "timeseries",
				Model:       ModelConfig{Name: "pump", Version: "v1"},
				OutputTopic: "test.topic",
				Sources:     map[string]string{"press": "test.source"},
				Mapping:     nil,
			}

			err := js_engine.analyzeMappingsWithDetection(&config)
			Expect(err).ToNot(HaveOccurred())
			Expect(config.StaticMappings).To(BeEmpty())
			Expect(config.DynamicMappings).To(BeEmpty())
		})
	})

	Describe("Utility Functions", func() {
		It("should check variable containment correctly", func() {
			dependencies := []string{"press", "tF", "r"}
			Expect(js_engine.containsVariable(dependencies, "press")).To(BeTrue())
			Expect(js_engine.containsVariable(dependencies, "tF")).To(BeTrue())
			Expect(js_engine.containsVariable(dependencies, "unknown")).To(BeFalse())
		})
	})
})
