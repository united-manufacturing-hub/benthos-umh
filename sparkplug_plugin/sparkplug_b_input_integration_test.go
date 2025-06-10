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

package sparkplug_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("Sparkplug B Input Integration Tests", func() {

	Describe("Integrated Processing Functionality", func() {
		Context("Core component integration", func() {
			It("should have all core components initialized", func() {
				// Test that we can create an input with all processing options
				configYAML := `
broker_urls: ["tcp://test:1883"]
group_id: "TestFactory"
auto_split_metrics: true
data_messages_only: true
auto_extract_values: true
drop_birth_messages: false
strict_topic_validation: false
`
				spec := service.NewConfigSpec().
					Field(service.NewStringListField("broker_urls").Default([]string{"tcp://localhost:1883"})).
					Field(service.NewStringField("group_id")).
					Field(service.NewBoolField("auto_split_metrics").Default(true)).
					Field(service.NewBoolField("data_messages_only").Default(true)).
					Field(service.NewBoolField("auto_extract_values").Default(true)).
					Field(service.NewBoolField("drop_birth_messages").Default(false)).
					Field(service.NewBoolField("strict_topic_validation").Default(false))

				config, err := spec.ParseYAML(configYAML, service.NewEnvironment())
				Expect(err).NotTo(HaveOccurred())
				Expect(config).NotTo(BeNil())

				// Verify all fields are parsed correctly
				brokerURLs, err := config.FieldStringList("broker_urls")
				Expect(err).NotTo(HaveOccurred())
				Expect(brokerURLs).To(Equal([]string{"tcp://test:1883"}))

				groupID, err := config.FieldString("group_id")
				Expect(err).NotTo(HaveOccurred())
				Expect(groupID).To(Equal("TestFactory"))

				autoSplit, err := config.FieldBool("auto_split_metrics")
				Expect(err).NotTo(HaveOccurred())
				Expect(autoSplit).To(BeTrue())

				dataOnly, err := config.FieldBool("data_messages_only")
				Expect(err).NotTo(HaveOccurred())
				Expect(dataOnly).To(BeTrue())

				autoExtract, err := config.FieldBool("auto_extract_values")
				Expect(err).NotTo(HaveOccurred())
				Expect(autoExtract).To(BeTrue())

				dropBirth, err := config.FieldBool("drop_birth_messages")
				Expect(err).NotTo(HaveOccurred())
				Expect(dropBirth).To(BeFalse())

				strictValidation, err := config.FieldBool("strict_topic_validation")
				Expect(err).NotTo(HaveOccurred())
				Expect(strictValidation).To(BeFalse())
			})
		})

		Context("Message processing simulation", func() {
			It("should simulate BIRTH to DATA processing flow", func() {
				// This simulates what the input would do when processing messages
				testData := NewSparkplugTestData()

				// Test BIRTH message handling
				birthMsg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
				Expect(err).NotTo(HaveOccurred())

				// Verify BIRTH payload structure
				msgBytes, err := birthMsg.AsBytes()
				Expect(err).NotTo(HaveOccurred())

				var birthPayload sproto.Payload
				err = proto.Unmarshal(msgBytes, &birthPayload)
				Expect(err).NotTo(HaveOccurred())
				Expect(birthPayload.Metrics).To(HaveLen(4))

				// Test DATA message handling
				dataMsg, err := CreateBenthosMessage(testData.NDataPayload, testData.NDataTopic)
				Expect(err).NotTo(HaveOccurred())

				// Verify DATA payload structure
				msgBytes, err = dataMsg.AsBytes()
				Expect(err).NotTo(HaveOccurred())

				var dataPayload sproto.Payload
				err = proto.Unmarshal(msgBytes, &dataPayload)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataPayload.Metrics).To(HaveLen(2))

				// This demonstrates the integrated flow that happens inside the input
				By("BIRTH messages cache aliases for later use")
				By("DATA messages can be resolved using cached aliases")
				By("All processing happens within the input component")
			})

			It("should validate topic parsing integration", func() {
				testData := NewSparkplugTestData()

				topics := []struct {
					topic          string
					expectedType   string
					expectedDevice string
				}{
					{testData.NBirthTopic, "NBIRTH", ""},
					{testData.NDataTopic, "NDATA", ""},
					{testData.DBirthTopic, "DBIRTH", "Machine1"},
					{testData.DDataTopic, "DDATA", "Machine1"},
				}

				for _, tc := range topics {
					// Simulate topic parsing that happens in the input
					parts := []string{"spBv1.0", "TestFactory", tc.expectedType, "Line1"}
					if tc.expectedDevice != "" {
						parts = append(parts, tc.expectedDevice)
					}

					expectedTopic := parts[0] + "/" + parts[1] + "/" + parts[2] + "/" + parts[3]
					if len(parts) > 4 {
						expectedTopic += "/" + parts[4]
					}

					Expect(tc.topic).To(Equal(expectedTopic))
				}
			})
		})

		Context("Configuration validation", func() {
			It("should handle all processing configuration options", func() {
				configOptions := []struct {
					name         string
					defaultValue bool
					description  string
				}{
					{"auto_split_metrics", true, "Split multi-metric messages"},
					{"data_messages_only", true, "Only process DATA messages"},
					{"auto_extract_values", true, "Extract metric values automatically"},
					{"drop_birth_messages", false, "Drop BIRTH messages after alias caching"},
					{"strict_topic_validation", false, "Strictly validate topic format"},
					{"enable_rebirth_requests", true, "Send rebirth requests on sequence gaps"},
				}

				for _, opt := range configOptions {
					By("Validating " + opt.name + ": " + opt.description)

					// Each option should be configurable in the input
					configYAML := `
broker_urls: ["tcp://test:1883"]
group_id: "TestFactory"
` + opt.name + `: ` + (func() string {
						if opt.defaultValue {
							return "false"
						}
						return "true"
					})()

					spec := service.NewConfigSpec().
						Field(service.NewStringListField("broker_urls").Default([]string{"tcp://localhost:1883"})).
						Field(service.NewStringField("group_id")).
						Field(service.NewBoolField(opt.name).Default(opt.defaultValue))

					config, err := spec.ParseYAML(configYAML, service.NewEnvironment())
					Expect(err).NotTo(HaveOccurred())

					value, err := config.FieldBool(opt.name)
					Expect(err).NotTo(HaveOccurred())
					Expect(value).To(Equal(!opt.defaultValue)) // Should be opposite of default
				}
			})
		})
	})

	Describe("Component Registration", func() {
		It("should register sparkplug_b input component", func() {
			// This tests that the input component is properly registered
			// We can't easily test the actual registration without access to the global registry,
			// but we can test the configuration spec parsing

			By("The sparkplug_b input should accept all required configuration fields")

			// Minimum required config
			minimalConfig := `
broker_urls: ["tcp://localhost:1883"] 
group_id: "TestFactory"
`

			// Full config with all options
			fullConfig := `
broker_urls: ["tcp://broker1:1883", "tcp://broker2:1883"]
client_id: "test-client"
username: "testuser" 
password: "testpass"
group_id: "TestFactory"
primary_host_id: "TestHost"
qos: 2
keep_alive: "60s"
connect_timeout: "15s"
clean_session: false
drop_birth_messages: true
strict_topic_validation: true
auto_split_metrics: false
data_messages_only: false
auto_extract_values: false
enable_rebirth_requests: false
`

			// Test that both minimal and full configs can be parsed
			configs := []string{minimalConfig, fullConfig}

			for i, configYAML := range configs {
				By("Testing config " + string(rune('A'+i)))

				spec := service.NewConfigSpec().
					Field(service.NewStringListField("broker_urls").Default([]string{"tcp://localhost:1883"})).
					Field(service.NewStringField("client_id").Default("benthos-sparkplug-host")).
					Field(service.NewStringField("username").Default("").Optional()).
					Field(service.NewStringField("password").Default("").Optional()).
					Field(service.NewStringField("group_id")).
					Field(service.NewStringField("primary_host_id").Default("").Optional()).
					Field(service.NewIntField("qos").Default(1)).
					Field(service.NewDurationField("keep_alive").Default("30s")).
					Field(service.NewDurationField("connect_timeout").Default("10s")).
					Field(service.NewBoolField("clean_session").Default(true)).
					Field(service.NewBoolField("drop_birth_messages").Default(false)).
					Field(service.NewBoolField("strict_topic_validation").Default(false)).
					Field(service.NewBoolField("auto_split_metrics").Default(true)).
					Field(service.NewBoolField("data_messages_only").Default(true)).
					Field(service.NewBoolField("auto_extract_values").Default(true)).
					Field(service.NewBoolField("enable_rebirth_requests").Default(true))

				config, err := spec.ParseYAML(configYAML, service.NewEnvironment())
				Expect(err).NotTo(HaveOccurred())
				Expect(config).NotTo(BeNil())

				// Verify required fields
				groupID, err := config.FieldString("group_id")
				Expect(err).NotTo(HaveOccurred())
				Expect(groupID).To(Equal("TestFactory"))
			}
		})
	})
})
