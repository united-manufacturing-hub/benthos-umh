//go:build !integration

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

package sparkplug_plugin

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// These tests cover the auto-extract (no configured metrics:) path of the
// Sparkplug B output, which is what a standard `tag_processor -> sparkplug_b`
// pipeline exercises. Regression for ENG-5087: the value must be read from the
// UMH-Core payload ("value"), and a metric must be produced from
// virtual_path + tag_name (or just tag_name when virtual_path is absent),
// rather than being silently dropped.
var _ = Describe("Sparkplug B output extractMessageData (auto-extract path)", func() {
	var out *sparkplugOutput

	BeforeEach(func() {
		out = &sparkplugOutput{
			autoExtractTagName: true, // no configured metrics -> dynamic path
			logger:             service.MockResources().Logger(),
			formatConverter:    NewFormatConverter(),
		}
	})

	// newMsg builds a UMH-Core message with the given payload JSON and metadata.
	newMsg := func(payload string, meta map[string]string) *service.Message {
		msg := service.NewMessage([]byte(payload))
		for k, v := range meta {
			msg.MetaSet(k, v)
		}
		return msg
	}
	umhCore := `{"value": 42, "timestamp_ms": 1640995200000}`

	It("extracts the value and builds virtual_path:tag_name (the ENG-5087 bug case)", func() {
		data, err := out.extractMessageData(newMsg(umhCore, map[string]string{
			"location_path": "enterprise.site.area.line",
			"tag_name":      "FirstFlagOfDiscreteInput",
			"virtual_path":  "modbus",
		}))

		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(HaveLen(1))
		// Assert the typed value, not its string rendering: the extracted value's
		// Go type drives the Sparkplug wire datatype, which is what ENG-5087 is about.
		Expect(data["modbus:FirstFlagOfDiscreteInput"]).To(Equal(json.Number("42")))
	})

	It("makes virtual_path optional: metric name is just tag_name", func() {
		data, err := out.extractMessageData(newMsg(umhCore, map[string]string{
			"location_path": "enterprise.site.area.line",
			"tag_name":      "FirstFlagOfDiscreteInput",
		}))

		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(HaveLen(1))
		Expect(data["FirstFlagOfDiscreteInput"]).To(Equal(json.Number("42")))
	})

	It("joins a dotted virtual_path with colons", func() {
		data, err := out.extractMessageData(newMsg(umhCore, map[string]string{
			"location_path": "enterprise.site.area.line",
			"tag_name":      "temperature",
			"virtual_path":  "motor.diagnostics",
		}))

		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(HaveKey("motor:diagnostics:temperature"))
	})

	It("reads alternate UMH-Core value field names (val/data/measurement)", func() {
		data, err := out.extractMessageData(newMsg(`{"val": 7}`, map[string]string{
			"location_path": "enterprise.site.area.line",
			"tag_name":      "temperature",
		}))

		Expect(err).NotTo(HaveOccurred())
		Expect(data["temperature"]).To(Equal(json.Number("7")))
	})

	It("drops a payload with no scalar value field instead of publishing the whole map", func() {
		// No value/val/data/measurement field and no field named after the tag ->
		// parseUMHMessage falls back to the whole payload map; the output must NOT
		// publish that as a stringified-map metric.
		data, err := out.extractMessageData(newMsg(`{"foo": 1, "bar": 2}`, map[string]string{
			"location_path": "enterprise.site.area.line",
			"tag_name":      "temperature",
		}))

		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(BeEmpty())
	})

	It("produces no metric when location_path is missing (message would be dropped)", func() {
		data, err := out.extractMessageData(newMsg(umhCore, map[string]string{
			"tag_name":     "temperature",
			"virtual_path": "motor",
		}))

		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(BeEmpty())
	})
})

// Guards the construction seam: the auto-extract path dereferences
// s.formatConverter, which newSparkplugOutput must wire up. A struct-literal test
// can't catch a regression that drops that initialization, so build via the real
// constructor here.
var _ = Describe("Sparkplug B output construction", func() {
	It("wires up the FormatConverter and extracts via the constructed instance", func() {
		parsed, err := sparkplugOutputConfigSpec().ParseYAML(`
mqtt:
  urls: ["tcp://localhost:1883"]
identity:
  group_id: FactoryA
  edge_node_id: benthos-fix
`, nil)
		Expect(err).NotTo(HaveOccurred())

		out, err := newSparkplugOutput(parsed, service.MockResources())
		Expect(err).NotTo(HaveOccurred())
		Expect(out.formatConverter).NotTo(BeNil())

		msg := service.NewMessage([]byte(`{"value": 42, "timestamp_ms": 1640995200000}`))
		msg.MetaSet("location_path", "enterprise.site.area.line")
		msg.MetaSet("tag_name", "temperature")
		msg.MetaSet("virtual_path", "modbus")

		data, err := out.extractMessageData(msg)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(HaveKey("modbus:temperature"))
	})
})
