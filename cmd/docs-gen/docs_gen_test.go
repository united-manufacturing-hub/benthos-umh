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

package main

import (
	"encoding/json"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var inputs, outputs = sections[0], sections[1]

var _ = Describe("linkRewriter", func() {
	rw := linkRewriter{from: inputs, documented: map[string]bool{"inputs/mqtt": true, "outputs/mqtt": true}}

	It("points a documented input at its page in the same section", func() {
		Expect(rw.rewrite("[x](https://docs.redpanda.com/redpanda-connect/components/inputs/mqtt/#tls-enabled)")).
			To(Equal("[x](mqtt.md)"))
	})

	It("points a documented output at its page in the other section", func() {
		Expect(rw.rewrite("[x](https://docs.redpanda.com/redpanda-connect/components/outputs/mqtt/)")).
			To(Equal("[x](../../output/connect/mqtt.md)"))
	})

	It("points the upstream catalog at the generated index", func() {
		Expect(rw.rewrite("[x](https://docs.redpanda.com/redpanda-connect/components/outputs/about/)")).
			To(Equal("[x](../../output/connect/README.md)"))
	})

	It("keeps links to components without a page", func() {
		link := "[x](https://docs.redpanda.com/redpanda-connect/components/inputs/kafka/)"
		Expect(rw.rewrite(link)).To(Equal(link))
	})
})

var _ = Describe("updateSummary", func() {
	const summary = `- [Input](input/README.md)
  - [Modbus](input/modbus.md)
  - [Redpanda Connect inputs](input/connect/README.md)
    - [Old](input/connect/old.md)
  - [More](https://docs.redpanda.com/redpanda-connect/components/inputs/about/)`

	It("replaces the generated entries before the upstream link", func() {
		out, err := updateSummary(summary, inputs, []entry{{Name: "mqtt", Title: "MQTT"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(`- [Input](input/README.md)
  - [Modbus](input/modbus.md)
  - [Redpanda Connect inputs](input/connect/README.md)
    - [MQTT](input/connect/mqtt.md)
  - [More](https://docs.redpanda.com/redpanda-connect/components/inputs/about/)`))

		again, err := updateSummary(out, inputs, []entry{{Name: "mqtt", Title: "MQTT"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(again).To(Equal(out))
	})

	It("fails when the section has no upstream link to place entries before", func() {
		_, err := updateSummary("- [Input](input/README.md)", inputs, nil)
		Expect(err).To(MatchError(ContainSubstring("/components/inputs/about/")))
	})
})

var _ = Describe("configYAML", func() {
	c := component{Name: "demo", Config: field{Children: []field{
		{Name: "url", Type: "string", Kind: "scalar"},
		{Name: "timeout", Type: "string", Kind: "scalar", Default: json.RawMessage(`"5s"`)},
		{Name: "tuning", Type: "int", Kind: "scalar", Default: json.RawMessage(`3`), IsAdvanced: true},
		{Name: "secret", Type: "string", Kind: "scalar", Default: json.RawMessage(`""`)},
		{Name: "old", Type: "string", Kind: "scalar", IsDeprecated: true},
	}}}

	It("leaves advanced, deprecated and hidden fields out of the common snippet", func() {
		out, err := configYAML(inputs, c, map[string]bool{"secret": true}, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(`input:
  label: ""
  demo:
    url: "" # No default (required)
    timeout: 5s
`))
	})

	It("shows advanced fields in the full snippet", func() {
		out, err := configYAML(inputs, c, nil, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("tuning: 3"))
		Expect(out).NotTo(ContainSubstring("old:"))
	})
})

var _ = Describe("generate", func() {
	schema := &schemaDoc{Inputs: []component{{Name: "mqtt", Status: "stable", Config: field{Children: []field{
		{Name: "urls", Type: "string", Kind: "array"},
	}}}}}
	noOverlay := func(section, string) (string, error) { return "", nil }

	It("fails on a component the binary does not include", func() {
		_, err := generate(schema, allowlist{Inputs: []entry{{Name: "opcua", Title: "OPC UA"}}}, "v0", noOverlay)
		Expect(err).To(MatchError(ContainSubstring(`"opcua"`)))
	})

	It("fails when hide names a field that does not exist", func() {
		_, err := generate(schema, allowlist{Inputs: []entry{{Name: "mqtt", Title: "MQTT", Hide: []string{"topic"}}}}, "v0", noOverlay)
		Expect(err).To(MatchError(ContainSubstring("topic")))
	})

	It("inserts the overlay into the page", func() {
		files, err := generate(schema, allowlist{Inputs: []entry{{Name: "mqtt", Title: "MQTT"}}}, "v0",
			func(section, string) (string, error) { return "## Using it with UMH\n\nHello.", nil })
		Expect(err).NotTo(HaveOccurred())
		Expect(string(files["input/connect/mqtt.md"])).To(ContainSubstring("## Using it with UMH\n\nHello."))
	})
})

// The pages in components.yaml, rendered from the Redpanda Connect version in
// go.mod. This catches an upstream bump that renames a component or a hidden
// field, or leaves AsciiDoc in the Markdown doc strings.
var _ = Describe("components.yaml", func() {
	It("renders every listed component without AsciiDoc left over", func() {
		raw, err := os.ReadFile("components.yaml")
		Expect(err).NotTo(HaveOccurred())
		var list allowlist
		Expect(yaml.Unmarshal(raw, &list)).To(Succeed())

		schema, err := loadSchema(service.GlobalEnvironment(), "test")
		Expect(err).NotTo(HaveOccurred())
		files, err := generate(schema, list, "test", func(section, string) (string, error) { return "", nil })
		Expect(err).NotTo(HaveOccurred())

		for path, content := range files {
			for _, token := range []string{"xref:", "ifdef::", "ifndef::", "endif::", "include::", "<<"} {
				Expect(string(content)).NotTo(ContainSubstring(token), "%s contains %q", path, token)
			}
		}
	})
})
