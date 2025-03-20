package benthos

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
)

var _ = Describe("Benthos YAML Template", func() {
	type testCase struct {
		config      *config.BenthosServiceConfig
		expected    []string
		notExpected []string
	}

	DescribeTable("template rendering",
		func(tc testCase) {
			var b bytes.Buffer
			err := benthosYamlTemplate.Execute(&b, tc.config)
			Expect(err).NotTo(HaveOccurred())

			result := b.String()

			// Check for expected strings
			for _, exp := range tc.expected {
				Expect(result).To(ContainSubstring(exp))
			}

			// Check for strings that should not be present
			for _, notExp := range tc.notExpected {
				Expect(result).NotTo(ContainSubstring(notExp))
			}
		},
		Entry("should render empty stdout output correctly",
			testCase{
				config: &config.BenthosServiceConfig{
					Output: map[string]interface{}{
						"stdout": map[string]interface{}{},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"output:",
					"  stdout:",
					"    {}",
				},
				notExpected: []string{
					"output: []",
				},
			}),
		Entry("should render configured output correctly",
			testCase{
				config: &config.BenthosServiceConfig{
					Output: map[string]interface{}{
						"kafka": map[string]interface{}{
							"topic": "test-topic",
						},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"output:",
					"  kafka:",
					"    topic: test-topic",
				},
				notExpected: []string{
					"output: []",
					"    {}",
				},
			}),
		Entry("should render empty input correctly",
			testCase{
				config: &config.BenthosServiceConfig{
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"input: []",
				},
				notExpected: nil,
			}),
	)
})
