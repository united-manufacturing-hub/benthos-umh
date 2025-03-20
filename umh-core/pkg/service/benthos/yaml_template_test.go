package benthos

import (
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
			yamlStr, err := RenderBenthosYAML(
				tc.config.Input,
				tc.config.Output,
				tc.config.Pipeline,
				tc.config.CacheResources,
				tc.config.RateLimitResources,
				tc.config.Buffer,
				tc.config.MetricsPort,
				tc.config.LogLevel,
			)
			Expect(err).NotTo(HaveOccurred())

			// Check for expected strings
			for _, exp := range tc.expected {
				Expect(yamlStr).To(ContainSubstring(exp))
			}

			// Check for strings that should not be present
			for _, notExp := range tc.notExpected {
				Expect(yamlStr).NotTo(ContainSubstring(notExp))
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
					"  stdout: {}",
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
		Entry("should render Kafka input with processor and AWS S3 output",
			testCase{
				config: &config.BenthosServiceConfig{
					Input: map[string]interface{}{
						"kafka": map[string]interface{}{
							"addresses":      []string{"localhost:9092"},
							"topics":         []string{"foo", "bar"},
							"consumer_group": "foogroup",
						},
					},
					Pipeline: map[string]interface{}{
						"processors": []map[string]interface{}{
							{
								"mapping": map[string]interface{}{
									"value": `"%vend".format(content().uppercase().string())`,
								},
							},
						},
					},
					Output: map[string]interface{}{
						"aws_s3": map[string]interface{}{
							"bucket": "my-bucket",
							"path":   "${! meta(\"kafka_topic\") }/${! json(\"message.id\") }.json",
						},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"input:",
					"  kafka:",
					"    addresses:",
					"    consumer_group:",
					"    topics:",
					"pipeline:",
					"  processors:",
					"    - mapping:",
					"output:",
					"  aws_s3:",
					"    bucket:",
					"    path:",
				},
				notExpected: []string{
					"input: []",
					"pipeline: []",
					"output: []",
				},
			}),
		Entry("should render configuration with redis streams",
			testCase{
				config: &config.BenthosServiceConfig{
					Input: map[string]interface{}{
						"redis_streams": map[string]interface{}{
							"url": "tcp://localhost:6379",
							"streams": []string{
								"benthos_stream",
							},
							"body_key":       "body",
							"consumer_group": "benthos_group",
						},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"input:",
					"  redis_streams:",
					"    url:",
					"    streams:",
					"    body_key:",
					"    consumer_group:",
				},
				notExpected: []string{
					"input: []",
				},
			}),
		Entry("should render configuration with inproc",
			testCase{
				config: &config.BenthosServiceConfig{
					Input: map[string]interface{}{
						"inproc": map[string]interface{}{
							"name": "test-stream",
						},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"input:",
					"  inproc:",
					"    name:",
				},
				notExpected: []string{
					"input: []",
				},
			}),
		Entry("should render configuration with rate limit resources",
			testCase{
				config: &config.BenthosServiceConfig{
					RateLimitResources: []map[string]interface{}{
						{
							"local": map[string]interface{}{
								"count":    500,
								"interval": "1s",
							},
						},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"rate_limit_resources:",
					"  - local:",
					"      count:",
					"      interval:",
				},
				notExpected: []string{
					"rate_limit_resources: []",
				},
			}),
		Entry("should render configuration with branch processor",
			testCase{
				config: &config.BenthosServiceConfig{
					Pipeline: map[string]interface{}{
						"processors": []map[string]interface{}{
							{
								"branch": map[string]interface{}{
									"processors": []map[string]map[string]interface{}{
										{
											"mapping": {
												"value": "root.foo = this.bar",
											},
										},
									},
								},
							},
						},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"pipeline:",
					"  processors:",
					"    - branch:",
					"        processors:",
				},
				notExpected: []string{
					"pipeline: []",
				},
			}),
		Entry("should render stdin -> pipeline processors -> stdout",
			testCase{
				config: &config.BenthosServiceConfig{
					Input: map[string]interface{}{
						"stdin": map[string]interface{}{},
					},
					Pipeline: map[string]interface{}{
						"processors": []map[string]interface{}{
							{
								"sleep": map[string]interface{}{
									"duration": "500ms",
								},
							},
							{
								"mapping": map[string]interface{}{
									// If you want multi-line with `|` in your final YAML,
									// just store the string as is:
									"value": `root.doc = this
root.first_name = this.names.index(0).uppercase()
root.last_name = this.names.index(-1).hash("sha256").encode("base64")`,
								},
							},
						},
					},
					Output: map[string]interface{}{
						"stdout": map[string]interface{}{},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				},
				expected: []string{
					"input:",
					"  stdin: {}",
					"pipeline:",
					"processors:",
					"sleep:",
					"duration: 500ms",
					"- mapping:",
					"root.first_name = this.names.index(0).uppercase()",
					"output:",
					"  stdout: {}",
				},
				notExpected: []string{
					// any lines you specifically don't want to appear
					"input: []",
					"output: []",
				},
			},
		),
	)
})
