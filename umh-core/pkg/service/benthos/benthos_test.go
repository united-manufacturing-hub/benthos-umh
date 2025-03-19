package benthos

import (
	"context"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Benthos Service", func() {
	var (
		service *BenthosService
		client  *MockHTTPClient
	)

	BeforeEach(func() {
		client = NewMockHTTPClient()
		service = NewDefaultBenthosService("test", WithHTTPClient(client))
	})

	Describe("GetHealthCheckAndMetrics", func() {
		Context("with valid metrics port", func() {
			BeforeEach(func() {
				client.SetReadyStatus(200, true, true, "")
				client.SetMetricsResponse(MetricsConfig{
					Input: MetricsConfigInput{
						Received:     10,
						ConnectionUp: 1,
						LatencyNS: LatencyConfig{
							P50:   1000000,
							P90:   2000000,
							P99:   3000000,
							Sum:   1500000,
							Count: 5,
						},
					},
					Output: MetricsConfigOutput{
						Sent:         8,
						BatchSent:    2,
						ConnectionUp: 1,
						LatencyNS: LatencyConfig{
							P50:   1000000,
							P90:   2000000,
							P99:   3000000,
							Sum:   1500000,
							Count: 5,
						},
					},
					Processors: []ProcessorConfig{
						{
							Path:          "/pipeline/processors/0",
							Label:         "0",
							Received:      5,
							BatchReceived: 1,
							Sent:          5,
							BatchSent:     1,
							Error:         0,
							LatencyNS: LatencyConfig{
								P50:   1000000,
								P90:   2000000,
								P99:   3000000,
								Sum:   1500000,
								Count: 5,
							},
						},
					},
				})
			})

			It("should return health check and metrics", func() {
				ctx := context.Background()
				status, err := service.GetHealthCheckAndMetrics(ctx, "test", 4195)
				Expect(err).NotTo(HaveOccurred())
				Expect(status.HealthCheck.IsReady).To(BeTrue())
				Expect(status.Metrics.Input.Received).To(Equal(int64(10)))
				Expect(status.Metrics.Input.ConnectionUp).To(Equal(int64(1)))
				Expect(status.Metrics.Output.Sent).To(Equal(int64(8)))
				Expect(status.Metrics.Output.BatchSent).To(Equal(int64(2)))
				Expect(status.Metrics.Process.Processors).To(HaveLen(1))
				proc := status.Metrics.Process.Processors["/pipeline/processors/0"]
				Expect(proc.Label).To(Equal("0"))
				Expect(proc.Received).To(Equal(int64(5)))
				Expect(proc.BatchReceived).To(Equal(int64(1)))
				Expect(proc.Sent).To(Equal(int64(5)))
				Expect(proc.BatchSent).To(Equal(int64(1)))
				Expect(proc.Error).To(Equal(int64(0)))
				Expect(proc.LatencyNS.P50).To(Equal(float64(1000000)))
				Expect(proc.LatencyNS.P90).To(Equal(float64(2000000)))
				Expect(proc.LatencyNS.P99).To(Equal(float64(3000000)))
				Expect(proc.LatencyNS.Sum).To(Equal(float64(1500000)))
				Expect(proc.LatencyNS.Count).To(Equal(int64(5)))
			})
		})

		Context("with invalid metrics port", func() {
			BeforeEach(func() {
				client.SetResponse("/ping", MockResponse{
					StatusCode: 500,
					Body:       []byte("connection refused"),
				})
				client.SetResponse("/ready", MockResponse{
					StatusCode: 500,
					Body:       []byte(`{"error": "connection refused"}`),
				})
				client.SetResponse("/version", MockResponse{
					StatusCode: 500,
					Body:       []byte(`{"error": "connection refused"}`),
				})
			})

			It("should return error", func() {
				ctx := context.Background()
				_, err := service.GetHealthCheckAndMetrics(ctx, "test", 4195)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to check liveness: failed to execute request for /ping: connection refused"))
			})
		})

		Context("with context cancellation", func() {
			BeforeEach(func() {
				client.SetResponse("/ready", MockResponse{
					StatusCode: 200,
					Delay:      100 * time.Millisecond,
				})
			})

			It("should return error when context is cancelled", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()

				_, err := service.GetHealthCheckAndMetrics(ctx, "test", 4195)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("context deadline exceeded"))
			})
		})
	})

	Describe("GenerateS6ConfigForBenthos", func() {
		Context("with minimal configuration", func() {
			It("should generate valid YAML", func() {
				cfg := &config.BenthosServiceConfig{
					MetricsPort: 4195,
					LogLevel:    "INFO",
				}

				s6Config, err := service.GenerateS6ConfigForBenthos(cfg, "test")
				Expect(err).NotTo(HaveOccurred())
				Expect(s6Config.ConfigFiles).To(HaveKey("benthos.yaml"))
				yaml := s6Config.ConfigFiles["benthos.yaml"]

				Expect(yaml).To(ContainSubstring("input: []"))
				Expect(yaml).To(ContainSubstring("output: []"))
				Expect(yaml).To(ContainSubstring("pipeline: []"))
				Expect(yaml).To(ContainSubstring("http:\n  address: \"0.0.0.0:4195\""))
				Expect(yaml).To(ContainSubstring("logger:\n  level: \"INFO\""))
			})
		})

		Context("with complete configuration", func() {
			It("should generate valid YAML with all sections", func() {
				cfg := &config.BenthosServiceConfig{
					Input: []map[string]interface{}{
						{"mqtt": map[string]interface{}{"topic": "test/topic"}},
					},
					Output: []map[string]interface{}{
						{"kafka": map[string]interface{}{"topic": "test-output"}},
					},
					Pipeline: []map[string]interface{}{
						{
							"processors": []map[string]interface{}{
								{"text": map[string]interface{}{"operator": "to_upper"}},
							},
						},
					},
					CacheResources: []map[string]interface{}{
						{"memory": map[string]interface{}{"ttl": "60s"}},
					},
					RateLimitResources: []map[string]interface{}{
						{"local": map[string]interface{}{"count": "100"}},
					},
					Buffer: []map[string]interface{}{
						{"memory": map[string]interface{}{"limit": "10MB"}},
					},
					MetricsPort: 4195,
					LogLevel:    "INFO",
				}

				s6Config, err := service.GenerateS6ConfigForBenthos(cfg, "test")
				Expect(err).NotTo(HaveOccurred())
				Expect(s6Config.ConfigFiles).To(HaveKey("benthos.yaml"))
				yaml := s6Config.ConfigFiles["benthos.yaml"]

				Expect(yaml).To(ContainSubstring("mqtt:\n    topic: test/topic"))
				Expect(yaml).To(ContainSubstring("kafka:\n    topic: test-output"))
				Expect(yaml).To(ContainSubstring("processors:\n    - text:\n        operator: to_upper"))
				Expect(yaml).To(ContainSubstring("- memory:\n      ttl: 60s"))
				Expect(yaml).To(ContainSubstring("- local:\n      count: 100"))
				Expect(yaml).To(ContainSubstring("memory:\n    limit: 10MB"))
				Expect(yaml).To(ContainSubstring("http:\n  address: \"0.0.0.0:4195\""))
				Expect(yaml).To(ContainSubstring("logger:\n  level: \"INFO\""))
			})
		})

		Context("with nil configuration", func() {
			It("should return error", func() {
				s6Config, err := service.GenerateS6ConfigForBenthos(nil, "test")
				Expect(err).To(HaveOccurred())
				Expect(s6Config).To(Equal(config.S6ServiceConfig{}))
			})
		})
	})

	Context("Log Analysis", func() {
		var service *BenthosService

		BeforeEach(func() {
			service = NewDefaultBenthosService("test")
		})

		Context("IsLogsFine", func() {
			It("should return true when there are no logs", func() {
				Expect(service.IsLogsFine([]string{})).To(BeTrue())
			})

			It("should detect official Benthos error logs", func() {
				logs := []string{
					`level=error msg="failed to connect to broker"`,
				}
				Expect(service.IsLogsFine(logs)).To(BeFalse())
			})

			It("should detect critical warnings in official Benthos logs", func() {
				logs := []string{
					`level=warn msg="failed to process message"`,
					`level=warn msg="connection lost to server"`,
					`level=warn msg="unable to reach endpoint"`,
				}
				Expect(service.IsLogsFine(logs)).To(BeFalse())
			})

			It("should ignore non-critical warnings in official Benthos logs", func() {
				logs := []string{
					`level=warn msg="rate limit applied"`,
					`level=warn msg="message batch partially processed"`,
				}
				Expect(service.IsLogsFine(logs)).To(BeTrue())
			})

			It("should detect configuration file read errors", func() {
				logs := []string{
					`configuration file read error: file not found`,
				}
				Expect(service.IsLogsFine(logs)).To(BeFalse())
			})

			It("should detect logger creation errors", func() {
				logs := []string{
					`failed to create logger: invalid log level`,
				}
				Expect(service.IsLogsFine(logs)).To(BeFalse())
			})

			It("should detect linter errors", func() {
				logs := []string{
					`Config lint error: invalid input type`,
					`shutting down due to linter errors`,
				}
				Expect(service.IsLogsFine(logs)).To(BeFalse())
			})

			It("should ignore error-like strings in message content", func() {
				logs := []string{
					`level=info msg="Processing message: configuration file read error in payload"`,
					`level=info msg="Log contains warning: user notification"`,
					`level=info msg="Error rate metrics collected"`,
				}
				Expect(service.IsLogsFine(logs)).To(BeTrue())
			})

			It("should ignore error patterns not at start of line", func() {
				logs := []string{
					`level=info msg="User reported: configuration file read error"`,
					`level=info msg="System status: failed to create logger mentioned in docs"`,
					`level=info msg="Documentation: Config lint error examples"`,
				}
				Expect(service.IsLogsFine(logs)).To(BeTrue())
			})

			It("should handle mixed log types correctly", func() {
				logs := []string{
					`level=info msg="Starting up Benthos service"`,
					`level=warn msg="rate limit applied"`,
					`level=info msg="Processing message: warning in content"`,
					`level=error msg="failed to connect"`, // This should trigger false
				}
				Expect(service.IsLogsFine(logs)).To(BeFalse())
			})

			It("should handle malformed Benthos logs gracefully", func() {
				logs := []string{
					`levelerror msg="broken log format"`,
					`level=info missing quote`,
					`random text with warning and error keywords`,
				}
				Expect(service.IsLogsFine(logs)).To(BeTrue())
			})
		})
	})

	Context("Metrics Analysis", func() {
		var service *BenthosService

		BeforeEach(func() {
			service = NewDefaultBenthosService("test")
		})

		Context("IsMetricsErrorFree", func() {
			It("should return true when there are no errors", func() {
				metrics := Metrics{
					Input: InputMetrics{
						ConnectionFailed: 5, // Should be ignored now
						ConnectionLost:   1,
						ConnectionUp:     1,
						Received:         100,
					},
					Output: OutputMetrics{
						Error:            0,
						ConnectionFailed: 3, // Should be ignored now
						ConnectionLost:   0,
						ConnectionUp:     1,
						Sent:             90,
						BatchSent:        10,
					},
					Process: ProcessMetrics{
						Processors: map[string]ProcessorMetrics{
							"proc1": {
								Error:         0,
								Received:      100,
								Sent:          100,
								BatchReceived: 10,
								BatchSent:     10,
							},
						},
					},
				}
				Expect(service.IsMetricsErrorFree(metrics)).To(BeTrue())
			})

			It("should detect output errors", func() {
				metrics := Metrics{
					Output: OutputMetrics{
						Error: 1,
					},
				}
				Expect(service.IsMetricsErrorFree(metrics)).To(BeFalse())
			})

			It("should ignore connection failures", func() {
				metrics := Metrics{
					Input: InputMetrics{
						ConnectionFailed: 1,
					},
					Output: OutputMetrics{
						ConnectionFailed: 1,
					},
				}
				Expect(service.IsMetricsErrorFree(metrics)).To(BeTrue())
			})

			It("should detect processor errors", func() {
				metrics := Metrics{
					Process: ProcessMetrics{
						Processors: map[string]ProcessorMetrics{
							"proc1": {
								Error: 1,
							},
						},
					},
				}
				Expect(service.IsMetricsErrorFree(metrics)).To(BeFalse())
			})

			It("should detect errors in any processor", func() {
				metrics := Metrics{
					Process: ProcessMetrics{
						Processors: map[string]ProcessorMetrics{
							"proc1": {Error: 0},
							"proc2": {Error: 1},
							"proc3": {Error: 0},
						},
					},
				}
				Expect(service.IsMetricsErrorFree(metrics)).To(BeFalse())
			})
		})
	})
})
