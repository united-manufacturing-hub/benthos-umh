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
		service     *BenthosService
		client      *MockHTTPClient
		tick        uint64
		benthosName string
		serviceName string
	)

	BeforeEach(func() {
		client = NewMockHTTPClient()
		service = NewDefaultBenthosService(benthosName, WithHTTPClient(client))
		tick = 0
		serviceName = service.getS6ServiceName(benthosName)
		// Add the service to the S6 manager
		err := service.AddBenthosToS6Manager(context.Background(), &config.BenthosServiceConfig{
			MetricsPort: 4195,
			LogLevel:    "info",
		}, benthosName)
		Expect(err).NotTo(HaveOccurred())

		// Reconcile the S6 manager
		err, _ = service.ReconcileManager(context.Background(), tick)
		Expect(err).NotTo(HaveOccurred())

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
			Processors: []ProcessorMetricsConfig{
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
					Processors: []ProcessorMetricsConfig{
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
				status, err := service.GetHealthCheckAndMetrics(ctx, serviceName, 4195, tick)
				tick++
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
				_, err := service.GetHealthCheckAndMetrics(ctx, serviceName, 4195, tick)
				tick++
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

				_, err := service.GetHealthCheckAndMetrics(ctx, serviceName, 4195, tick)
				tick++
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
				Expect(yaml).To(ContainSubstring("pipeline:"))
				Expect(yaml).To(ContainSubstring("processors: []"))
				Expect(yaml).To(ContainSubstring("http:\n  address: 0.0.0.0:4195"))
				Expect(yaml).To(ContainSubstring("logger:\n  level: INFO"))
			})
		})

		Context("with complete configuration", func() {
			It("should generate valid YAML with all sections", func() {
				cfg := &config.BenthosServiceConfig{
					Input: map[string]interface{}{
						"mqtt": map[string]interface{}{"topic": "test/topic"},
					},
					Output: map[string]interface{}{
						"kafka": map[string]interface{}{"topic": "test-output"},
					},
					Pipeline: map[string]interface{}{
						"processors": []map[string]interface{}{
							{"text": map[string]interface{}{"operator": "to_upper"}},
						},
					},
					CacheResources: []map[string]interface{}{
						{"memory": map[string]interface{}{"ttl": "60s"}},
					},
					RateLimitResources: []map[string]interface{}{
						{"local": map[string]interface{}{"count": "100"}},
					},
					Buffer: map[string]interface{}{
						"memory": map[string]interface{}{"limit": "10MB"},
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
				Expect(yaml).To(ContainSubstring("- local:\n      count: \"100\""))
				Expect(yaml).To(ContainSubstring("memory:\n    limit: 10MB"))
				Expect(yaml).To(ContainSubstring("http:\n  address: 0.0.0.0:4195"))
				Expect(yaml).To(ContainSubstring("logger:\n  level: INFO"))
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

	Context("Tick-based throughput tracking", Label("tick_based_throughput_tracking"), func() {
		var (
			service       *BenthosService
			mockClient    *MockHTTPClient
			tick          uint64
			benthosName   = "test"
			metricsPort   = 8080
			s6ServiceName string
		)

		BeforeEach(func() {
			mockClient = NewMockHTTPClient()
			service = NewDefaultBenthosService(benthosName, WithHTTPClient(mockClient))
			tick = 0

			// Add the service to the S6 manager
			err := service.AddBenthosToS6Manager(context.Background(), &config.BenthosServiceConfig{
				MetricsPort: metricsPort,
				LogLevel:    "info",
			}, benthosName)
			Expect(err).NotTo(HaveOccurred())

			s6ServiceName = service.getS6ServiceName(benthosName)
			s6ServiceName = s6ServiceName

			// Reconcile the S6 manager
			// TODO: maybe use a mock for the S6 manager, there is one in fsm/mock.go
			err, _ = service.ReconcileManager(context.Background(), tick)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should calculate throughput based on ticks", func() {
			// Mock metrics responses for two consecutive ticks
			tick1Response := `# HELP input_received Benthos Counter metric
# TYPE input_received counter
input_received{path="root.input"} 100

# HELP output_sent Benthos Counter metric
# TYPE output_sent counter
output_sent{path="root.output"} 90

# HELP output_batch_sent Benthos Counter metric
# TYPE output_batch_sent counter
output_batch_sent{path="root.output"} 10

# HELP processor_received Benthos Counter metric
# TYPE processor_received counter
processor_received{label="0",path="root.pipeline.processors.0"} 100

# HELP processor_sent Benthos Counter metric
# TYPE processor_sent counter
processor_sent{label="0",path="root.pipeline.processors.0"} 95

# HELP processor_batch_received Benthos Counter metric
# TYPE processor_batch_received counter
processor_batch_received{label="0",path="root.pipeline.processors.0"} 10

# HELP processor_batch_sent Benthos Counter metric
# TYPE processor_batch_sent counter
processor_batch_sent{label="0",path="root.pipeline.processors.0"} 9
`

			tick2Response := `# HELP input_received Benthos Counter metric
# TYPE input_received counter
input_received{path="root.input"} 200

# HELP output_sent Benthos Counter metric
# TYPE output_sent counter
output_sent{path="root.output"} 180

# HELP output_batch_sent Benthos Counter metric
# TYPE output_batch_sent counter
output_batch_sent{path="root.output"} 20

# HELP processor_received Benthos Counter metric
# TYPE processor_received counter
processor_received{label="0",path="root.pipeline.processors.0"} 200

# HELP processor_sent Benthos Counter metric
# TYPE processor_sent counter
processor_sent{label="0",path="root.pipeline.processors.0"} 195

# HELP processor_batch_received Benthos Counter metric
# TYPE processor_batch_received counter
processor_batch_received{label="0",path="root.pipeline.processors.0"} 20

# HELP processor_batch_sent Benthos Counter metric
# TYPE processor_batch_sent counter
processor_batch_sent{label="0",path="root.pipeline.processors.0"} 19
`

			// Setup mock responses
			mockClient.SetResponse("/metrics", MockResponse{
				StatusCode: 200,
				Body:       []byte(tick1Response),
			})
			mockClient.SetResponse("/ready", MockResponse{
				StatusCode: 200,
				Body:       []byte("{}"),
			})

			status1, err := service.Status(context.Background(), benthosName, metricsPort, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// Verify initial state
			Expect(status1.BenthosStatus.Metrics.Input.Received).To(Equal(int64(100)))
			Expect(status1.BenthosStatus.Metrics.Output.Sent).To(Equal(int64(90)))
			Expect(status1.BenthosStatus.Metrics.Output.BatchSent).To(Equal(int64(10)))
			Expect(status1.BenthosStatus.MetricsState.Input.MessagesPerTick).To(Equal(float64(100)))
			Expect(status1.BenthosStatus.MetricsState.Output.MessagesPerTick).To(Equal(float64(90)))
			Expect(status1.BenthosStatus.MetricsState.Output.BatchesPerTick).To(Equal(float64(10)))

			// Setup second tick response
			mockClient.SetResponse("/metrics", MockResponse{
				StatusCode: 200,
				Body:       []byte(tick2Response),
			})
			mockClient.SetResponse("/ready", MockResponse{
				StatusCode: 200,
				Body:       []byte("{}"),
			})
			status2, err := service.Status(context.Background(), benthosName, metricsPort, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// Verify throughput calculations
			Expect(status2.BenthosStatus.Metrics.Input.Received).To(Equal(int64(200)))
			Expect(status2.BenthosStatus.Metrics.Output.Sent).To(Equal(int64(180)))
			Expect(status2.BenthosStatus.Metrics.Output.BatchSent).To(Equal(int64(20)))
			Expect(status2.BenthosStatus.MetricsState.Input.MessagesPerTick).To(Equal(float64(100))) // (200-100)/1
			Expect(status2.BenthosStatus.MetricsState.Output.MessagesPerTick).To(Equal(float64(90))) // (180-90)/1
			Expect(status2.BenthosStatus.MetricsState.Output.BatchesPerTick).To(Equal(float64(10)))  // (20-10)/1
		})

		It("should handle counter resets", func() {
			// First tick with normal values
			tick1Response := `# HELP input_received Benthos Counter metric
# TYPE input_received counter
input_received{path="root.input"} 100
`

			// Second tick with reset counter (lower than previous)
			tick2Response := `# HELP input_received Benthos Counter metric
# TYPE input_received counter
input_received{path="root.input"} 30
`

			// Setup mock responses
			mockClient.SetResponse("/metrics", MockResponse{
				StatusCode: 200,
				Body:       []byte(tick1Response),
			})
			mockClient.SetResponse("/ready", MockResponse{
				StatusCode: 200,
				Body:       []byte("{}"),
			})
			status1, err := service.Status(context.Background(), benthosName, metricsPort, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// Verify initial state
			Expect(status1.BenthosStatus.Metrics.Input.Received).To(Equal(int64(100)))
			Expect(status1.BenthosStatus.MetricsState.Input.MessagesPerTick).To(Equal(float64(100)))
			Expect(status1.BenthosStatus.MetricsState.Input.LastCount).To(Equal(int64(100)))

			// Setup second tick response
			mockClient.SetResponse("/metrics", MockResponse{
				StatusCode: 200,
				Body:       []byte(tick2Response),
			})
			mockClient.SetResponse("/ready", MockResponse{
				StatusCode: 200,
				Body:       []byte("{}"),
			})
			status2, err := service.Status(context.Background(), benthosName, metricsPort, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// After reset, we should treat the new value as the baseline
			Expect(status2.BenthosStatus.Metrics.Input.Received).To(Equal(int64(30)))
			Expect(status2.BenthosStatus.MetricsState.Input.MessagesPerTick).To(Equal(float64(30)))
			Expect(status2.BenthosStatus.MetricsState.Input.LastCount).To(Equal(int64(30)))
		})

		It("should detect inactivity", func() {
			// First tick with some activity
			tick1Response := `# HELP input_received Benthos Counter metric
# TYPE input_received counter
input_received{path="root.input"} 100

# HELP output_sent Benthos Counter metric
# TYPE output_sent counter
output_sent{path="root.output"} 90
`

			// Second tick with no change in counters
			tick2Response := `# HELP input_received Benthos Counter metric
# TYPE input_received counter
input_received{path="root.input"} 100

# HELP output_sent Benthos Counter metric
# TYPE output_sent counter
output_sent{path="root.output"} 90
`

			// Setup mock responses
			mockClient.SetResponse("/metrics", MockResponse{
				StatusCode: 200,
				Body:       []byte(tick1Response),
			})
			mockClient.SetResponse("/ready", MockResponse{
				StatusCode: 200,
				Body:       []byte("{}"),
			})
			status1, err := service.Status(context.Background(), benthosName, metricsPort, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// Verify initial state
			Expect(status1.BenthosStatus.Metrics.Input.Received).To(Equal(int64(100)))
			Expect(status1.BenthosStatus.MetricsState.Input.MessagesPerTick).To(Equal(float64(100)))
			Expect(status1.BenthosStatus.MetricsState.IsActive).To(BeTrue())

			// Setup second tick response
			mockClient.SetResponse("/metrics", MockResponse{
				StatusCode: 200,
				Body:       []byte(tick2Response),
			})
			mockClient.SetResponse("/ready", MockResponse{
				StatusCode: 200,
				Body:       []byte("{}"),
			})
			status2, err := service.Status(context.Background(), benthosName, metricsPort, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// No change in counters should indicate inactivity
			Expect(status2.BenthosStatus.Metrics.Input.Received).To(Equal(int64(100)))
			Expect(status2.BenthosStatus.MetricsState.Input.MessagesPerTick).To(Equal(float64(0)))
			Expect(status2.BenthosStatus.MetricsState.IsActive).To(BeFalse())
		})

		It("should track component throughput", func() {
			// First tick with initial component metrics
			tick1Response := `# HELP input_received Benthos Counter metric
# TYPE input_received counter
input_received{path="root.input"} 100

# HELP output_sent Benthos Counter metric
# TYPE output_sent counter
output_sent{path="root.output"} 90

# HELP processor_received Benthos Counter metric
# TYPE processor_received counter
processor_received{label="0",path="root.pipeline.processors.0"} 100
processor_received{label="1",path="root.pipeline.processors.1"} 95

# HELP processor_sent Benthos Counter metric
# TYPE processor_sent counter
processor_sent{label="0",path="root.pipeline.processors.0"} 95
processor_sent{label="1",path="root.pipeline.processors.1"} 90

# HELP processor_batch_received Benthos Counter metric
# TYPE processor_batch_received counter
processor_batch_received{label="0",path="root.pipeline.processors.0"} 10
processor_batch_received{label="1",path="root.pipeline.processors.1"} 9

# HELP processor_batch_sent Benthos Counter metric
# TYPE processor_batch_sent counter
processor_batch_sent{label="0",path="root.pipeline.processors.0"} 9
processor_batch_sent{label="1",path="root.pipeline.processors.1"} 8
`

			// Second tick with updated component metrics
			tick2Response := `# HELP input_received Benthos Counter metric
# TYPE input_received counter
input_received{path="root.input"} 200

# HELP output_sent Benthos Counter metric
# TYPE output_sent counter
output_sent{path="root.output"} 180

# HELP processor_received Benthos Counter metric
# TYPE processor_received counter
processor_received{label="0",path="root.pipeline.processors.0"} 200
processor_received{label="1",path="root.pipeline.processors.1"} 195

# HELP processor_sent Benthos Counter metric
# TYPE processor_sent counter
processor_sent{label="0",path="root.pipeline.processors.0"} 195
processor_sent{label="1",path="root.pipeline.processors.1"} 180

# HELP processor_batch_received Benthos Counter metric
# TYPE processor_batch_received counter
processor_batch_received{label="0",path="root.pipeline.processors.0"} 20
processor_batch_received{label="1",path="root.pipeline.processors.1"} 19

# HELP processor_batch_sent Benthos Counter metric
# TYPE processor_batch_sent counter
processor_batch_sent{label="0",path="root.pipeline.processors.0"} 19
processor_batch_sent{label="1",path="root.pipeline.processors.1"} 18
`

			// Setup mock responses for first tick
			mockClient.SetResponse("/metrics", MockResponse{
				StatusCode: 200,
				Body:       []byte(tick1Response),
			})
			mockClient.SetResponse("/ready", MockResponse{
				StatusCode: 200,
				Body:       []byte("{}"),
			})

			status1, err := service.Status(context.Background(), benthosName, metricsPort, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// Verify initial component metrics
			Expect(status1.BenthosStatus.Metrics.Process.Processors).To(HaveLen(2))
			processor0 := status1.BenthosStatus.Metrics.Process.Processors["root.pipeline.processors.0"]
			Expect(processor0.Received).To(Equal(int64(100)))
			Expect(processor0.Sent).To(Equal(int64(95)))
			Expect(processor0.BatchReceived).To(Equal(int64(10)))
			Expect(processor0.BatchSent).To(Equal(int64(9)))

			processor1 := status1.BenthosStatus.Metrics.Process.Processors["root.pipeline.processors.1"]
			Expect(processor1.Received).To(Equal(int64(95)))
			Expect(processor1.Sent).To(Equal(int64(90)))
			Expect(processor1.BatchReceived).To(Equal(int64(9)))
			Expect(processor1.BatchSent).To(Equal(int64(8)))

			// Setup mock responses for second tick
			mockClient.SetResponse("/metrics", MockResponse{
				StatusCode: 200,
				Body:       []byte(tick2Response),
			})
			mockClient.SetResponse("/ready", MockResponse{
				StatusCode: 200,
				Body:       []byte("{}"),
			})
			status2, err := service.Status(context.Background(), benthosName, metricsPort, tick)
			tick++
			Expect(err).NotTo(HaveOccurred())

			// Verify component throughput calculations
			Expect(status2.BenthosStatus.Metrics.Process.Processors).To(HaveLen(2))
			processor0 = status2.BenthosStatus.Metrics.Process.Processors["root.pipeline.processors.0"]
			Expect(processor0.Received).To(Equal(int64(200)))
			Expect(processor0.Sent).To(Equal(int64(195)))
			Expect(processor0.BatchReceived).To(Equal(int64(20)))
			Expect(processor0.BatchSent).To(Equal(int64(19)))

			processor1 = status2.BenthosStatus.Metrics.Process.Processors["root.pipeline.processors.1"]
			Expect(processor1.Received).To(Equal(int64(195)))
			Expect(processor1.Sent).To(Equal(int64(180)))
			Expect(processor1.BatchReceived).To(Equal(int64(19)))
			Expect(processor1.BatchSent).To(Equal(int64(18)))

			// Verify component throughput state
			processor0State := status2.BenthosStatus.MetricsState.Processors["root.pipeline.processors.0"]
			Expect(processor0State.MessagesPerTick).To(Equal(float64(100))) // (200-100)/1
			Expect(processor0State.BatchesPerTick).To(Equal(float64(10)))   // (20-10)/1

			processor1State := status2.BenthosStatus.MetricsState.Processors["root.pipeline.processors.1"]
			Expect(processor1State.MessagesPerTick).To(Equal(float64(90))) // (180-90)/1
			Expect(processor1State.BatchesPerTick).To(Equal(float64(10)))  // (19-9)/1
		})
	})
})
