package benthos_test

import (
	"context"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/benthos"
)

var _ = Describe("Benthos Service", func() {
	var (
		mockClient *benthos.MockHTTPClient
		service    *benthos.BenthosService
		ctx        context.Context
	)

	BeforeEach(func() {
		mockClient = benthos.NewMockHTTPClient()
		service = benthos.NewDefaultBenthosService("test", benthos.WithHTTPClient(mockClient))
		ctx = context.Background()
	})

	Describe("GetHealthCheck", func() {
		Context("with valid metrics port", func() {
			const metricsPort = 4195

			When("all services are healthy", func() {
				It("should return healthy status", func() {
					health, err := service.GetHealthCheck(ctx, "test-service", metricsPort)
					Expect(err).NotTo(HaveOccurred())
					Expect(health.IsLive).To(BeTrue())
					Expect(health.IsReady).To(BeTrue())
					Expect(health.Version).To(Equal("mock-version"))
					Expect(health.ReadyError).To(BeEmpty())
					Expect(health.ConnectionStatuses).To(HaveLen(2))
					Expect(health.ConnectionStatuses[0].Path).To(Equal("input"))
					Expect(health.ConnectionStatuses[0].Connected).To(BeTrue())
					Expect(health.ConnectionStatuses[1].Path).To(Equal("output"))
					Expect(health.ConnectionStatuses[1].Connected).To(BeTrue())
				})
			})

			When("input is disconnected", func() {
				BeforeEach(func() {
					mockClient.SetReadyStatus(
						http.StatusServiceUnavailable,
						false,
						true,
						"input disconnected",
					)
				})

				It("should return unhealthy status with input error", func() {
					health, err := service.GetHealthCheck(ctx, "test-service", metricsPort)
					Expect(err).NotTo(HaveOccurred())
					Expect(health.IsLive).To(BeTrue())
					Expect(health.IsReady).To(BeFalse())
					Expect(health.ReadyError).To(Equal("input disconnected"))
					Expect(health.ConnectionStatuses).To(HaveLen(2))
					Expect(health.ConnectionStatuses[0].Path).To(Equal("input"))
					Expect(health.ConnectionStatuses[0].Connected).To(BeFalse())
					Expect(health.ConnectionStatuses[1].Path).To(Equal("output"))
					Expect(health.ConnectionStatuses[1].Connected).To(BeTrue())
				})
			})

			When("output is disconnected", func() {
				BeforeEach(func() {
					mockClient.SetReadyStatus(
						http.StatusServiceUnavailable,
						true,
						false,
						"output disconnected",
					)
				})

				It("should return unhealthy status with output error", func() {
					health, err := service.GetHealthCheck(ctx, "test-service", metricsPort)
					Expect(err).NotTo(HaveOccurred())
					Expect(health.IsLive).To(BeTrue())
					Expect(health.IsReady).To(BeFalse())
					Expect(health.ReadyError).To(Equal("output disconnected"))
					Expect(health.ConnectionStatuses).To(HaveLen(2))
					Expect(health.ConnectionStatuses[0].Path).To(Equal("input"))
					Expect(health.ConnectionStatuses[0].Connected).To(BeTrue())
					Expect(health.ConnectionStatuses[1].Path).To(Equal("output"))
					Expect(health.ConnectionStatuses[1].Connected).To(BeFalse())
				})
			})

			When("context is cancelled", func() {
				It("should return context error", func() {
					ctx, cancel := context.WithCancel(ctx)
					cancel()

					health, err := service.GetHealthCheck(ctx, "test-service", metricsPort)
					Expect(err).To(MatchError(context.Canceled))
					Expect(health).To(Equal(benthos.HealthCheck{}))
				})
			})

			When("context has timeout", func() {
				It("should respect the timeout", func() {
					ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
					defer cancel()

					// Set up a mock that delays
					mockClient.SetResponse("/ping", benthos.MockResponse{
						StatusCode: http.StatusOK,
						Delay:      100 * time.Millisecond,
					})

					health, err := service.GetHealthCheck(ctx, "test-service", metricsPort)
					Expect(err).To(MatchError(context.DeadlineExceeded))
					Expect(health).To(Equal(benthos.HealthCheck{}))
				})
			})
		})

		Context("with invalid metrics port", func() {
			It("should return error", func() {
				health, err := service.GetHealthCheck(ctx, "test-service", 0)
				Expect(err).To(MatchError("could not find metrics port for service test-service"))
				Expect(health).To(Equal(benthos.HealthCheck{}))
			})
		})
	})

	Describe("GenerateS6ConfigForBenthos", func() {
		var (
			benthosConfig *config.BenthosServiceConfig
		)

		BeforeEach(func() {
			benthosConfig = &config.BenthosServiceConfig{
				MetricsPort: 4195,
				LogLevel:    "INFO",
			}
		})

		Context("with minimal configuration", func() {
			It("should generate valid YAML", func() {
				yaml, err := service.GenerateS6ConfigForBenthos(benthosConfig, "test")
				Expect(err).NotTo(HaveOccurred())
				Expect(yaml.ConfigFiles["benthos.yaml"]).To(ContainSubstring("http:\n  address: \"0.0.0.0:4195\""))
				Expect(yaml.ConfigFiles["benthos.yaml"]).To(ContainSubstring("logger:\n  level: \"INFO\""))
			})
		})

		Context("with complete configuration", func() {
			BeforeEach(func() {
				input := []map[string]interface{}{
					{
						"http_server": map[string]interface{}{
							"address": "0.0.0.0:8080",
							"path":    "/input",
						},
					},
				}
				output := []map[string]interface{}{
					{
						"http_client": map[string]interface{}{
							"url": "http://localhost:8081/output",
						},
					},
				}
				pipeline := []map[string]interface{}{
					{
						"processors": []interface{}{
							map[string]interface{}{
								"text": map[string]interface{}{
									"operator": "to_upper",
								},
							},
						},
					},
				}
				benthosConfig.Input = input
				benthosConfig.Output = output
				benthosConfig.Pipeline = pipeline
			})

			It("should generate valid YAML with all sections", func() {
				yaml, err := service.GenerateS6ConfigForBenthos(benthosConfig, "test")
				Expect(err).NotTo(HaveOccurred())
				yamlContent := yaml.ConfigFiles["benthos.yaml"]

				// Check input section
				Expect(yamlContent).To(ContainSubstring("input:"))
				Expect(yamlContent).To(ContainSubstring("http_server:"))
				Expect(yamlContent).To(ContainSubstring("address: 0.0.0.0:8080"))
				Expect(yamlContent).To(ContainSubstring("path: /input"))

				// Check output section
				Expect(yamlContent).To(ContainSubstring("output:"))
				Expect(yamlContent).To(ContainSubstring("http_client:"))
				Expect(yamlContent).To(ContainSubstring("url: http://localhost:8081/output"))

				// Check pipeline section
				Expect(yamlContent).To(ContainSubstring("pipeline:"))
				Expect(yamlContent).To(ContainSubstring("processors:"))
				Expect(yamlContent).To(ContainSubstring("text:"))
				Expect(yamlContent).To(ContainSubstring("operator: to_upper"))

				// Check standard sections
				Expect(yamlContent).To(ContainSubstring("http:\n  address: \"0.0.0.0:4195\""))
				Expect(yamlContent).To(ContainSubstring("logger:\n  level: \"INFO\""))
			})
		})

		Context("with cache and rate limit resources", func() {
			BeforeEach(func() {
				cacheResources := []map[string]interface{}{
					{
						"memory": map[string]interface{}{
							"ttl": "300s",
						},
					},
				}
				rateLimitResources := []map[string]interface{}{
					{
						"local": map[string]interface{}{
							"count":    100,
							"interval": "60s",
						},
					},
				}
				benthosConfig.CacheResources = cacheResources
				benthosConfig.RateLimitResources = rateLimitResources
			})

			It("should generate valid YAML with resources", func() {
				yaml, err := service.GenerateS6ConfigForBenthos(benthosConfig, "test")
				Expect(err).NotTo(HaveOccurred())
				yamlContent := yaml.ConfigFiles["benthos.yaml"]

				// Check cache resources
				Expect(yamlContent).To(ContainSubstring("cache_resources:"))
				Expect(yamlContent).To(ContainSubstring("memory:"))
				Expect(yamlContent).To(ContainSubstring("ttl: 300s"))

				// Check rate limit resources
				Expect(yamlContent).To(ContainSubstring("rate_limit_resources:"))
				Expect(yamlContent).To(ContainSubstring("local:"))
				Expect(yamlContent).To(ContainSubstring("count: 100"))
				Expect(yamlContent).To(ContainSubstring("interval: 60s"))
			})
		})

		Context("with buffer configuration", func() {
			BeforeEach(func() {
				buffer := []map[string]interface{}{
					{
						"memory": map[string]interface{}{
							"limit": 10000000,
						},
					},
				}
				benthosConfig.Buffer = buffer
			})

			It("should generate valid YAML with buffer section", func() {
				yaml, err := service.GenerateS6ConfigForBenthos(benthosConfig, "test")
				Expect(err).NotTo(HaveOccurred())
				yamlContent := yaml.ConfigFiles["benthos.yaml"]

				// Check buffer section
				Expect(yamlContent).To(ContainSubstring("buffer:"))
				Expect(yamlContent).To(ContainSubstring("memory:"))
				Expect(yamlContent).To(ContainSubstring("limit: 10000000"))
			})
		})

		Context("with invalid configuration", func() {
			It("should handle nil config", func() {
				yaml, err := service.GenerateS6ConfigForBenthos(nil, "test")
				Expect(err).To(HaveOccurred())
				Expect(yaml).To(Equal(config.S6ServiceConfig{}))
			})
		})
	})
})
