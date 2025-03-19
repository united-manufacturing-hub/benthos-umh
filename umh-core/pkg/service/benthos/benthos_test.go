package benthos_test

import (
	"context"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
})
