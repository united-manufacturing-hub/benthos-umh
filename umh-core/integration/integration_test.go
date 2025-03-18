// integration_test.go

package integration_test

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ---------- Actual Ginkgo Tests ----------

var _ = Describe("UMH Container Integration", Ordered, Label("integration"), func() {

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			fmt.Println("Test failed, printing container logs:")
			printContainerLogs()
		}
	})

	AfterAll(func() {
		// Always stop container after the entire suite
		StopContainer()
	})

	Context("with an empty config", func() {
		BeforeAll(func() {
			By("Building an empty config and writing to data/config.yaml")
			emptyConfig := `
agent:
  metricsPort: 8080
services: []
benthos: []
`
			Expect(writeConfigFile(emptyConfig)).To(Succeed())
			Expect(BuildAndRunContainer(emptyConfig)).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with empty config")
		})

		AfterAll(func() {
			StopContainer() // Stop container after empty config scenario
		})

		It("exposes metrics and has zero s6 services running", func() {
			// Check the /metrics endpoint
			resp, err := http.Get(metricsURL)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())

			// Check that the metrics endpoint contains the expected metrics
			Expect(string(body)).To(ContainSubstring("umh_core_reconcile_duration_milliseconds"))
		})
	})

	Context("with a golden service config", func() {
		BeforeAll(func() {
			By("Building a config with the golden service and writing to data/config.yaml")
			cfg := NewBuilder().
				AddGoldenService().
				BuildYAML()

			Expect(writeConfigFile(cfg)).To(Succeed())
			Expect(BuildAndRunContainer(cfg)).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with golden service config")
		})

		AfterAll(func() {
			StopContainer() // Stop container after golden config scenario
		})

		It("should have the golden service up and expose metrics", func() {
			// Check /metrics
			resp, err := http.Get(metricsURL)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("umh_core_reconcile_duration_milliseconds"))

			Eventually(func() int {
				return checkGoldenServiceStatusOnly()
			}, 10*time.Second, 1*time.Second).Should(Equal(200),
				"Golden service should respond with 200 OK on the mapped port")
		})
	})

	Context("with multiple services (golden + a 'sleep' service)", func() {
		BeforeAll(func() {
			By("Building a configuration with the golden service and a sleep service")
			cfg := NewBuilder().
				AddGoldenService().
				AddSleepService("sleepy", "600").
				BuildYAML()

			// Write the config and start the container with the new configuration.
			Expect(writeConfigFile(cfg)).To(Succeed())
			Expect(BuildAndRunContainer(cfg)).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with golden + sleep service config")

			// Verify that the golden service is ready
			Eventually(func() int {
				return checkGoldenServiceStatusOnly()
			}, 10*time.Second, 1*time.Second).Should(Equal(200),
				"Golden service should respond with 200 OK on the mapped port")
			GinkgoWriter.Println("Golden service is up and running")
		})

		AfterAll(func() {
			By("Stopping container after the multiple services test")
			StopContainer()
		})

		It("should have both services active and expose healthy metrics", func() {
			By("Verifying the metrics endpoint contains expected metrics")
			resp, err := http.Get(metricsURL)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("umh_core_reconcile_duration_milliseconds"))

			By("Verifying that the golden service is returning 200 OK")
			Eventually(func() int {
				return checkGoldenServiceStatusOnly()
			}, 10*time.Second, 1*time.Second).Should(Equal(200),
				"Golden service should respond with 200 OK on the mapped port")
		})
	})

	Context("with service scaling test", Label("scaling"), func() {
		BeforeAll(func() {
			By("Starting with an empty configuration")
			cfg := NewBuilder().BuildYAML()
			// Write the empty config and start the container
			Expect(writeConfigFile(cfg)).To(Succeed())
			Expect(BuildAndRunContainer(cfg)).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with empty config")
		})

		AfterAll(func() {
			By("Stopping the container after the scaling test")
			StopContainer()
		})

		It("should scale up to multiple services while maintaining healthy metrics", func() {
			By("Adding the golden service as a baseline")
			// Build configuration with the golden service first
			builder := NewBuilder().AddGoldenService()
			cfg := builder.BuildYAML()
			Expect(writeConfigFile(cfg)).To(Succeed())

			By("Waiting for the golden service to become responsive")
			Eventually(func() int {
				return checkGoldenServiceStatusOnly()
			}, 20*time.Second, 1*time.Second).Should(Equal(200),
				"Golden service should respond with 200 OK")

			By("Scaling up by adding 10 sleep services")
			// Add 10 sleep services to the configuration
			for i := 0; i < 10; i++ {
				serviceName := fmt.Sprintf("sleepy-%d", i)
				builder.AddSleepService(serviceName, "600")
				cfg = builder.BuildYAML()
				GinkgoWriter.Printf("Added service %s\n", serviceName)
				Expect(writeConfigFile(cfg)).To(Succeed())
			}

			By("Simulating random stop/start actions on sleep services (chaos monkey)")
			// Create a deterministic random number generator for reproducibility
			r := rand.New(rand.NewSource(42))
			for i := 0; i < 100; i++ {
				// Pick a random sleep service index (0-9)
				randomIndex := r.Intn(10)
				randomServiceName := fmt.Sprintf("sleepy-%d", randomIndex)

				// Randomly decide to start or stop the service
				action := "start"
				if r.Float64() < 0.5 {
					action = "stop"
					builder.StopService(randomServiceName)
				} else {
					builder.StartService(randomServiceName)
				}
				GinkgoWriter.Printf("Chaos monkey: %sing service %s\n", action, randomServiceName)
				// Apply the updated configuration
				Expect(writeConfigFile(builder.BuildYAML())).To(Succeed())

				// Random delay between operations
				delay := time.Duration(100+r.Intn(500)) * time.Millisecond

				// Check the health of the system
				monitorHealth()
				time.Sleep(delay)
			}

			GinkgoWriter.Println("Scaling test completed successfully")
		})
	})

	Context("with comprehensive chaos test", Label("chaos"), func() {
		BeforeAll(func() {
			Skip("Skipping comprehensive chaos test due to time constraints")
			// Start with an empty config
			cfg := NewBuilder().BuildYAML()
			Expect(writeConfigFile(cfg)).To(Succeed())
			Expect(BuildAndRunContainer(cfg)).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with empty config")
		})

		AfterAll(func() {
			StopContainer()
		})

		It("should handle random service additions, deletions, starts and stops", func() {
			// Start monitoring goroutine
			testDuration := 10 * time.Minute

			// Create deterministic random number generator
			r := rand.New(rand.NewSource(42))

			// Add golden service as constant baseline
			builder := NewBuilder().AddGoldenService()
			Expect(writeConfigFile(builder.BuildYAML())).To(Succeed())

			// Wait for golden service to be ready
			Eventually(func() int {
				return checkGoldenServiceStatusOnly()
			}, 20*time.Second, 1*time.Second).Should(Equal(200),
				"Golden service should respond with 200 OK")

			GinkgoWriter.Println("Starting comprehensive chaos test with up to 1000 services")

			// Track existing services (with their current state)
			existingServices := map[string]string{} // serviceName -> state ("running"/"stopped")
			maxServices := 100

			// Test runs until the duration is reached
			startTime := time.Now()
			actionCount := 0
			bulkSize := 2 // Size of bulk operations

			for time.Since(startTime) < testDuration {
				actionCount++

				// Randomly choose an action type with wider distribution of actions
				// 0=add single, 1=delete single, 2=start, 3=stop, 4=bulk add, 5=bulk delete
				actionType := r.Intn(10)

				switch {
				case actionType < 3: // Add a single service (30% chance)
					if len(existingServices) < maxServices {
						// Create a new service with a unique name
						serviceName := fmt.Sprintf("chaos-svc-%d", actionCount)
						builder.AddSleepService(serviceName, fmt.Sprintf("%d", 60+r.Intn(600)))
						existingServices[serviceName] = "running"
						GinkgoWriter.Printf("Chaos: ADDING service %s (total: %d)\n",
							serviceName, len(existingServices))
					}

				case actionType < 6: // Delete a single service (30% chance)
					if len(existingServices) > 0 {
						// Get a random existing service
						keys := getKeys(existingServices)
						serviceToDelete := keys[r.Intn(len(keys))]

						// Instead of using RemoveService which doesn't exist,
						// build a new config from scratch without the deleted service
						newBuilder := NewBuilder().AddGoldenService()
						for svc, state := range existingServices {
							if svc != serviceToDelete { // Skip the one being deleted
								if state == "running" {
									newBuilder.AddSleepService(svc, "600")
								} else {
									newBuilder.AddSleepService(svc, "600")
									newBuilder.StopService(svc)
								}
							}
						}
						builder = newBuilder // Replace the builder
						delete(existingServices, serviceToDelete)

						GinkgoWriter.Printf("Chaos: DELETING service %s (remaining: %d)\n",
							serviceToDelete, len(existingServices))
					}

				case actionType < 7: // Start a stopped service (10% chance)
					// Find stopped services
					stoppedServices := []string{}
					for svc, state := range existingServices {
						if state == "stopped" {
							stoppedServices = append(stoppedServices, svc)
						}
					}

					if len(stoppedServices) > 0 {
						serviceToStart := stoppedServices[r.Intn(len(stoppedServices))]
						builder.StartService(serviceToStart)
						existingServices[serviceToStart] = "running"

						GinkgoWriter.Printf("Chaos: STARTING service %s\n", serviceToStart)
					}

				case actionType < 8: // Stop a running service (10% chance)
					// Find running services
					runningServices := []string{}
					for svc, state := range existingServices {
						if state == "running" {
							runningServices = append(runningServices, svc)
						}
					}

					if len(runningServices) > 0 {
						serviceToStop := runningServices[r.Intn(len(runningServices))]
						builder.StopService(serviceToStop)
						existingServices[serviceToStop] = "stopped"

						GinkgoWriter.Printf("Chaos: STOPPING service %s\n", serviceToStop)
					}

				case actionType < 9: // Bulk add services (10% chance)
					numToAdd := min(bulkSize, maxServices-len(existingServices))
					if numToAdd > 0 {
						GinkgoWriter.Printf("Chaos: BULK ADDING %d services\n", numToAdd)
						for i := 0; i < numToAdd; i++ {
							serviceName := fmt.Sprintf("bulk-add-%d-%d", actionCount, i)
							builder.AddSleepService(serviceName, fmt.Sprintf("%d", 60+r.Intn(600)))
							existingServices[serviceName] = "running"
						}
						GinkgoWriter.Printf("Chaos: BULK ADD completed (total: %d)\n", len(existingServices))
					}

				case actionType < 10: // Bulk delete services (10% chance)
					keys := getKeys(existingServices)
					numToDelete := min(bulkSize, len(keys))
					if numToDelete > 0 {
						// Recreate config without the deleted services
						newBuilder := NewBuilder().AddGoldenService()

						// Choose random services to delete
						indicesToDelete := make(map[int]bool)
						for i := 0; i < numToDelete; i++ {
							for {
								idx := r.Intn(len(keys))
								if !indicesToDelete[idx] {
									indicesToDelete[idx] = true
									break
								}
							}
						}

						// Rebuild config without deleted services
						GinkgoWriter.Printf("Chaos: BULK DELETING %d services\n", numToDelete)
						for idx, svc := range keys {
							if !indicesToDelete[idx] {
								state := existingServices[svc]
								if state == "running" {
									newBuilder.AddSleepService(svc, "600")
								} else {
									newBuilder.AddSleepService(svc, "600")
									newBuilder.StopService(svc)
								}
							} else {
								delete(existingServices, svc)
							}
						}
						builder = newBuilder
						GinkgoWriter.Printf("Chaos: BULK DELETE completed (remaining: %d)\n", len(existingServices))
					}

				}

				// Apply changes
				Expect(writeConfigFile(builder.BuildYAML())).To(Succeed())

				// Random delay between operations, shorter for smaller changes
				var delay time.Duration
				if actionType >= 8 { // Bulk operations get longer delays
					delay = time.Duration(500+r.Intn(2000)) * time.Millisecond // 0.5-2.5s
				} else {
					delay = time.Duration(50+r.Intn(200)) * time.Millisecond // 50-250ms
				}

				// Check the health of the system
				monitorHealth()
				time.Sleep(delay)

				// Every 20 actions, print a status update
				if actionCount%20 == 0 {
					running := countRunningServices(existingServices)
					elapsedTime := time.Since(startTime).Round(time.Second)
					remainingTime := (testDuration - elapsedTime).Round(time.Second)
					GinkgoWriter.Printf("Chaos test status: %d actions completed, %d services (%d running), elapsed: %v, remaining: %v\n",
						actionCount, len(existingServices), running, elapsedTime, remainingTime)
				}
			}

			GinkgoWriter.Printf("Chaos test actions completed (%d total actions), waiting for monitoring to complete\n", actionCount)
			GinkgoWriter.Println("Chaos test completed successfully")
		})
	})
})

// Helper functions for the chaos test

// getKeys returns all keys from a map as a slice
func getKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// countRunningServices counts how many services are in the "running" state
func countRunningServices(services map[string]string) int {
	count := 0
	for _, state := range services {
		if state == "running" {
			count++
		}
	}
	return count
}
