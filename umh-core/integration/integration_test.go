// integration_test.go

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/service/s6"
)

const (
	containerName = "umh-core"        // Docker container name
	imageName     = "umh-core:latest" // Docker image name/tag
	metricsURL    = "http://localhost:8081/metrics"
)

// ----------- Docker helper functions -----------

func runDockerCommand(args ...string) (string, error) {
	cmd := exec.Command("docker", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

// startContainer rebuilds the Docker image, copies config, then starts the container
func startContainer() error {
	// stop + remove any existing container
	_, _ = runDockerCommand("stop", containerName)
	_, _ = runDockerCommand("rm", "-f", containerName)

	// Rebuild image if you want to ensure it's up to date:
	coreDir := filepath.Dir(GetCurrentDir()) // Get parent directory (umh-core)
	dockerfilePath := filepath.Join(coreDir, "Dockerfile")
	out, err := runDockerCommand("build", "-t", imageName, "-f", dockerfilePath, coreDir)
	if err != nil {
		return fmt.Errorf("failed to build image. output=%s, err=%w", out, err)
	}

	// Now run it
	out, err = runDockerCommand(
		"run", "-d",
		"--name", containerName,
		"--cpus=1",
		"--memory=512m",
		"-v", fmt.Sprintf("%s/data:/data", GetCurrentDir()), // mount local ./data to /data in container
		"-p", "8081:8080", // metrics port
		"-p", "8082:8082", // golden service port
		imageName,
	)
	if err != nil {
		return fmt.Errorf("failed to start container: %v, output: %s", err, out)
	}
	return nil
}

func stopContainer() error {
	out, err := runDockerCommand("rm", "-f", containerName)
	if err != nil {
		return fmt.Errorf("failed to stop container: %v, output: %s", err, out)
	}
	return nil
}

// waitForMetrics polls the /metrics endpoint until it returns 200
func waitForMetrics() error {
	Eventually(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "GET", metricsURL, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
		}
		return nil
	}, 30*time.Second, 1*time.Second).Should(Succeed())
	return nil
}

// GetCurrentDir returns the directory of this test file (or your project root).
// Adjust if you need something else.
func GetCurrentDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return strings.TrimSpace(wd)
}

// writeConfigFile writes the given YAML content to ./data/config.yaml so the container will read it.
func writeConfigFile(yamlContent string) error {
	dataDir := filepath.Join(GetCurrentDir(), "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}
	configPath := filepath.Join(dataDir, "config.yaml")
	return os.WriteFile(configPath, []byte(yamlContent), 0o644)
}

// parseFloat is a small helper to parse a string to float64
func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// checkGoldenService sends a test request to the golden service and returns its status code
func checkGoldenService() int {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", "http://localhost:8082", bytes.NewBuffer([]byte(`{"message": "test"}`)))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	checkResp, e := http.DefaultClient.Do(req)
	if e != nil {
		return 0
	}

	defer checkResp.Body.Close()

	return checkResp.StatusCode
}

// startMonitoringGoroutine starts a goroutine that continuously monitors metrics and golden service health
func startMonitoringGoroutine(duration time.Duration, interval time.Duration) (chan bool, chan error) {
	done := make(chan bool)
	errorChan := make(chan error, 10) // Buffer for errors
	var lastMetrics string            // Store last successful metrics

	go func() {
		defer GinkgoRecover() // Required for Gomega assertions in goroutines

		Consistently(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, "GET", metricsURL, nil)
			if err != nil {
				errorChan <- fmt.Errorf("failed to create request: %w\nLast metrics:\n%s", err, lastMetrics)
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errorChan <- fmt.Errorf("failed to get metrics: %w\nLast metrics:\n%s", err, lastMetrics)
				return err
			}
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				errorChan <- fmt.Errorf("failed to read metrics: %w\nLast metrics:\n%s", err, lastMetrics)
				return err
			}

			// Store the latest metrics
			lastMetrics = string(data)

			// Use InterceptGomegaFailures to catch any assertion failures
			failures := InterceptGomegaFailures(func() {
				checkWhetherMetricsHealthy(string(data))
			})

			if len(failures) > 0 {
				err := fmt.Errorf("metrics unhealthy: %s\nLast metrics:\n%s", failures[0], lastMetrics)
				errorChan <- err
				return err
			}

			GinkgoWriter.Println("✅ Metrics are healthy")

			status := checkGoldenService()
			if status != 200 {
				err := fmt.Errorf("golden service returned status %d\nLast metrics:\n%s", status, lastMetrics)
				errorChan <- err
				return err
			}
			GinkgoWriter.Println("✅ Golden service is running")

			return nil
		}, duration, interval).Should(Succeed())

		done <- true
	}()

	return done, errorChan
}

// ---------- Actual Ginkgo Tests ----------

var _ = Describe("UMH Container Integration", Ordered, Label("integration"), func() {

	AfterAll(func() {
		// Always stop container after the entire suite
		_ = stopContainer()
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
			Expect(startContainer()).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with empty config")
		})

		AfterAll(func() {
			Expect(stopContainer()).To(Succeed(), "Stop container after empty config scenario")
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
			Expect(startContainer()).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with golden service config")
		})

		AfterAll(func() {
			Expect(stopContainer()).To(Succeed(), "Stop container after golden config scenario")
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
				return checkGoldenService()
			}, 10*time.Second, 1*time.Second).Should(Equal(200),
				"Golden service should respond with 200 OK on the mapped port")
		})
	})

	Context("with multiple services (golden + a 'sleep' service)", func() {
		BeforeAll(func() {
			// Build a config that has both the golden service and another dummy s6 service
			extraService := config.S6FSMConfig{
				FSMInstanceConfig: config.FSMInstanceConfig{
					Name: "sleepy",
				},
				S6ServiceConfig: s6.S6ServiceConfig{
					Command: []string{"sleep", "1000"},
				},
			}

			cfg := NewBuilder().
				AddGoldenService().
				AddService(extraService).
				BuildYAML()

			Expect(writeConfigFile(cfg)).To(Succeed())
			Expect(startContainer()).To(Succeed())
			Expect(waitForMetrics()).To(Succeed())
		})

		AfterAll(func() {
			Expect(stopContainer()).To(Succeed())
		})

		It("should have both services active and expose metrics", func() {
			// Retrieve the metrics as a string
			resp, err := http.Get(metricsURL)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())

			// First check the metrics - we expect this to fail if they're unhealthy
			checkWhetherMetricsHealthy(string(data))

			// Now verify metrics are consistently healthy over time
			Consistently(func() error {
				// Make a fresh request each time to get updated metrics
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, "GET", metricsURL, nil)
				if err != nil {
					return err
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()

				freshData, err := io.ReadAll(resp.Body)
				if err != nil {
					return err
				}

				// This will panic and fail the test if metrics aren't healthy
				checkWhetherMetricsHealthy(string(freshData))
				GinkgoWriter.Println("Metrics are healthy")

				status := checkGoldenService()
				if status != 200 {
					GinkgoWriter.Printf("❌ Golden service returned status %d\n", status)
					return fmt.Errorf("golden service returned status %d", status)
				}
				GinkgoWriter.Println("✅ Golden service is running")

				return nil
			}, 5*time.Minute, 1*time.Second).Should(Succeed())
		})
	})

	Context("with service scaling test", Label("integration", "scaling"), func() {
		BeforeAll(func() {
			// Start with an empty config
			emptyConfig := `
agent:
  metricsPort: 8080
services: []
benthos: []
`
			Expect(writeConfigFile(emptyConfig)).To(Succeed())
			Expect(startContainer()).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with empty config")
		})

		AfterAll(func() {
			Expect(stopContainer()).To(Succeed(), "Stop container after scaling test")
		})

		It("should scale up to multiple services while maintaining healthy metrics", func() {
			// Main test: Scale up to 10 services
			GinkgoWriter.Println("Starting service scaling test")

			// First add just the golden service to verify it works
			cfg := NewBuilder().AddGoldenService().BuildYAML()
			Expect(writeConfigFile(cfg)).To(Succeed())

			// Wait for golden service to be ready
			Eventually(func() int {
				return checkGoldenService()
			}, 20*time.Second, 1*time.Second).Should(Equal(200),
				"Golden service should respond with 200 OK")

			// Start monitoring goroutine
			done, errorChan := startMonitoringGoroutine(5*time.Minute, 5*time.Second)

			GinkgoWriter.Println("Golden service is up, now adding 10 services at once...")

			// Create builder with golden service + 10 sleep services
			builder := NewBuilder().AddGoldenService()

			// Add all 10 "sleep" services at once
			for i := 0; i < 10; i++ {
				serviceName := fmt.Sprintf("sleepy-%d", i)

				// Add sleeping services
				builder.AddSleepService(serviceName, "600")
			}

			// Write single config with all services
			fullConfig := builder.BuildYAML()
			GinkgoWriter.Println("Generated config with 11 services (1 golden + 10 sleep services)")
			GinkgoWriter.Println(fullConfig)
			Expect(writeConfigFile(fullConfig)).To(Succeed())

			// Create a deterministic random number generator for reproducible tests
			r := rand.New(rand.NewSource(42))

			// Chaos monkey: randomly stop and start services
			for i := 0; i < 100; i++ { // Do 100 random actions
				// Random service index (0-9)
				randomIndex := r.Intn(10)
				randomServiceName := fmt.Sprintf("sleepy-%d", randomIndex)

				// Random action (stop or start)
				action := "start"
				if r.Float64() < 0.5 {
					action = "stop"
					builder.StopService(randomServiceName)
				} else {
					builder.StartService(randomServiceName)
				}

				GinkgoWriter.Printf("Chaos monkey: %sing service %s\n", action, randomServiceName)
				Expect(writeConfigFile(builder.BuildYAML())).To(Succeed())

				// Random delay
				delay := time.Duration(100+r.Intn(500)) * time.Millisecond
				time.Sleep(delay)
			}

			// Check for any errors from the monitoring goroutine
			select {
			case err := <-errorChan:
				Fail(fmt.Sprintf("Error in background monitoring: %v", err))
			case <-done:
				GinkgoWriter.Println("A monitoring routine completed successfully")
			}
			GinkgoWriter.Println("Scaling test completed successfully")
		})
	})

	Context("with comprehensive chaos test", Label("integration", "chaos"), func() {
		BeforeAll(func() {
			// Start with an empty config
			emptyConfig := `
agent:
  metricsPort: 8080
services: []
benthos: []
`
			Expect(writeConfigFile(emptyConfig)).To(Succeed())
			Expect(startContainer()).To(Succeed())
			Expect(waitForMetrics()).To(Succeed(), "Metrics endpoint should be available with empty config")
		})

		AfterAll(func() {
			Expect(stopContainer()).To(Succeed(), "Stop container after chaos test")
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
				return checkGoldenService()
			}, 20*time.Second, 1*time.Second).Should(Equal(200),
				"Golden service should respond with 200 OK")

			done, errorChan := startMonitoringGoroutine(testDuration, 5*time.Second) // this needs to be after the golden service is up
			GinkgoWriter.Println("Starting comprehensive chaos test with up to 1000 services")

			// Track existing services (with their current state)
			existingServices := map[string]string{} // serviceName -> state ("running"/"stopped")
			maxServices := 1000

			// Test runs until the duration is reached
			startTime := time.Now()
			actionCount := 0
			bulkSize := 100 // Size of bulk operations

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

			// Check for any errors from the monitoring goroutine
			select {
			case err := <-errorChan:
				Fail(fmt.Sprintf("Error in background monitoring: %v", err))
			case <-done:
				GinkgoWriter.Println("Monitoring routine completed successfully")
			}

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
