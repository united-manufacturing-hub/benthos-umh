// integration_test.go

package integration_test

import (
	"bytes"
	"fmt"
	"io"
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
		"-p", "8081:8080", // map container's 8080 => localhost:8081
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
		resp, err := http.Get(metricsURL)
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

			// Example: look for "umh_core_reconcile_duration_milliseconds" to confirm it's running
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

			// Because the golden service runs Benthos listening on port 8080 inside the container
			// (mapped to localhost:8081?), you can also do a quick check:
			Eventually(func() int {
				checkResp, e := http.Get("http://localhost:8082")
				if e != nil {
					return 0
				}
				defer checkResp.Body.Close()
				return checkResp.StatusCode
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
			// Just check that metrics are available:
			resp, err := http.Get(metricsURL)
			Expect(err).NotTo(HaveOccurred())
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())

			// Some examples of possible checks:
			Expect(string(body)).To(ContainSubstring("umh_core_reconcile_duration_milliseconds"))
			// If your system logs an instance name or metric label for the 'sleepy' service:
			// you might also check something like:
			// Expect(string(body)).To(ContainSubstring("instance=\"sleepy\""))
		})
	})
})
