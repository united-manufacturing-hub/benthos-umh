// Copyright 2026 UMH Systems GmbH
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

package beckhoff_ads_plugin_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"
)

// --- Config File Loading ---

// testdataDir returns the absolute path to the testdata directory.
func testdataDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "testdata")
}

// loadTestConfig reads a YAML config file from testdata/.
func loadTestConfig(filename string) string {
	path := filepath.Join(testdataDir(), filename)
	data, err := os.ReadFile(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to read config file %s", path)
	return string(data)
}

// --- Symbol Extraction ---

// extractSymbolsFromConfig parses a benthos ADS YAML config and returns the symbol list.
// Works with raw YAML (env var placeholders are only in connection settings, not symbols).
func extractSymbolsFromConfig(yamlConfig string) []string {
	var cfg struct {
		Input struct {
			ADS struct {
				Symbols []string `yaml:"symbols"`
			} `yaml:"ads"`
		} `yaml:"input"`
	}
	err := yaml.Unmarshal([]byte(yamlConfig), &cfg)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to parse YAML config for symbol extraction")
	return cfg.Input.ADS.Symbols
}

// extractInputYAML strips the top-level "input:" key from a full benthos config,
// returning just the inner input config suitable for AddInputYAML.
func extractInputYAML(fullConfig string) string {
	var cfg map[string]interface{}
	err := yaml.Unmarshal([]byte(fullConfig), &cfg)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to parse full config")
	inputCfg, ok := cfg["input"]
	ExpectWithOffset(1, ok).To(BeTrue(), "config has no 'input' key")
	out, err := yaml.Marshal(inputCfg)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to marshal input config")
	return string(out)
}

// --- Notification Config Templates (small, inline) ---

// TC3 notification: 2 symbols, no file needed.
const tc3NotificationConfigTpl = `
input:
  ads:
    targetIP: "${TEST_ADS_TC3_TARGET_IP}"
    targetAMS: "${TEST_ADS_TC3_TARGET_AMS}"
    runtimePort: ${TEST_ADS_TC3_RUNTIME_PORT:851}
    readType: notification
    cycleTime: 100
    maxDelay: 0
    logLevel: warn
    transmissionMode: serverCycle
    username: "${TEST_ADS_TC3_ROUTE_USER:}"
    password: "${TEST_ADS_TC3_ROUTE_PASS:}"
    symbols:
      - "PRG_Diagnostics.nFastDint"
      - "PRG_Diagnostics.nInt"
`

// TC2 notification: 2 symbols, program-prefixed (no dot).
const tc2NotificationConfigTpl = `
input:
  ads:
    targetIP: "${TEST_ADS_TC2_TARGET_IP}"
    targetAMS: "${TEST_ADS_TC2_TARGET_AMS}"
    runtimePort: ${TEST_ADS_TC2_RUNTIME_PORT:801}
    readType: notification
    cycleTime: 100
    maxDelay: 0
    logLevel: warn
    transmissionMode: serverCycle
    username: "${TEST_ADS_TC2_ROUTE_USER:}"
    password: "${TEST_ADS_TC2_ROUTE_PASS:}"
    symbols:
      - "PRG_DIAGNOSTICS.nFastDint"
      - "PRG_DIAGNOSTICS.nInt"
`

// --- StreamBuilder Pipeline Tests ---

var _ = Describe("Config-Driven ADS Pipeline Tests", func() {

	// ---- TC3 Full Pipeline ----

	Context("TC3 - Full Pipeline Interval Read", Ordered, func() {
		BeforeAll(func() {
			if os.Getenv("TEST_ADS_TC3_TARGET_IP") == "" || os.Getenv("TEST_ADS_TC3_TARGET_AMS") == "" {
				Skip("TC3 env vars not set")
			}
		})

		It("reads all configured symbols through benthos pipeline", func() {
			yamlCfg := loadTestConfig("benthos-tc3.yaml")
			expectedSymbols := extractSymbolsFromConfig(yamlCfg)

			builder := service.NewStreamBuilder()
			err := builder.AddInputYAML(extractInputYAML(yamlCfg))
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			symbolsSeen := make(map[string]string)

			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				name, _ := msg.MetaGet("symbol_name")
				b, _ := msg.AsBytes()
				mu.Lock()
				symbolsSeen[name] = string(b)
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			go func() { _ = stream.Run(ctx) }()

			// Allow up to 2 missing for known enum issues
			minExpected := len(expectedSymbols) - 2
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(symbolsSeen)
			}, 10*time.Second, 500*time.Millisecond).Should(
				BeNumerically(">=", minExpected),
			)

			cancel()

			mu.Lock()
			defer mu.Unlock()
			var missing []string
			for _, sym := range expectedSymbols {
				sanitized := sanitizeSymbolName(sym)
				if _, ok := symbolsSeen[sanitized]; !ok {
					missing = append(missing, sym)
				}
			}
			if len(missing) > 0 {
				GinkgoWriter.Printf("Missing symbols (%d): %v\n", len(missing), missing)
			}
			Expect(len(symbolsSeen)).To(BeNumerically(">=", minExpected),
				"too many symbols missing: %v", missing)
		})
	})

	Context("TC3 - Full Pipeline Notification Read", Ordered, func() {
		BeforeAll(func() {
			if os.Getenv("TEST_ADS_TC3_TARGET_IP") == "" || os.Getenv("TEST_ADS_TC3_TARGET_AMS") == "" {
				Skip("TC3 env vars not set")
			}
		})

		It("receives notifications for all configured symbols", func() {
			expectedSymbols := extractSymbolsFromConfig(tc3NotificationConfigTpl)

			builder := service.NewStreamBuilder()
			err := builder.AddInputYAML(extractInputYAML(tc3NotificationConfigTpl))
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			symbolCounts := make(map[string]int)

			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				name, _ := msg.MetaGet("symbol_name")
				mu.Lock()
				symbolCounts[name]++
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			go func() { _ = stream.Run(ctx) }()

			// Pipeline Connect may take tens of seconds under go-ads flap-backoff
			// when the PLC is busy (route table thrashed by other test runs).
			// 90s absorbs a fully escalated flap sequence plus first notifications.
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(symbolCounts)
			}, 90*time.Second, 500*time.Millisecond).Should(
				BeNumerically(">=", len(expectedSymbols)),
			)

			time.Sleep(3 * time.Second)
			cancel()

			mu.Lock()
			defer mu.Unlock()
			for _, sym := range expectedSymbols {
				sanitized := sanitizeSymbolName(sym)
				Expect(symbolCounts).To(HaveKey(sanitized),
					"symbol %s never received notification", sym)
				Expect(symbolCounts[sanitized]).To(BeNumerically(">=", 2),
					"symbol %s should have received multiple notifications", sym)
			}
		})
	})

	// ---- TC2 Full Pipeline ----

	Context("TC2 - Full Pipeline Interval Read with Route", Ordered, func() {
		BeforeAll(func() {
			if os.Getenv("TEST_ADS_TC2_TARGET_IP") == "" || os.Getenv("TEST_ADS_TC2_TARGET_AMS") == "" {
				Skip("TC2 env vars not set")
			}
		})

		It("reads all configured symbols through benthos pipeline", func() {
			yamlCfg := loadTestConfig("benthos-tc2.yaml")
			expectedSymbols := extractSymbolsFromConfig(yamlCfg)

			builder := service.NewStreamBuilder()
			err := builder.AddInputYAML(extractInputYAML(yamlCfg))
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			symbolsSeen := make(map[string]string)

			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				name, _ := msg.MetaGet("symbol_name")
				b, _ := msg.AsBytes()
				mu.Lock()
				symbolsSeen[name] = string(b)
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			go func() { _ = stream.Run(ctx) }()

			// Allow up to 2 missing for known enum issues
			minExpected := len(expectedSymbols) - 2
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(symbolsSeen)
			}, 10*time.Second, 500*time.Millisecond).Should(
				BeNumerically(">=", minExpected),
			)

			cancel()

			mu.Lock()
			defer mu.Unlock()
			var missing []string
			for _, sym := range expectedSymbols {
				sanitized := sanitizeSymbolName(sym)
				if _, ok := symbolsSeen[sanitized]; !ok {
					missing = append(missing, sym)
				}
			}
			if len(missing) > 0 {
				GinkgoWriter.Printf("Missing symbols (%d): %v\n", len(missing), missing)
			}
			Expect(len(symbolsSeen)).To(BeNumerically(">=", minExpected),
				"too many symbols missing: %v", missing)
		})
	})

	Context("TC2 - Full Pipeline Notification Read", Ordered, func() {
		BeforeAll(func() {
			if os.Getenv("TEST_ADS_TC2_TARGET_IP") == "" || os.Getenv("TEST_ADS_TC2_TARGET_AMS") == "" {
				Skip("TC2 env vars not set")
			}
		})

		It("receives notifications for all configured symbols", func() {
			expectedSymbols := extractSymbolsFromConfig(tc2NotificationConfigTpl)

			builder := service.NewStreamBuilder()
			err := builder.AddInputYAML(extractInputYAML(tc2NotificationConfigTpl))
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			symbolCounts := make(map[string]int)

			err = builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
				name, _ := msg.MetaGet("symbol_name")
				mu.Lock()
				symbolCounts[name]++
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			go func() { _ = stream.Run(ctx) }()

			// Pipeline Connect may take tens of seconds under go-ads flap-backoff
			// when the PLC is busy (route table thrashed by other test runs).
			// 90s absorbs a fully escalated flap sequence plus first notifications.
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(symbolCounts)
			}, 90*time.Second, 500*time.Millisecond).Should(
				BeNumerically(">=", len(expectedSymbols)),
			)

			time.Sleep(3 * time.Second)
			cancel()

			mu.Lock()
			defer mu.Unlock()
			for _, sym := range expectedSymbols {
				sanitized := sanitizeSymbolName(sym)
				Expect(symbolCounts).To(HaveKey(sanitized),
					"symbol %s never received notification", sym)
				Expect(symbolCounts[sanitized]).To(BeNumerically(">=", 2),
					"symbol %s should have received multiple notifications", sym)
			}
		})
	})

	// ---- Config Validation (no PLC needed) ----

	Context("Config Validation", func() {
		It("accepts minimal valid config with all defaults", func() {
			builder := service.NewStreamBuilder()
			err := builder.AddInputYAML(`
ads:
  targetIP: "1.2.3.4"
  targetAMS: "1.2.3.4.1.1"
  symbols:
    - "MAIN.var"
`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects config missing required targetAMS", func() {
			builder := service.NewStreamBuilder()
			err := builder.AddInputYAML(`
ads:
  targetIP: "1.2.3.4"
  symbols:
    - "MAIN.var"
`)
			Expect(err).To(HaveOccurred())
		})

		It("rejects config missing required targetIP", func() {
			builder := service.NewStreamBuilder()
			err := builder.AddInputYAML(`
ads:
  targetAMS: "1.2.3.4.1.1"
  symbols:
    - "MAIN.var"
`)
			Expect(err).To(HaveOccurred())
		})

		It("accepts config without symbols (validated at runtime)", func() {
			builder := service.NewStreamBuilder()
			err := builder.AddInputYAML(`
ads:
  targetIP: "1.2.3.4"
  targetAMS: "1.2.3.4.1.1"
`)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
