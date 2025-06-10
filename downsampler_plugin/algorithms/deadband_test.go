// Copyright 2025 UMH Systems GmbH
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

package algorithms_test

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin/algorithms"
)

func TestDeadbandAlgorithms(t *testing.T) {
	if os.Getenv("TEST_DOWNSAMPLER") == "" {
		t.Skip("Skipping downsampler algorithm tests (set TEST_DOWNSAMPLER to enable)")
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "Deadband Algorithm Suite")
}

var _ = Describe("Deadband Algorithm", func() {
	var (
		algo     algorithms.DownsampleAlgorithm
		err      error
		baseTime time.Time
	)

	BeforeEach(func() {
		baseTime = time.Unix(1609459200, 0) // 2021-01-01 00:00:00
	})

	Describe("basic functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"threshold": 0.5,
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should always keep the first point", func() {
			keep, err := algo.ShouldKeep(10.0, nil, baseTime, time.Time{})
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "First point should always be kept")
		})

		It("should filter out small changes", func() {
			keep, err := algo.ShouldKeep(10.3, 10.0, baseTime.Add(time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse(), "Small change (0.3 < 0.5) should be filtered")
		})

		It("should keep large changes", func() {
			keep, err := algo.ShouldKeep(10.6, 10.0, baseTime.Add(2*time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Large change (0.6 >= 0.5) should be kept")
		})

		It("should keep changes at exact threshold", func() {
			keep, err := algo.ShouldKeep(10.5, 10.0, baseTime.Add(3*time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Exact threshold change (0.5 = 0.5) should be kept")
		})
	})

	Describe("max interval functionality", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"threshold":    0.5,
				"max_interval": "60s",
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should filter small changes within time limit", func() {
			keep, err := algo.ShouldKeep(10.3, 10.0, baseTime.Add(30*time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse(), "Small change within time limit should be filtered")
		})

		It("should keep small changes after max interval", func() {
			keep, err := algo.ShouldKeep(10.3, 10.0, baseTime.Add(70*time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Small change after max interval should be kept")
		})
	})

	Describe("data type handling", func() {
		BeforeEach(func() {
			config := map[string]interface{}{
				"threshold": 1.0,
			}
			algo, err = algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle boolean values", func() {
			keep, err := algo.ShouldKeep(true, false, baseTime.Add(time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Boolean change (false->true = 1.0) should be kept")
		})

		It("should handle integer values", func() {
			keep, err := algo.ShouldKeep(11, 10, baseTime.Add(2*time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Integer change (10->11 = 1.0) should be kept")
		})

		It("should keep different strings", func() {
			keep, err := algo.ShouldKeep("stopped", "running", baseTime.Add(3*time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeTrue(), "Different strings should be kept")
		})

		It("should filter identical strings", func() {
			keep, err := algo.ShouldKeep("running", "running", baseTime.Add(4*time.Second), baseTime)
			Expect(err).NotTo(HaveOccurred())
			Expect(keep).To(BeFalse(), "Same strings should be filtered")
		})
	})

	Describe("metadata generation", func() {
		It("should generate correct metadata without max interval", func() {
			config := map[string]interface{}{
				"threshold": 0.5,
			}
			algo, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			metadata := algo.GetMetadata()
			Expect(metadata).To(Equal("deadband(threshold=0.500)"))
		})

		It("should generate correct metadata with max interval", func() {
			config := map[string]interface{}{
				"threshold":    0.5,
				"max_interval": "60s",
			}
			algo, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())

			metadata := algo.GetMetadata()
			Expect(metadata).To(Equal("deadband(threshold=0.500,max_interval=1m0s)"))
		})
	})

	Describe("configuration validation", func() {
		It("should accept valid configuration", func() {
			config := map[string]interface{}{
				"threshold": 1.5,
			}
			_, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject invalid threshold type", func() {
			config := map[string]interface{}{
				"threshold": "invalid",
			}
			_, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).To(HaveOccurred())
		})

		It("should reject invalid max_interval", func() {
			config := map[string]interface{}{
				"threshold":    1.0,
				"max_interval": "invalid",
			}
			_, err := algorithms.NewDeadbandAlgorithm(config)
			Expect(err).To(HaveOccurred())
		})
	})
})
