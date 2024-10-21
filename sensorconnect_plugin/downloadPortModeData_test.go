package sensorconnect_plugin_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"os"

	"github.com/united-manufacturing-hub/benthos-umh/v2/sensorconnect_plugin"
)

var _ = Describe("DownloadPortModeData Integration Tests", func() {

	var endpoint string

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_DEBUG_IFM_ENDPOINT")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}

	})

	AfterEach(func() {
	})

	Describe("DownloadPortModeData", func() {
		Context("when the device responds successfully", func() {
			It("should successfully retrieve port mode information", func() {
				// Initialize SensorConnectInput
				input := &sensorconnect_plugin.SensorConnectInput{
					DeviceAddress: endpoint,
					CurrentCid:    0,
				}

				portMap, err := input.GetUsedPortsAndMode(context.Background())
				Expect(err).NotTo(HaveOccurred())

				Expect(portMap[1].Mode).To(Equal(uint(3)))
				Expect(portMap[1].Connected).To(BeTrue())
				Expect(portMap[1].DeviceID).To(Equal(uint(1028)))
				Expect(portMap[1].VendorID).To(Equal(uint(310)))
				Expect(portMap[1].ProductName).To(Equal("VVB001"))
				Expect(portMap[1].Serial).To(Equal("000008512993"))
			})
		})
	})
})
