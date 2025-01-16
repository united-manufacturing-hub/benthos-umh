package sensorconnect_plugin_test

import (
	"context"
	"fmt"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
)

var _ = Describe("DownloadSensorData Integration Tests", func() {

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

	Describe("DownloadSensorData", func() {
		Context("when the device responds successfully", func() {
			It("should successfully retrieve port mode information", func() {
				// Initialize SensorConnectInput
				input := &sensorconnect_plugin.SensorConnectInput{
					DeviceAddress: endpoint,
					CurrentCid:    0,
				}

				portMap, err := input.GetConnectedDevices(context.Background())
				Expect(err).NotTo(HaveOccurred())
				input.ConnectedDevices = portMap

				dataMap, err := input.GetSensorDataMap(context.Background())
				Expect(err).NotTo(HaveOccurred())

				fmt.Printf("%v\n", dataMap)
			})
		})
	})
})
