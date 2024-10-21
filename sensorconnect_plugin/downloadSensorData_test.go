package sensorconnect_plugin_test

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/v2/sensorconnect_plugin"
)

var _ = Describe("DownloadSensorData Integration Tests", func() {

	var (
		address string
	)

	BeforeEach(func() {
		address = "10.13.37.178" // AL1350-1
	})

	AfterEach(func() {
	})

	Describe("DownloadSensorData", func() {
		Context("when the device responds successfully", func() {
			It("should successfully retrieve port mode information", func() {
				// Initialize SensorConnectInput
				input := &sensorconnect_plugin.SensorConnectInput{
					DeviceAddress: address,
					CurrentCid:    0,
				}

				portMap, err := input.GetUsedPortsAndMode(context.Background())
				Expect(err).NotTo(HaveOccurred())
				input.CurrentPortMap = portMap

				dataMap, err := input.GetSensorDataMap(context.Background())
				Expect(err).NotTo(HaveOccurred())

				fmt.Printf("%v\n", dataMap)

				Fail("test")
			})
		})
	})
})
