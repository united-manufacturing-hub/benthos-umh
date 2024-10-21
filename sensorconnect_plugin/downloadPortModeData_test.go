package sensorconnect_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/v2/sensorconnect_plugin"
)

var _ = Describe("DownloadPortModeData Integration Tests", func() {

	var (
		address string
	)

	BeforeEach(func() {
		address = "10.13.37.178" // AL1350-1
	})

	AfterEach(func() {
	})

	Describe("DownloadPortModeData", func() {
		Context("when the device responds successfully", func() {
			It("should successfully retrieve port mode information", func() {
				// Initialize SensorConnectInput
				input := &sensorconnect_plugin.SensorConnectInput{
					DeviceAddress: address,
					CurrentCid:    0,
				}

				portMap, err := input.GetUsedPortsAndMode()
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
