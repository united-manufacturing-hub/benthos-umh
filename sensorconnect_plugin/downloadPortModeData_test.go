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

package sensorconnect_plugin_test

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"os"

	"github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
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

				portMap, err := input.GetConnectedDevices(context.Background())
				Expect(err).NotTo(HaveOccurred())

				// Debugging: print out the port map including mode, connected, deviceID, vendorID, productName, serial
				for i, port := range portMap {
					debugInfo := fmt.Sprintf(
						"Port %d:\n  Mode: %d\n  Connected: %t\n  DeviceID: %d\n  VendorID: %d\n  ProductName: %s\n  Serial: %s\n",
						i, port.Mode, port.Connected, port.DeviceID, port.VendorID, port.ProductName, port.Serial,
					)
					_, err := GinkgoWriter.Write([]byte(debugInfo))
					if err != nil {
						Fail(fmt.Sprintf("Failed to write debug info: %v", err))
					}
				}

				Expect(portMap[0].Mode).To(Equal(uint(3)))
				Expect(portMap[0].Connected).To(BeTrue())
				Expect(portMap[0].DeviceID).To(Equal(uint(1028)))
				Expect(portMap[0].VendorID).To(Equal(uint(310)))
				Expect(portMap[0].ProductName).To(Equal("VVB001"))
				Expect(portMap[0].Serial).To(Equal("000008512993"))
			})
		})
	})
})
