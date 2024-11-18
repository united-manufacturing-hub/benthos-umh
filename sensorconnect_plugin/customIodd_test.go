package sensorconnect_plugin_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
)

var _ = Describe("FetchAndStoreCustomIODD", func() {
	var (
		ctx          context.Context
		input        *sensorconnect_plugin.SensorConnectInput
		mockServer   *httptest.Server
		deviceConfig sensorconnect_plugin.DeviceConfig
	)

	BeforeEach(func() {
		endpoint := os.Getenv("TEST_DEBUG_IFM_ENDPOINT")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}

		ctx = context.Background()

		// Initialize SensorConnectInput with necessary fields
		input = &sensorconnect_plugin.SensorConnectInput{
			IoDeviceMap:      sync.Map{},
			UseOnlyRawData:   false,
			DeviceAddress:    "192.168.0.1",
			IODDAPI:          "https://management.umh.app/iodd",
			CurrentCid:       0,
			ConnectedDevices: []sensorconnect_plugin.ConnectedDeviceInfo{},
		}

		// Setup mock server to simulate IODD URL responses
		mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Customize response based on URL or request parameters if needed
			// For simplicity, return a static XML response
			ioddXML := `
			<IoDevice>
				<Name>Test Device</Name>
				<Description>Mock IoDevice for Testing</Description>
			</IoDevice>`
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintln(w, ioddXML)
		}))

		// Define DeviceConfig with the mock server URL
		deviceConfig = sensorconnect_plugin.DeviceConfig{
			DeviceID: 1234,
			VendorID: 5678,
			IoddURL:  mockServer.URL + "/customIodd/device1234.iodd",
		}
	})

	AfterEach(func() {
		mockServer.Close()
	})

	Describe("FetchAndStoreIoddFromURL", func() {
		Context("Successful IODD Fetch and Store", func() {
			It("should fetch IODD from custom URL and store it in IoDeviceMap", func() {
				err := input.FetchAndStoreIoddFromURL(ctx, deviceConfig)
				Expect(err).ToNot(HaveOccurred())

				fileMapKey := sensorconnect_plugin.IoddFilemapKey{
					VendorId: int64(deviceConfig.VendorID),
					DeviceId: deviceConfig.DeviceID,
				}

				value, ok := input.IoDeviceMap.Load(fileMapKey)
				Expect(ok).To(BeTrue())
				Expect(value).ToNot(BeNil())
			})
		})

		Context("Failed IODD Fetch due to HTTP Error", func() {
			It("should return an error when the IODD URL is unreachable", func() {
				// Shutdown the mock server to simulate unreachable URL
				mockServer.Close()

				err := input.FetchAndStoreIoddFromURL(ctx, deviceConfig)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to download IODD from URL"))
			})
		})

		Context("Failed IODD Fetch due to Invalid XML", func() {
			It("should return an error when the IODD XML is malformed", func() {
				// Reconfigure the mock server to return invalid XML
				mockServer.Close()
				mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					invalidXML := `<IoDevice><Name>Test Device</Name><Description>Missing closing tag`
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprintln(w, invalidXML)
				}))

				// Update deviceConfig with the new mock server URL
				deviceConfig.IoddURL = mockServer.URL + "/customIodd/device1234.iodd"

				err := input.FetchAndStoreIoddFromURL(ctx, deviceConfig)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to unmarshal IODD XML"))
			})
		})

	})
})
