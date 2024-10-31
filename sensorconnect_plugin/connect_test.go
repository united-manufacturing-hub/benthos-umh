package sensorconnect_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
)

var _ = Describe("SensorConnect Plugin Unittests", func() {

	var (
		testServer *httptest.Server
		address    string
	)

	BeforeEach(func() {

		endpoint := os.Getenv("TEST_DEBUG_IFM_ENDPOINT")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}

		// Set up a test HTTP server
		testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Read request body
			bodyBytes, err := io.ReadAll(r.Body)
			Expect(err).NotTo(HaveOccurred())
			defer r.Body.Close()

			// Parse request body
			var requestMap map[string]interface{}
			err = json.Unmarshal(bodyBytes, &requestMap)
			Expect(err).NotTo(HaveOccurred())

			// Extract cid from request
			cid, ok := requestMap["cid"].(float64)
			Expect(ok).To(BeTrue(), "Request does not contain 'cid' field")

			// Respond with a JSON response, including the same cid
			response := map[string]interface{}{
				"data": map[string]interface{}{
					"/deviceinfo/serialnumber/": map[string]interface{}{
						"data": "SN123456",
						"code": 200,
					},
					"/deviceinfo/productcode/": map[string]interface{}{
						"data": "PN987654",
						"code": 200,
					},
				},
				"cid": int(cid),
			}

			responseBytes, err := json.Marshal(response)
			Expect(err).NotTo(HaveOccurred())

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(responseBytes)
		}))

		address = testServer.URL[len("http://"):] // Extract the address part
	})

	AfterEach(func() {
		testServer.Close()
	})

	Describe("GetDeviceInformation", func() {
		Context("when the device responds successfully", func() {
			It("should successfully retrieve device information and validate cid", func() {

				// Initialize SensorConnectInput
				input := &sensorconnect_plugin.SensorConnectInput{
					DeviceAddress: address,
					CurrentCid:    0,
				}

				deviceInfo, err := input.GetDeviceInformation(context.Background())
				Expect(err).NotTo(HaveOccurred())
				Expect(deviceInfo.ProductCode).To(Equal("PN987654"))
				Expect(deviceInfo.SerialNumber).To(Equal("SN123456"))
				Expect(deviceInfo.URL).To(Equal(fmt.Sprintf("http://%s", address)))
			})
		})

		Context("when the device returns an error", func() {
			BeforeEach(func() {
				testServer.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintln(w, "Internal Server Error")
				})
			})

			It("should return an error", func() {

				input := &sensorconnect_plugin.SensorConnectInput{
					DeviceAddress: address,
					CurrentCid:    0,
				}

				_, err := input.GetDeviceInformation(context.Background())
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unexpected status code 500"))
			})
		})

		Context("when the response cid does not match", func() {
			BeforeEach(func() {
				testServer.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// Read request body
					bodyBytes, err := io.ReadAll(r.Body)
					Expect(err).NotTo(HaveOccurred())
					defer r.Body.Close()

					// Parse request body
					var requestMap map[string]interface{}
					err = json.Unmarshal(bodyBytes, &requestMap)
					Expect(err).NotTo(HaveOccurred())

					// Incorrect cid in response
					response := map[string]interface{}{
						"data": map[string]interface{}{
							"/deviceinfo/serialnumber/": map[string]interface{}{
								"data": "SN123456",
								"code": 200,
							},
							"/deviceinfo/productcode/": map[string]interface{}{
								"data": "PN987654",
								"code": 200,
							},
						},
						"cid": 9999, // Deliberately incorrect cid
					}

					responseBytes, err := json.Marshal(response)
					Expect(err).NotTo(HaveOccurred())

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.Write(responseBytes)
				})
			})

			It("should return an error due to cid mismatch", func() {

				input := &sensorconnect_plugin.SensorConnectInput{
					DeviceAddress: address,
					CurrentCid:    0,
				}

				_, err := input.GetDeviceInformation(context.Background())
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unexpected correlation ID in response"))
			})
		})
	})

	Context("when the device returns a non-200 code in data points", func() {
		BeforeEach(func() {
			testServer.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Read request body
				bodyBytes, err := io.ReadAll(r.Body)
				Expect(err).NotTo(HaveOccurred())
				defer r.Body.Close()

				// Parse request body
				var requestMap map[string]interface{}
				err = json.Unmarshal(bodyBytes, &requestMap)
				Expect(err).NotTo(HaveOccurred())

				cid, ok := requestMap["cid"].(float64)
				Expect(ok).To(BeTrue(), "Request does not contain 'cid' field")

				// Respond with a JSON response, including the same cid but with non-200 code
				response := map[string]interface{}{
					"data": map[string]interface{}{
						"/deviceinfo/serialnumber/": map[string]interface{}{
							"data": "",
							"code": 530, // Non-200 code
						},
						"/deviceinfo/productcode/": map[string]interface{}{
							"data": "",
							"code": 530, // Non-200 code
						},
					},
					"cid": int(cid),
				}

				responseBytes, err := json.Marshal(response)
				Expect(err).NotTo(HaveOccurred())

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(responseBytes)
			})
		})

		It("should return an error due to non-200 code in data points", func() {

			input := &sensorconnect_plugin.SensorConnectInput{
				DeviceAddress: address,
				CurrentCid:    0,
			}

			_, err := input.GetDeviceInformation(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("requested data is invalid"))
		})
	})
})

var _ = Describe("SensorConnect Integration Tests", func() {

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

	Describe("GetDeviceInformation", func() {
		Context("when the device responds successfully", func() {
			It("should successfully retrieve device information", func() {
				// Initialize SensorConnectInput
				input := &sensorconnect_plugin.SensorConnectInput{
					DeviceAddress: endpoint,
					CurrentCid:    0,
				}

				deviceInfo, err := input.GetDeviceInformation(context.Background())
				Expect(err).NotTo(HaveOccurred())
				Expect(deviceInfo.ProductCode).To(Equal("AL1350"))
				Expect(deviceInfo.SerialNumber).To(Equal("000201610237"))
				Expect(deviceInfo.URL).To(Equal(fmt.Sprintf("http://%s", endpoint)))
			})
		})
	})
})
