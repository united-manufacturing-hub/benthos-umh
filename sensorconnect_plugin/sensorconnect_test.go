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
	"os"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
)

var _ = Describe("Sensorconnect", func() {
	var endpoint string

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_DEBUG_IFM_ENDPOINT")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}
	})

	When("CID is set to an invalid CID", func() {
		It("should work for negative cid", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			input := &sensorconnect_plugin.SensorConnectInput{
				DeviceAddress: endpoint,
				CurrentCid:    -5,
			}

			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should work for out of bounds cid", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			input := &sensorconnect_plugin.SensorConnectInput{
				DeviceAddress: endpoint,
				CurrentCid:    32764,
			}

			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("ReadBatch", func() {
		It("should receive data from the AL1350", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			input := &sensorconnect_plugin.SensorConnectInput{
				DeviceAddress: endpoint,
				CurrentCid:    0,
			}

			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Read a batch of messages
			msgs, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())
			fmt.Printf("Received %d messages\n", len(msgs))
			Expect(msgs).ToNot(BeEmpty())

			for _, message := range msgs {
				messageStruct, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())

				portMode, found := message.MetaGetMut("sensorconnect_port_mode")
				Expect(found).To(BeTrue())
				Expect(portMode).To(Equal("io-link"))

				portNumber, found := message.MetaGet("sensorconnect_port_number")
				Expect(found).To(BeTrue())
				Expect(portNumber).To(Equal("1"))

				portIolinkVendorId, found := message.MetaGet("sensorconnect_port_iolink_vendor_id")
				Expect(found).To(BeTrue())
				Expect(portIolinkVendorId).To(Equal("310"))

				portIolinkDeviceId, found := message.MetaGet("sensorconnect_port_iolink_device_id")
				Expect(found).To(BeTrue())
				Expect(portIolinkDeviceId).To(Equal("1028"))

				portIolinkProductName, found := message.MetaGet("sensorconnect_port_iolink_product_name")
				Expect(found).To(BeTrue())
				Expect(portIolinkProductName).To(Equal("VVB001"))

				portIolinkSerial, found := message.MetaGet("sensorconnect_port_iolink_serial")
				Expect(found).To(BeTrue())
				Expect(portIolinkSerial).To(Equal("000008512993"))

				deviceUrl, found := message.MetaGet("sensorconnect_device_url")
				Expect(found).To(BeTrue())
				Expect(deviceUrl).To(Equal("http://" + endpoint))

				deviceProductCode, found := message.MetaGet("sensorconnect_device_product_code")
				Expect(found).To(BeTrue())
				Expect(deviceProductCode).To(Equal("AL1350"))

				deviceSerialNumber, found := message.MetaGet("sensorconnect_device_serial_number")
				Expect(found).To(BeTrue())
				Expect(deviceSerialNumber).To(Equal("000201610237"))

				// Check if messageStruct of type any has the field {"Crest":41,"Device status":0,"OUT1":true,"OUT2":true,"Temperature":394,"a-Peak":2,"a-Rms":0,"v-Rms":0}

				// Type assert messageStruct to map[string]interface{}
				payload, ok := messageStruct.(map[string]interface{})
				Expect(ok).To(BeTrue(), "messageStruct should be of type map[string]interface{}")

				expectedFields := map[string]interface{}{
					"Crest":         41,
					"Device status": 0,
					"OUT1":          true,
					"OUT2":          true,
					"Temperature":   394,
					"a-Peak":        2,
					"a-Rms":         0,
					"v-Rms":         0,
				}

				// Iterate over each expected field and assert its presence and value
				for key := range expectedFields {
					_, exists := payload[key]
					Expect(exists).To(BeTrue(), fmt.Sprintf("Field '%s' should exist in the messageStruct", key))
				}

				fmt.Printf("Received messageBytes: %+v\n", messageStruct)
			}

			// Close the connection
			input.Close(ctx)
		})

		It("should receive raw data from the AL1350 ", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			input := &sensorconnect_plugin.SensorConnectInput{
				DeviceAddress:  endpoint,
				CurrentCid:     0,
				UseOnlyRawData: true,
			}

			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Read a batch of messages
			msgs, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())
			fmt.Printf("Received %d messages\n", len(msgs))
			Expect(msgs).ToNot(BeEmpty())

			for _, message := range msgs {
				messageStruct, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())

				portMode, found := message.MetaGetMut("sensorconnect_port_mode")
				Expect(found).To(BeTrue())
				Expect(portMode).To(Equal("io-link"))

				portNumber, found := message.MetaGet("sensorconnect_port_number")
				Expect(found).To(BeTrue())
				Expect(portNumber).To(Equal("1"))

				portIolinkVendorId, found := message.MetaGet("sensorconnect_port_iolink_vendor_id")
				Expect(found).To(BeTrue())
				Expect(portIolinkVendorId).To(Equal("310"))

				portIolinkDeviceId, found := message.MetaGet("sensorconnect_port_iolink_device_id")
				Expect(found).To(BeTrue())
				Expect(portIolinkDeviceId).To(Equal("1028"))

				portIolinkProductName, found := message.MetaGet("sensorconnect_port_iolink_product_name")
				Expect(found).To(BeTrue())
				Expect(portIolinkProductName).To(Equal("VVB001"))

				portIolinkSerial, found := message.MetaGet("sensorconnect_port_iolink_serial")
				Expect(found).To(BeTrue())
				Expect(portIolinkSerial).To(Equal("000008512993"))

				deviceUrl, found := message.MetaGet("sensorconnect_device_url")
				Expect(found).To(BeTrue())
				Expect(deviceUrl).To(Equal("http://" + endpoint))

				deviceProductCode, found := message.MetaGet("sensorconnect_device_product_code")
				Expect(found).To(BeTrue())
				Expect(deviceProductCode).To(Equal("AL1350"))

				deviceSerialNumber, found := message.MetaGet("sensorconnect_device_serial_number")
				Expect(found).To(BeTrue())
				Expect(deviceSerialNumber).To(Equal("000201610237"))

				// Check if messageStruct of type any has the field {"Crest":41,"Device status":0,"OUT1":true,"OUT2":true,"Temperature":394,"a-Peak":2,"a-Rms":0,"v-Rms":0}

				// Type assert messageStruct to map[string]interface{}
				payload, ok := messageStruct.(map[string]interface{})
				Expect(ok).To(BeTrue(), "messageStruct should be of type map[string]interface{}")

				expectedFields := map[string]interface{}{
					"raw_sensor_output": "MDAwMEZDMDAwMDAyRkYwMDAwMDBGRjAwMDE4RkZGMDAwMDI3RkYwMw==",
				}

				// Iterate over each expected field and assert its presence and value
				for key := range expectedFields {
					_, exists := payload[key]
					Expect(exists).To(BeTrue(), fmt.Sprintf("Field '%s' should exist in the messageStruct", key))
				}

				fmt.Printf("Received messageBytes: %+v\n", messageStruct)
			}

			// Close the connection
			input.Close(ctx)
		})
	})

	When("using a yaml and stream builder", func() {
		It("should receive data", func() {
			// Create a new stream builder
			builder := service.NewStreamBuilder()

			// Create a new stream
			err := builder.AddInputYAML(fmt.Sprintf(`
sensorconnect:
  device_address: "%s"
`, endpoint))

			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetTracerYAML(`type: none`)
			Expect(err).NotTo(HaveOccurred())

			// Add a total message count consumer
			var count int64
			err = builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
				atomic.AddInt64(&count, 1)
				return err
			})

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			timeout := time.Second * 45

			// Run the stream
			ctx, cncl := context.WithTimeout(context.Background(), timeout)
			go func() {
				_ = stream.Run(ctx)
			}()

			// Check if we received any messages continuously
			Eventually(
				func() int64 {
					return atomic.LoadInt64(&count)
				}, timeout).Should(BeNumerically(">", int64(0)))

			cncl()
		})
	})
})
