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

package modbus_plugin_test

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/grid-x/modbus"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/modbus_plugin"
)

var _ = Describe("Test Against Docker Modbus Simulator", func() {
	BeforeEach(func() {
		testModbusSimulator := os.Getenv("TEST_MODBUS_SIMULATOR")

		// Check if environment variables are set
		if testModbusSimulator == "" {
			Skip("Skipping test: environment variables not set")
			return
		}
	})

	It("should connect and read discrete register", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		input := &ModbusInput{
			SlaveIDs:    []byte{1},
			BusyRetries: 1,
			Addresses: []ModbusDataItemWithAddress{
				{
					Name:     "firstFlagOfDiscreteInput",
					Register: "discrete",
					Address:  1,
					Type:     "BIT",
					Output:   "BOOL",
				},
				{
					Name:     "secondFlagOfDiscreteInput",
					Register: "discrete",
					Address:  2,
					Type:     "BIT",
					Output:   "BOOL",
				},
				{
					Name:     "fourthFlagOfDiscreteInput",
					Register: "discrete",
					Address:  4,
					Type:     "BIT",
					Output:   "BOOL",
				},
			},
			Handler: modbus.NewTCPClientHandler("127.0.0.1:50502"),
		}
		input.Client = modbus.NewClient(input.Handler)

		// Parse the addresses into batches
		var err error
		input.RequestSet, err = input.CreateBatchesFromAddresses(input.Addresses)
		Expect(err).NotTo(HaveOccurred())

		// Attempt to connect
		err = input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		messageBatch, _, err := input.ReadBatch(ctx)
		Expect(err).NotTo(HaveOccurred())

		Expect(messageBatch).To(HaveLen(3))

		for _, message := range messageBatch {
			messageStruct, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())

			tagName, exists := message.MetaGet("modbus_tag_name")
			Expect(exists).To(BeTrue())
			Expect(err).NotTo(HaveOccurred())

			Expect(messageStruct).To(BeAssignableToTypeOf(true))
			switch tagName {
			case "firstFlagOfDiscreteInput":
				Expect(messageStruct).To(BeTrue())
			case "secondFlagOfDiscreteInput":
				Expect(messageStruct).To(BeFalse())
			case "fourthFlagOfDiscreteInput":
				Expect(messageStruct).To(BeFalse())
			}

			GinkgoWriter.Printf("Received message: %+v\n", messageStruct)
		}

		// Close connection
		err = input.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should connect and read input register", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		input := &ModbusInput{
			SlaveIDs:    []byte{1},
			BusyRetries: 1,
			Addresses: []ModbusDataItemWithAddress{
				{
					Name:     "zeroElementOfInputRegister",
					Register: "input",
					Address:  0,
					Type:     "UINT16",
				},
				{
					Name:     "firstElementOfInputRegister",
					Register: "input",
					Address:  1,
					Type:     "UINT16",
				},
				{
					Name:     "eighthElementOfInputRegister",
					Register: "input",
					Address:  8,
					Type:     "UINT16",
				},
			},
			Handler: modbus.NewTCPClientHandler("127.0.0.1:50502"),
		}
		input.Client = modbus.NewClient(input.Handler)

		// Parse the addresses into batches
		var err error
		input.RequestSet, err = input.CreateBatchesFromAddresses(input.Addresses)
		Expect(err).NotTo(HaveOccurred())

		// Attempt to connect
		err = input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		messageBatch, _, err := input.ReadBatch(ctx)
		Expect(err).NotTo(HaveOccurred())

		Expect(messageBatch).To(HaveLen(3))

		for _, message := range messageBatch {
			messageStruct, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())

			tagName, exists := message.MetaGet("modbus_tag_name")
			Expect(exists).To(BeTrue())
			Expect(err).NotTo(HaveOccurred())

			var zeroElement json.Number = "34009"
			var firstElement json.Number = "16877"
			var eighthElement json.Number = "116"
			Expect(messageStruct).To(BeAssignableToTypeOf(zeroElement))

			switch tagName {
			case "zeroElementOfInputRegister":

				Expect(messageStruct).To(Equal(zeroElement))
			case "firstElementOfInputRegister":
				Expect(messageStruct).To(Equal(firstElement))
			case "eighthElementOfInputRegister":
				Expect(messageStruct).To(Equal(eighthElement))
			}

			GinkgoWriter.Printf("Received message: %+v\n", messageStruct)
		}

		// Close connection
		err = input.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("Test Against Wago-PLC", func() {
	var wagoModbusEndpoint string

	BeforeEach(func() {
		wagoModbusEndpoint = os.Getenv("TEST_WAGO_MODBUS_ENDPOINT")

		// Check if environment variables are set
		if wagoModbusEndpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}
	})

	type ModbusRegister struct {
		Addresses     []ModbusDataItemWithAddress
		ExpectedValue json.Number
	}

	DescribeTable("Read discrete/coil/input/holding", func(tableEntry ModbusRegister) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		input := &ModbusInput{
			SlaveIDs:    []byte{1},
			BusyRetries: 1,
			Addresses:   tableEntry.Addresses,
			Handler:     modbus.NewTCPClientHandler(wagoModbusEndpoint),
		}
		input.Client = modbus.NewClient(input.Handler)

		var err error
		input.RequestSet, err = input.CreateBatchesFromAddresses(input.Addresses)
		Expect(err).NotTo(HaveOccurred())

		err = input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		messageBatch, _, err := input.ReadBatch(ctx)
		Expect(err).NotTo(HaveOccurred())

		// increase the batch-length by 1 because of the heartbeat
		Expect(messageBatch).To(HaveLen(len(tableEntry.Addresses) + 1))

		for _, message := range messageBatch {

			tagName, exists := message.MetaGet("modbus_tag_name")
			Expect(exists).To(BeTrue())

			// skip the heartbeat if processed
			if tagName == "heartbeat" {
				GinkgoWriter.Printf("Skipping heartbeat message: %+v\n", message)
				continue
			}
			Expect(tagName).To(Equal(tableEntry.Addresses[0].Name))

			register, exists := message.MetaGet("modbus_tag_register")
			Expect(exists).To(BeTrue())
			Expect(register).To(Equal(tableEntry.Addresses[0].Register))

			address, exists := message.MetaGet("modbus_tag_address")
			Expect(exists).To(BeTrue())
			Expect(address).To(Equal(strconv.FormatUint(uint64(tableEntry.Addresses[0].Address), 10)))

			messageStruct, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())

			Expect(messageStruct).To(BeAssignableToTypeOf(tableEntry.ExpectedValue))
			Expect(messageStruct).To(Equal(tableEntry.ExpectedValue))

			GinkgoWriter.Printf("Received message: %+v\n", messageStruct)
		}

		// Close connection
		err = input.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
	},
		Entry("discrete (input)",
			ModbusRegister{
				Addresses: []ModbusDataItemWithAddress{
					{
						Name:     "modbusBoolIn",
						Register: "discrete",
						Address:  0,
						Type:     "BOOL",
					},
				},
				ExpectedValue: "1",
			}),
		Entry("coil (output)",
			ModbusRegister{
				Addresses: []ModbusDataItemWithAddress{
					{
						Name:     "modbusBoolOut",
						Register: "coil",
						Address:  1,
						Type:     "BOOL",
					},
				},
				ExpectedValue: "1",
			}),
		Entry("input (input)",
			ModbusRegister{
				Addresses: []ModbusDataItemWithAddress{
					{
						Name:     "modbusIntIn",
						Register: "input",
						Address:  1,
						Type:     "INT16",
					},
				},
				ExpectedValue: "1234",
			}),
		Entry("holding (output)",
			ModbusRegister{
				Addresses: []ModbusDataItemWithAddress{
					{
						Name:     "modbusIntOut",
						Register: "holding",
						Address:  2,
						Type:     "INT16",
					},
				},
				ExpectedValue: "1234",
			}),
	)
})
