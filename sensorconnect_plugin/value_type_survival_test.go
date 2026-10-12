// Copyright 2026 UMH Systems GmbH
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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin/tagprocessortest"
)

const (
	survivalVendorID      = 310
	survivalDeviceID      = 1234
	survivalProcessDataID = "TI_PD_Value"
	survivalProcessData   = "Value"
)

var digitalInputPort = sensorconnect_plugin.ConnectedDeviceInfo{Uri: "/iolinkmaster/port[1]", Mode: 1, Connected: true, Port: "1"}

var ioLinkPort = sensorconnect_plugin.ConnectedDeviceInfo{
	Uri:       "/iolinkmaster/port[2]",
	Mode:      3,
	Connected: true,
	Port:      "2",
	VendorID:  survivalVendorID,
	DeviceID:  survivalDeviceID,
}

func ioddWithProcessDataIn(processDataIn sensorconnect_plugin.Datatype) sensorconnect_plugin.IoDevice {
	return sensorconnect_plugin.IoDevice{
		ExternalTextCollection: sensorconnect_plugin.ExternalTextCollection{PrimaryLanguage: sensorconnect_plugin.PrimaryLanguage{Text: []sensorconnect_plugin.Text{
			{Id: survivalProcessDataID, Value: survivalProcessData},
		}}},
		ProfileBody: sensorconnect_plugin.ProfileBody{DeviceFunction: sensorconnect_plugin.DeviceFunction{ProcessDataCollection: sensorconnect_plugin.ProcessDataCollection{ProcessData: sensorconnect_plugin.ProcessData{
			ProcessDataIn: sensorconnect_plugin.ProcessDataIn{Name: sensorconnect_plugin.Name{TextId: survivalProcessDataID}, Datatype: processDataIn},
		}}}},
	}
}

var _ = Describe("sensorconnect value type through tag_processor", func() {
	// the IO-Link master answers with JSON, so each pin2in value is what
	// encoding/json decodes from the master's response
	DescribeTable("keeps the type and value of a digital-input port",
		func(masterValue any) {
			input := &sensorconnect_plugin.SensorConnectInput{}
			sensorData := map[string]any{
				digitalInputPort.Uri + "/pin2in": map[string]any{"code": float64(200), "data": masterValue},
			}

			batch, err := input.ProcessSensorData(context.Background(), []sensorconnect_plugin.ConnectedDeviceInfo{digitalInputPort}, sensorData)
			Expect(err).NotTo(HaveOccurred())
			Expect(batch).To(HaveLen(1))

			tagprocessortest.ExpectTagProcessorKeepsValue(masterValue, batch[0])
		},
		Entry("numeric string", tagprocessortest.NumericString),
		Entry("non-numeric string", tagprocessortest.NonNumericString),
		Entry("17-digit numeric string", tagprocessortest.SeventeenDigitString),
		Entry("number", float64(tagprocessortest.PositiveInteger)),
		Entry("fractional number", tagprocessortest.FractionalNumber),
		Entry("boolean", true),
	)

	DescribeTable("keeps the type and value of an IO-Link process data value",
		func(processDataIn sensorconnect_plugin.Datatype, pdinHex string, sensorValue any) {
			input := &sensorconnect_plugin.SensorConnectInput{}
			ioddKey := sensorconnect_plugin.IoddFilemapKey{VendorId: survivalVendorID, DeviceId: survivalDeviceID}
			input.IoDeviceMap.Store(ioddKey, ioddWithProcessDataIn(processDataIn))
			sensorData := map[string]any{
				ioLinkPort.Uri + "/iolinkdevice/pdin": map[string]any{"code": float64(200), "data": pdinHex},
			}

			batch, err := input.ProcessSensorData(context.Background(), []sensorconnect_plugin.ConnectedDeviceInfo{ioLinkPort}, sensorData)
			Expect(err).NotTo(HaveOccurred())
			Expect(batch).To(HaveLen(1))

			tagprocessortest.ExpectTagProcessorKeepsPayloadField(sensorValue, batch[0], survivalProcessData)
		},
		Entry("UIntegerT", sensorconnect_plugin.Datatype{Type: "UIntegerT", BitLength: 16}, "04D2", 1234),
		Entry("IntegerT", sensorconnect_plugin.Datatype{Type: "IntegerT", BitLength: 16}, "FFFE", tagprocessortest.NegativeInteger),
		Entry("Float32T", sensorconnect_plugin.Datatype{Type: "Float32T", BitLength: 32}, "41BC0000", tagprocessortest.FractionalNumber),
		Entry("BooleanT", sensorconnect_plugin.Datatype{Type: "BooleanT", BitLength: 8}, "01", true),
		Entry("OctetStringT", sensorconnect_plugin.Datatype{Type: "OctetStringT", FixedLength: 2}, "1234", "1234"),
	)
})
