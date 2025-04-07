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

package sensorconnect_plugin

import (
	"context"
	"fmt"
)

// SensorDataInformation represents the response structure for sensor data
type SensorDataInformation struct {
	Data map[string]interface{} `json:"data"`
	Cid  int                    `json:"cid"`
}

// GetSensorDataMap retrieves sensor data from the connected devices
func (s *SensorConnectInput) GetSensorDataMap(ctx context.Context) (map[string]interface{}, error) {
	// Get the connected ports and their modes
	devices := s.ConnectedDevices
	if len(devices) == 0 {
		// No devices connected, return empty map
		s.logger.Warn("No devices connected to any ports")
		return make(map[string]interface{}), nil
	}

	// Create the request data for sensor data
	requestData, err := s.createSensorDataRequestData(devices)
	if err != nil {
		s.logger.Errorf("Failed to create sensor data request: %v", err)
		return nil, err
	}

	// Send the request to the device
	response, err := s.SendRequestToDevice(ctx, requestData)
	if err != nil {
		s.logger.Errorf("Failed to send request to device: %v", err)
		return nil, err
	}

	// Unmarshal the sensor data from the response
	sensorDataMap, err := s.unmarshalSensorData(response)
	if err != nil {
		s.logger.Errorf("Failed to unmarshal sensor data: %v", err)
		return nil, err
	}

	return sensorDataMap, nil
}

// createSensorDataRequestData creates the request data to fetch sensor data from connected devices
func (s *SensorConnectInput) createSensorDataRequestData(connectedDeviceInfo []ConnectedDeviceInfo) (map[string]interface{}, error) {
	datatosend := []string{}

	for _, device := range connectedDeviceInfo {
		if !device.Connected {
			continue
		}

		var query string
		switch device.Mode {
		// DI mode
		case 1:
			query = device.Uri + "/pin2in"
		// DO mode
		case 2:
			return nil, fmt.Errorf("DO mode is currently not supported for %s", device.Uri)
		// IO-Link mode
		case 3:
			query = device.Uri + "/iolinkdevice/pdin"
		default:
			return nil, fmt.Errorf("invalid IO-Link port mode: %d for %s", device.Mode, device.Uri)
		}
		datatosend = append(datatosend, query)
	}

	if len(datatosend) == 0 {
		return nil, fmt.Errorf("no valid data points to request")
	}

	requestData := map[string]interface{}{
		"code": "request",
		"adr":  "/getdatamulti",
		"data": map[string]interface{}{
			"datatosend": datatosend,
		},
	}

	return requestData, nil
}

// unmarshalSensorData processes the response from the device and extracts sensor data
func (s *SensorConnectInput) unmarshalSensorData(response map[string]interface{}) (map[string]interface{}, error) {
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("response data is missing or invalid")
	}

	sensorDataMap := make(map[string]interface{})
	for key, element := range data {
		sensorDataMap[key] = element
	}
	return sensorDataMap, nil
}
