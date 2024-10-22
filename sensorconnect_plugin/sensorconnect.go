// Copyright 2024 UMH Systems GmbH
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
	"github.com/redpanda-data/benthos/v4/public/service"
	"sync"
	"time"
)

type SensorConnectInput struct {
	// Configuration fields
	DeviceAddress string

	// Internal fields
	DeviceInfo      DeviceInformation
	CurrentPortMap  map[int]ConnectedDeviceInfo
	lastPortMapTime time.Time
	mu              sync.Mutex
	logger          *service.Logger
	CurrentCid      int

	IoDeviceMap sync.Map // IoDeviceMap to store IoDevices
}

// ConfigSpec defines the plugin's configuration spec
func ConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("An input plugin that discovers devices and collects sensor data.").
		Description("This plugin replaces the 'sensorconnect' microservice as a Benthos plugin.").
		// Define all your configuration fields here
		Field(service.NewStringField("device_address").Description("IP address or hostname of the IFM IO-Link master device"))
}

// NewSensorConnectInput creates a new instance of SensorConnectInput
func NewSensorConnectInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	logger := mgr.Logger()

	input := &SensorConnectInput{
		logger:     logger,
		CurrentCid: 0,
	}

	var err error
	if input.DeviceAddress, err = conf.FieldString("device_address"); err != nil {
		return nil, err
	}

	// Validate that DeviceAddress is provided
	if input.DeviceAddress == "" {
		return nil, fmt.Errorf("'device_address' must be provided")
	}

	return input, nil
}

// Connect establishes connections and starts background processes
func (s *SensorConnectInput) Connect(ctx context.Context) error {
	s.logger.Infof("Connecting to device at %s", s.DeviceAddress)

	// Get device information
	deviceInfo, err := s.GetDeviceInformation(ctx)
	if err != nil {
		s.logger.Errorf("Failed to connect to device at %s: %v", s.DeviceAddress, err)
		return err
	}

	s.DeviceInfo = deviceInfo
	s.logger.Infof("Connected to device at %s (SN: %s, PN: %s)", deviceInfo.URL, deviceInfo.SerialNumber, deviceInfo.ProductCode)

	// Get Port Map and Print
	portMap, err := s.GetUsedPortsAndMode(ctx)
	if err != nil {
		s.logger.Errorf("Failed to fetch port map %s: %v", s.DeviceAddress, err)
		return err
	}

	s.mu.Lock()
	s.CurrentPortMap = portMap
	s.lastPortMapTime = time.Now()
	s.mu.Unlock()

	s.logger.Infof("Port Map for device at %s:", s.DeviceAddress)
	for port, info := range portMap {
		s.logger.Infof(
			"Port %d:\n"+
				"  Mode        : %d\n"+
				"  Connected   : %t\n"+
				"  DeviceID    : %d\n"+
				"  VendorID    : %d\n"+
				"  ProductName : %s\n"+
				"  Serial      : %s\n",
			port,
			info.Mode,
			info.Connected,
			info.DeviceID,
			info.VendorID,
			info.ProductName,
			info.Serial,
		)
	}

	return nil
}

// ReadBatch reads data from sensors and returns it as a batch of messages
func (s *SensorConnectInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	s.mu.Lock()
	timeSinceLastUpdate := time.Since(s.lastPortMapTime)
	if timeSinceLastUpdate >= 10*time.Second {
		s.logger.Infof("10 seconds elapsed since last port map update. Updating port map for device at %s.", s.DeviceAddress)

		// Attempt to fetch the updated port map
		updatedPortMap, err := s.GetUsedPortsAndMode(ctx)
		if err != nil {
			s.logger.Errorf("Failed to update port map for device at %s: %v", s.DeviceAddress, err)
			// Proceed with old port map
		} else {
			s.CurrentPortMap = updatedPortMap
			s.lastPortMapTime = time.Now()

			s.logger.Infof("Updated Port Map for device at %s:", s.DeviceAddress)
			for port, info := range updatedPortMap {
				s.logger.Infof(
					"Port %d:\n"+
						"  Mode        : %d\n"+
						"  Connected   : %t\n"+
						"  DeviceID    : %d\n"+
						"  VendorID    : %d\n"+
						"  ProductName : %s\n"+
						"  Serial      : %s\n",
					port,
					info.Mode,
					info.Connected,
					info.DeviceID,
					info.VendorID,
					info.ProductName,
					info.Serial,
				)
			}
		}
	}
	s.mu.Unlock()

	// Read sensor data
	sensorData, err := s.GetSensorDataMap(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Create a message batch
	msgBatch, err := s.ProcessSensorData(ctx, s.CurrentPortMap, sensorData)
	if err != nil {
		return nil, nil, err
	}

	return msgBatch, func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

// Close cleans up resources
func (s *SensorConnectInput) Close(ctx context.Context) error {
	return nil
}

// Register the plugin
func init() {
	err := service.RegisterBatchInput(
		"sensorconnect", ConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return NewSensorConnectInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}
