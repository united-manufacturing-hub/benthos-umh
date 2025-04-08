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
	"encoding/xml"
	"fmt"
)

type DeviceConfig struct {
	DeviceID int    `json:"device_id"`
	VendorID int    `json:"vendor_id"`
	IoddURL  string `json:"iodd_url"`
}

// FetchAndStoreIoddFromURL fetches the IODD file from the custom URL and stores it.
func (s *SensorConnectInput) FetchAndStoreIoddFromURL(ctx context.Context, device DeviceConfig) error {
	s.logger.Infof("Fetching IODD from custom URL for deviceID: %d", device.DeviceID)

	ioddData, err := s.GetUrlWithRetry(ctx, device.IoddURL)
	if err != nil {
		return fmt.Errorf("failed to download IODD from URL: %w", err)
	}

	// Unmarshal the IODD data
	var payload IoDevice
	err = xml.Unmarshal(ioddData, &payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal IODD XML for deviceID %d: %w", device.DeviceID, err)
	}

	// Create the key for IoDeviceMap
	fileMapKey := IoddFilemapKey{
		VendorId: int64(device.VendorID),
		DeviceId: device.DeviceID,
	}

	// Store the IoDevice in the map
	s.IoDeviceMap.Store(fileMapKey, payload)

	return nil
}
