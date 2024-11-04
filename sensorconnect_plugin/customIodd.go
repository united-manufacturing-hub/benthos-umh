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
