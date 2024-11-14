package sensorconnect_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"
)

// ConnectedDeviceInfo represents information about a connected device on a port
type ConnectedDeviceInfo struct {
	Uri         string
	Mode        uint
	Connected   bool
	DeviceID    uint
	VendorID    uint
	ProductName string
	Serial      string
	Port        string
	UseRawData  bool // If true, the raw data will be used instead of the parsed data. This flag is automatically set if there is no IODD file available
	BtAdapter   string
}

// BadgeSize is Maximum possible size of the "/getdatamulti" request, determined for AL1352 and EIO404 Devices.
const BadgeSize = 50

// GetConnectedDevices returns a map of the IO-Link Master's ports with port numbers as keys and ConnectedDeviceInfo as values
func (s *SensorConnectInput) GetConnectedDevices(ctx context.Context) ([]ConnectedDeviceInfo, error) {
	response, err := s.getUsedPortsAndMode(ctx)
	if err != nil {
		return nil, err
	}

	// Group the response data by uri
	uris := make(map[string]map[string]UPAMDatum)
	for key, value := range response {
		uri, err := extractUri(key)
		if err != nil {
			s.logger.Warnf(err.Error())
			continue
		}

		if _, exists := uris[uri]; !exists {
			uris[uri] = make(map[string]UPAMDatum)
		}
		uris[uri][key] = value
	}

	var connectedDevices []ConnectedDeviceInfo

	//// Process each uri's data
	for uri, data := range uris {
		deviceInfo := ConnectedDeviceInfo{Uri: uri}
		deviceInfo.Connected = true // Default to connected unless proven otherwise

		btAdapter, err := extractBluetoothAdapter(uri)
		if err != nil {
			s.logger.Debugf(err.Error())
			deviceInfo.BtAdapter = "none"
		} else {
			deviceInfo.BtAdapter = btAdapter
		}

		port, err := extractPort(uri)
		if err != nil {
			s.logger.Errorf(err.Error())
		} else {
			deviceInfo.Port = port
		}

		// Process the mode first
		modeKey := uri + "/mode"
		modeValue, modeExists := data[modeKey]
		if !modeExists {
			s.logger.Warnf("Mode not found for %s", uri)
			continue
		}

		if modeValue.Code == 200 && modeValue.Data != nil {
			mode, err := parseUint(modeValue.Data)
			if err != nil {
				s.logger.Errorf("Failed to parse mode for %s: %v", uri, err)
				return nil, err
			}
			deviceInfo.Mode = uint(mode)
			s.logger.Debugf("%s: Mode set to %d", uri, mode)
		} else {
			switch {
			case modeValue.Code == 503 && s.IsDeviceBluetoothMeshCompatible():
				// Handle the 503 case when the device is Bluetooth mesh compatible
				s.logger.Debugf("%s not paired", uri)
			case modeValue.Code == 404:
				s.logger.Debugf("%s does not exist on device", uri)
			default:
				diagnosticMessage := GetDiagnosticMessage(modeValue.Code)
				s.logger.Warnf("Response Code: %s", diagnosticMessage)
			}
			continue // Cannot proceed without mode
		}

		// Process other keys based on port mode
		if deviceInfo.Mode == 3 { // IO-Link mode

			// Process device ID
			deviceIDKey := uri + "/iolinkdevice/deviceid"
			deviceIDValue, exists := data[deviceIDKey]
			if !exists || deviceIDValue.Code != 200 || deviceIDValue.Data == nil {
				if deviceIDValue.Code == 503 {
					s.logger.Debugf("DeviceID not available for %s (code 503), marking as disconnected", uri)
					deviceInfo.Connected = false
					connectedDevices = append(connectedDevices, deviceInfo)
					continue
				}

				s.logger.Warnf("Failed to get deviceID for %s: %v", uri, deviceIDValue)
				deviceInfo.Connected = false
				connectedDevices = append(connectedDevices, deviceInfo)
				continue // Cannot proceed without device ID
			}

			deviceID, err := parseUint(deviceIDValue.Data)
			if err != nil {
				s.logger.Errorf("Failed to parse deviceID for %s: %v", uri, err)
				deviceInfo.Connected = false
				connectedDevices = append(connectedDevices, deviceInfo)
				continue // Cannot proceed without valid device ID
			}
			deviceInfo.DeviceID = uint(deviceID)
			s.logger.Debugf("%s: DeviceID set to %d", uri, deviceID)

			// Process vendor ID
			vendorIDKey := uri + "/iolinkdevice/vendorid"
			vendorIDValue, exists := data[vendorIDKey]
			if !exists || vendorIDValue.Code != 200 || vendorIDValue.Data == nil {
				if vendorIDValue.Code == 503 {
					s.logger.Debugf("VendorID not available for %s (code 503), marking as disconnected", uri)
					deviceInfo.Connected = false
					connectedDevices = append(connectedDevices, deviceInfo)
					continue
				}

				s.logger.Warnf("Failed to get vendorID for port %s: %v", uri, vendorIDValue)
				deviceInfo.Connected = false
				connectedDevices = append(connectedDevices, deviceInfo)
				continue // Cannot proceed without vendor ID
			}

			vendorID, err := parseUint(vendorIDValue.Data)
			if err != nil {
				s.logger.Errorf("Failed to parse vendorID for %s: %v", uri, err)
				deviceInfo.Connected = false
				connectedDevices = append(connectedDevices, deviceInfo)
				continue // Cannot proceed without valid vendor ID
			}
			deviceInfo.VendorID = uint(vendorID)
			s.logger.Debugf("%s: VendorID set to %d", uri, vendorID)

			// Process product name
			productNameKey := uri + "/iolinkdevice/productname"
			productNameValue, exists := data[productNameKey]
			if !exists || productNameValue.Data == nil || productNameValue.Code == 503 {
				s.logger.Infof("ProductName not available for %s", uri)
			} else if productNameValue.Code == 200 {
				productName, err := parseString(productNameValue.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse productName for %s: %v", uri, err)
				} else {
					deviceInfo.ProductName = productName
					s.logger.Debugf("%s: ProductName set to %s", uri, productName)
				}
			} else {
				s.logger.Warnf("Unexpected code for productName on %s: %d", uri, productNameValue.Code)
			}

			// Process serial number
			serialKey := uri + "/iolinkdevice/serial"
			serialValue, exists := data[serialKey]
			if !exists || serialValue.Data == nil || serialValue.Code == 503 {
				s.logger.Infof("Serial number not available for port %s", uri)
			} else if serialValue.Code == 200 {
				serial, err := parseString(serialValue.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse serial number for %s: %v", uri, err)
				} else {
					deviceInfo.Serial = serial
					s.logger.Debugf("%s: Serial number set to %s", uri, serial)
				}
			} else {
				s.logger.Warnf("Unexpected code for serial number on %s: %d", uri, serialValue.Code)
			}

		} else {
			// For non-IO-Link modes, no product information is expected
			s.logger.Debugf("%s is in mode %d, skipping product info", uri, deviceInfo.Mode)
		}

		// Add the deviceInfo to the map
		connectedDevices = append(connectedDevices, deviceInfo)
	}

	// Check and fetch IODD files for IO-Link devices if necessary
	for idx := range connectedDevices {
		device := &connectedDevices[idx] // we use this approach so that we can further modify the device object by setting it to UseRawData
		if device.Mode == 3 && device.Connected && device.DeviceID != 0 && device.VendorID != 0 {
			s.logger.Debugf("IO-Link device found %s, checking IODD files for Device ID: %d, Vendor ID: %d", device.Uri, device.DeviceID, device.VendorID)
			ioddFilemapKey := IoddFilemapKey{
				DeviceId: int(device.DeviceID),
				VendorId: int64(device.VendorID),
			}
			err := s.AddNewDeviceToIoddFilesAndMap(ctx, ioddFilemapKey)
			if err != nil || s.UseOnlyRawData { // If there is an error or the plugin is set to use only raw data, set the port to use raw data
				s.logger.Warnf("Failed to find iodd file: %v", err)
				s.logger.Warnf("Setting device %s to use raw data", device.Uri)
				device.UseRawData = true
				continue
			}
		}
	}

	return connectedDevices, nil
}

// getUsedPortsAndMode sends a request to the device to get information about used ports and their modes
func (s *SensorConnectInput) getUsedPortsAndMode(ctx context.Context) (map[string]UPAMDatum, error) {
	// Define the data points to request for each port
	// We also request the states of peripheral ports on EIO404, even though these models do not have any peripheral ports
	// Devices return status code 404 for not existing peripheral ports
	var dataPoints []string
	for port := 1; port <= 8; port++ {
		dataPoints = append(dataPoints,
			fmt.Sprintf("/iolinkmaster/port[%d]/mode", port),
			fmt.Sprintf("/iolinkmaster/port[%d]/iolinkdevice/deviceid", port),
			fmt.Sprintf("/iolinkmaster/port[%d]/iolinkdevice/vendorid", port),
			fmt.Sprintf("/iolinkmaster/port[%d]/iolinkdevice/productname", port),
			fmt.Sprintf("/iolinkmaster/port[%d]/iolinkdevice/serial", port),
		)
	}

	// The EIO404 Bluetooth Mesh IoT Base Station supports up to 50 EIO344 Bluetooth Mesh IO-Link Adapters.
	// Note that each EIO344 Bluetooth Mesh IO-Link Adapter is limited to a single IO-Link port.
	// Devices will return status code 503 for unprovisioned adapters and 404 for cases where no mesh adapters are supported.
	if s.DeviceInfo.ProductCode == "EIO404" {
		for meshAdapter := 1; meshAdapter <= 50; meshAdapter++ {
			dataPoints = append(dataPoints,
				fmt.Sprintf("/meshnetwork/mesh_adapter[%d]/iolinkmaster/port[1]/mode", meshAdapter),
				fmt.Sprintf("/meshnetwork/mesh_adapter[%d]/iolinkmaster/port[1]/iolinkdevice/deviceid", meshAdapter),
				fmt.Sprintf("/meshnetwork/mesh_adapter[%d]/iolinkmaster/port[1]/iolinkdevice/vendorid", meshAdapter),
				fmt.Sprintf("/meshnetwork/mesh_adapter[%d]/iolinkmaster/port[1]/iolinkdevice/productname", meshAdapter),
				fmt.Sprintf("/meshnetwork/mesh_adapter[%d]/iolinkmaster/port[1]/iolinkdevice/serial", meshAdapter),
			)
		}
	}

	// To avoid HTTP 413 "Payload Too Large" or 507 "Insufficient Storage" errors, we need to request data in batches, splitting the payload into smaller, manageable chunks.
	result := map[string]UPAMDatum{}
	var errstrings []string

	for offset := 0; offset < len(dataPoints); offset += BadgeSize {
		limit := min(BadgeSize, len(dataPoints)-offset)
		requestData := map[string]interface{}{
			"code": "request",
			"adr":  "/getdatamulti",
			"data": map[string]interface{}{
				"datatosend": dataPoints[offset : offset+limit],
			},
		}

		response, err := s.SendRequestToDevice(ctx, requestData)
		if err != nil {
			s.logger.Errorf("Request data from device: %v", err)
			errstrings = append(errstrings, err.Error())
			continue
		}

		// Parse the response into RawUsedPortsAndMode struct
		var rawData RawUsedPortsAndMode
		responseBytes, err := json.Marshal(response)
		if err != nil {
			s.logger.Errorf("Failed to marshal response: %v", err)
			errstrings = append(errstrings, err.Error())
			continue
		}
		err = json.Unmarshal(responseBytes, &rawData)
		if err != nil {
			s.logger.Errorf("Failed to parse response: %v", err)
			errstrings = append(errstrings, err.Error())
			continue
		}

		maps.Copy(result, rawData.Data)
	}

	if len(errstrings) > 0 {
		return result, errors.New(strings.Join(errstrings, "\n"))
	}

	return result, nil
}

// RawUsedPortsAndMode represents the raw response from the device for used ports and modes
type RawUsedPortsAndMode struct {
	Data map[string]UPAMDatum `json:"data"`
	Cid  int                  `json:"cid"`
	Code int                  `json:"code"`
}

// UPAMDatum represents a data point in the RawUsedPortsAndMode response
type UPAMDatum struct {
	Data interface{} `json:"data,omitempty"`
	Code int         `json:"code"`
}

// extractUri extracts the unique URI from a data request path.
// The result is used as a unique identifier to identify a connected sensor.
func extractUri(input string) (string, error) {
	rx := regexp.MustCompile("(.*port\\[\\d*])(.*$)")
	matches := rx.FindStringSubmatch(input)

	if len(matches) > 1 {
		return matches[1], nil
	}

	return "", fmt.Errorf("cannot find uri in %s", input)
}

// extractPort extracts the physical connected port id from the sensor uri
// to append it later to message metadata
func extractPort(input string) (string, error) {
	rx := regexp.MustCompile("port\\[(\\d+)\\]")
	matches := rx.FindStringSubmatch(input)

	if len(matches) > 1 {
		return matches[1], nil
	}

	return "", fmt.Errorf("cannot find port in %s", input)
}

// extractBluetoothAdapter extracts the bluetooth adapter id from the sensor uri.
// to append it later to message metadata
func extractBluetoothAdapter(input string) (string, error) {
	rx := regexp.MustCompile("mesh_adapter\\[(\\d)\\]")
	matches := rx.FindStringSubmatch(input)

	if len(matches) > 1 {
		return matches[1], nil
	}

	return "", fmt.Errorf("cannot find bluetooth mesh adapter in %s", input)
}

// parseUint parses an interface{} into uint64, handling both float64 and string types
func parseUint(data interface{}) (uint64, error) {
	switch v := data.(type) {
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected type for uint conversion: %T", v)
	}
}

// parseString parses an interface{} into string, handling only string types
func parseString(data interface{}) (string, error) {
	switch v := data.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("unexpected type for string conversion: %T", v)
	}
}
