package sensorconnect_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
)

// ConnectedDeviceInfo represents information about a connected device on a port
type ConnectedDeviceInfo struct {
	Mode        uint
	Connected   bool
	DeviceID    uint
	VendorID    uint
	ProductName string
	Serial      string
	UseRawData  bool // If true, the raw data will be used instead of the parsed data. This flag is automatically set if there is no IODD file available
}

// GetUsedPortsAndMode returns a map of the IO-Link Master's ports with port numbers as keys and ConnectedDeviceInfo as values
func (s *SensorConnectInput) GetUsedPortsAndMode(ctx context.Context) (map[int]ConnectedDeviceInfo, error) {
	response, err := s.getUsedPortsAndMode(ctx)
	if err != nil {
		return nil, err
	}

	// Group the response data by port
	portData := make(map[int]map[string]UPAMDatum)
	for key, value := range response.Data {
		port, err := extractIntFromString(key)
		if err != nil {
			s.logger.Warnf("Failed to extract port number from key %s: %v", key, err)
			continue
		}

		if _, exists := portData[port]; !exists {
			portData[port] = make(map[string]UPAMDatum)
		}
		portData[port][key] = value
	}

	portModeUsageMap := make(map[int]ConnectedDeviceInfo)

	// Process each port's data
	for port, data := range portData {
		deviceInfo := ConnectedDeviceInfo{}
		deviceInfo.Connected = true // Default to connected unless proven otherwise

		// Process the mode first
		modeKey := fmt.Sprintf("/iolinkmaster/port[%d]/mode", port)
		modeValue, modeExists := data[modeKey]
		if !modeExists {
			s.logger.Warnf("Mode not found for port %d", port)
			continue
		}

		if modeValue.Code == 200 && modeValue.Data != nil {
			mode, err := parseUint(modeValue.Data)
			if err != nil {
				s.logger.Errorf("Failed to parse mode for port %d: %v", port, err)
				return nil, err
			}
			deviceInfo.Mode = uint(mode)
			s.logger.Debugf("Port %d: Mode set to %d", port, mode)
		} else {
			s.logger.Warnf("Failed to get mode for port %d: %v", port, modeValue)
			if modeValue.Code != 200 {
				diagnosticMessage := GetDiagnosticMessage(modeValue.Code)
				s.logger.Warnf("Response Code: %s", diagnosticMessage)
			}
			continue // Cannot proceed without mode
		}

		// Process other keys based on port mode
		if deviceInfo.Mode == 3 { // IO-Link mode

			// Process device ID
			deviceIDKey := fmt.Sprintf("/iolinkmaster/port[%d]/iolinkdevice/deviceid", port)
			deviceIDValue, exists := data[deviceIDKey]
			if !exists || deviceIDValue.Code != 200 || deviceIDValue.Data == nil {
				if deviceIDValue.Code == 503 {
					s.logger.Debugf("DeviceID not available for port %d (code 503), marking as disconnected", port)
					deviceInfo.Connected = false
					portModeUsageMap[port] = deviceInfo
					continue
				}

				s.logger.Warnf("Failed to get deviceID for port %d: %v", port, deviceIDValue)
				deviceInfo.Connected = false
				portModeUsageMap[port] = deviceInfo
				continue // Cannot proceed without device ID
			}

			deviceID, err := parseUint(deviceIDValue.Data)
			if err != nil {
				s.logger.Errorf("Failed to parse deviceID for port %d: %v", port, err)
				deviceInfo.Connected = false
				portModeUsageMap[port] = deviceInfo
				continue // Cannot proceed without valid device ID
			}
			deviceInfo.DeviceID = uint(deviceID)
			s.logger.Debugf("Port %d: DeviceID set to %d", port, deviceID)

			// Process vendor ID
			vendorIDKey := fmt.Sprintf("/iolinkmaster/port[%d]/iolinkdevice/vendorid", port)
			vendorIDValue, exists := data[vendorIDKey]
			if !exists || vendorIDValue.Code != 200 || vendorIDValue.Data == nil {
				if vendorIDValue.Code == 503 {
					s.logger.Debugf("VendorID not available for port %d (code 503), marking as disconnected", port)
					deviceInfo.Connected = false
					portModeUsageMap[port] = deviceInfo
					continue
				}

				s.logger.Warnf("Failed to get vendorID for port %d: %v", port, vendorIDValue)
				deviceInfo.Connected = false
				portModeUsageMap[port] = deviceInfo
				continue // Cannot proceed without vendor ID
			}

			vendorID, err := parseUint(vendorIDValue.Data)
			if err != nil {
				s.logger.Errorf("Failed to parse vendorID for port %d: %v", port, err)
				deviceInfo.Connected = false
				portModeUsageMap[port] = deviceInfo
				continue // Cannot proceed without valid vendor ID
			}
			deviceInfo.VendorID = uint(vendorID)
			s.logger.Debugf("Port %d: VendorID set to %d", port, vendorID)

			// Process product name
			productNameKey := fmt.Sprintf("/iolinkmaster/port[%d]/iolinkdevice/productname", port)
			productNameValue, exists := data[productNameKey]
			if !exists || productNameValue.Data == nil || productNameValue.Code == 503 {
				s.logger.Infof("ProductName not available for port %d", port)
			} else if productNameValue.Code == 200 {
				productName, err := parseString(productNameValue.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse productName for port %d: %v", port, err)
				} else {
					deviceInfo.ProductName = productName
					s.logger.Debugf("Port %d: ProductName set to %s", port, productName)
				}
			} else {
				s.logger.Warnf("Unexpected code for productName on port %d: %d", port, productNameValue.Code)
			}

			// Process serial number
			serialKey := fmt.Sprintf("/iolinkmaster/port[%d]/iolinkdevice/serial", port)
			serialValue, exists := data[serialKey]
			if !exists || serialValue.Data == nil || serialValue.Code == 503 {
				s.logger.Infof("Serial number not available for port %d", port)
			} else if serialValue.Code == 200 {
				serial, err := parseString(serialValue.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse serial number for port %d: %v", port, err)
				} else {
					deviceInfo.Serial = serial
					s.logger.Debugf("Port %d: Serial number set to %s", port, serial)
				}
			} else {
				s.logger.Warnf("Unexpected code for serial number on port %d: %d", port, serialValue.Code)
			}

		} else {
			// For non-IO-Link modes, no product information is expected
			s.logger.Debugf("Port %d is in mode %d, skipping product info", port, deviceInfo.Mode)
		}

		// Add the deviceInfo to the map
		portModeUsageMap[port] = deviceInfo
	}

	// Check and fetch IODD files for IO-Link devices if necessary
	for port, info := range portModeUsageMap {
		if info.Mode == 3 && info.Connected && info.DeviceID != 0 && info.VendorID != 0 {
			s.logger.Debugf("IO-Link device found on port %d, checking IODD files for Device ID: %d, Vendor ID: %d", port, info.DeviceID, info.VendorID)
			ioddFilemapKey := IoddFilemapKey{
				DeviceId: int(info.DeviceID),
				VendorId: int64(info.VendorID),
			}
			err := s.AddNewDeviceToIoddFilesAndMap(ctx, ioddFilemapKey)
			if err != nil || s.UseOnlyRawData { // If there is an error or the plugin is set to use only raw data, set the port to use raw data
				s.logger.Warnf("Failed to find iodd file: %v", err)
				s.logger.Warnf("Setting port %d to use raw data", port)
				deviceInfo := portModeUsageMap[port]
				deviceInfo.UseRawData = true
				portModeUsageMap[port] = deviceInfo
				continue
			}
		}
	}

	return portModeUsageMap, nil
}

// getUsedPortsAndMode sends a request to the device to get information about used ports and their modes
func (s *SensorConnectInput) getUsedPortsAndMode(ctx context.Context) (RawUsedPortsAndMode, error) {
	// Define the data points to request for each port
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

	requestData := map[string]interface{}{
		"code": "request",
		"adr":  "/getdatamulti",
		"data": map[string]interface{}{
			"datatosend": dataPoints,
		},
	}

	response, err := s.SendRequestToDevice(ctx, requestData)
	if err != nil {
		return RawUsedPortsAndMode{}, err
	}

	// Parse the response into RawUsedPortsAndMode struct
	var rawData RawUsedPortsAndMode
	responseBytes, err := json.Marshal(response)
	if err != nil {
		s.logger.Errorf("Failed to marshal response: %v", err)
		return RawUsedPortsAndMode{}, err
	}
	err = json.Unmarshal(responseBytes, &rawData)
	if err != nil {
		s.logger.Errorf("Failed to parse response: %v", err)
		return RawUsedPortsAndMode{}, err
	}

	return rawData, nil
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

// extractIntFromString extracts exactly one integer from a given string.
// If no integer or more than one integer is found, it returns an error.
func extractIntFromString(input string) (int, error) {
	re := regexp.MustCompile("[0-9]+")
	outputSlice := re.FindAllString(input, -1)
	if len(outputSlice) != 1 {
		err := fmt.Errorf("not exactly one integer found in string: %s", input)
		return -1, err
	}
	outputNumber, err := strconv.Atoi(outputSlice[0])
	if err != nil {
		err := fmt.Errorf("failed to convert string to integer: %s", outputSlice[0])
		return -1, err
	}
	return outputNumber, nil
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
