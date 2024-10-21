package sensorconnect_plugin

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ConnectedDeviceInfo represents information about a connected device on a port
type ConnectedDeviceInfo struct {
	Mode        uint
	Connected   bool
	DeviceID    uint
	VendorID    uint
	ProductName string
	Serial      string
}

// GetUsedPortsAndMode returns a map of the IO-Link Master's ports with port numbers as keys and ConnectedDeviceInfo as values
func (s *SensorConnectInput) GetUsedPortsAndMode() (map[int]ConnectedDeviceInfo, error) {
	response, err := s.getUsedPortsAndMode()
	if err != nil {
		return nil, err
	}

	portModeUsageMap := make(map[int]ConnectedDeviceInfo)

	for key, value := range response.Data {
		port, err := extractIntFromString(key)
		if err != nil {
			s.logger.Warnf("Failed to extract port number from key %s: %v", key, err)
			continue
		}

		if strings.Contains(key, "mode") {
			if value.Code == 200 && value.Data != nil {
				mode, err := parseUint(value.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse mode for port %d: %v", port, err)
					return nil, err
				}
				connected := mode != 0
				deviceInfo := portModeUsageMap[port]
				deviceInfo.Mode = uint(mode)
				deviceInfo.Connected = connected
				portModeUsageMap[port] = deviceInfo
				s.logger.Debugf("Port %d: Mode set to %d, Connected: %v", port, mode, connected)
			}
		} else if strings.Contains(key, "deviceid") {
			if value.Code == 200 && value.Data != nil {
				deviceID, err := parseUint(value.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse deviceID for port %d: %v", port, err)
					return nil, err
				}
				deviceInfo := portModeUsageMap[port]
				deviceInfo.DeviceID = uint(deviceID)
				portModeUsageMap[port] = deviceInfo
				s.logger.Debugf("Port %d: DeviceID set to %d", port, deviceID)
			}
		} else if strings.Contains(key, "vendorid") {
			if value.Code == 200 && value.Data != nil {
				vendorID, err := parseUint(value.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse vendorID for port %d: %v", port, err)
					return nil, err
				}
				deviceInfo := portModeUsageMap[port]
				deviceInfo.VendorID = uint(vendorID)
				portModeUsageMap[port] = deviceInfo
				s.logger.Debugf("Port %d: VendorID set to %d", port, vendorID)
			}
		} else if strings.Contains(key, "productname") {
			if value.Code == 200 && value.Data != nil {
				productName, err := parseString(value.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse productName for port %d: %v", port, err)
					return nil, err
				}
				deviceInfo := portModeUsageMap[port]
				deviceInfo.ProductName = productName
				portModeUsageMap[port] = deviceInfo
				s.logger.Debugf("Port %d: ProductName set to %s", port, productName)
			}
		} else if strings.Contains(key, "serial") {
			if value.Code == 200 && value.Data != nil {
				serial, err := parseString(value.Data)
				if err != nil {
					s.logger.Errorf("Failed to parse serial for port %d: %v", port, err)
					return nil, err
				}
				deviceInfo := portModeUsageMap[port]
				deviceInfo.Serial = serial
				portModeUsageMap[port] = deviceInfo
				s.logger.Debugf("Port %d: Serial set to %s", port, serial)
			}
		} else {
			s.logger.Errorf("Invalid data returned from IO-Link Master: %v -> %v", key, value)
		}
	}

	return portModeUsageMap, nil
}

// getUsedPortsAndMode sends a request to the device to get information about used ports and their modes
func (s *SensorConnectInput) getUsedPortsAndMode() (RawUsedPortsAndMode, error) {
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

	response, err := s.SendRequestToDevice(requestData)
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
