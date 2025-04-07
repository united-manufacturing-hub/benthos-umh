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
	"regexp"
	"strconv"
	"strings"
)

// DiscoverResponseFromDevice structures for parsing device responses
type DiscoverResponseFromDevice struct {
	Data Data `json:"data"`
	Cid  int  `json:"cid"`
}

type Data struct {
	DeviceInfoSerialnumber StringDataPoint `json:"/deviceinfo/serialnumber/"`
	DeviceInfoProductCode  StringDataPoint `json:"/deviceinfo/productcode/"`
}

type StringDataPoint struct {
	Data string `json:"data"`
	Code int    `json:"code"`
}

// DeviceInformation holds relevant information about discovered devices
type DeviceInformation struct {
	ProductCode     string
	SerialNumber    string
	URL             string
	FirmwareVersion string
	BuggedFirmware  bool // This indicates if the firmware is known to have crash issues if one does too many requests. Set to true if the firmware version is not known.
}

// GetDeviceInformation connects to the device and retrieves its information
func (s *SensorConnectInput) GetDeviceInformation(ctx context.Context) (DeviceInformation, error) {
	requestData := map[string]interface{}{
		"code": "request",
		"adr":  "/getdatamulti",
		"data": map[string]interface{}{
			"datatosend": []string{
				"/deviceinfo/serialnumber/", "/deviceinfo/productcode/", "/deviceinfo/swrevision/",
			},
		},
	}

	response, err := s.SendRequestToDevice(ctx, requestData)
	if err != nil {
		return DeviceInformation{}, err
	}

	// Extract data from response
	data, ok := response["data"].(map[string]interface{})
	if !ok {
		s.logger.Errorf("Invalid data in response")
		return DeviceInformation{}, fmt.Errorf("invalid data in response")
	}

	getStringDataPoint := func(key string) (StringDataPoint, error) {
		rawDataPoint, ok := data[key]
		if !ok {
			return StringDataPoint{}, fmt.Errorf("missing key %s in response data", key)
		}
		rawDataPointMap, ok := rawDataPoint.(map[string]interface{})
		if !ok {
			return StringDataPoint{}, fmt.Errorf("invalid data point for key %s", key)
		}
		dataStr, ok := rawDataPointMap["data"].(string)
		if !ok {
			return StringDataPoint{}, fmt.Errorf("missing data field in data point for key %s", key)
		}
		codeFloat, ok := rawDataPointMap["code"].(float64)
		if !ok {
			return StringDataPoint{}, fmt.Errorf("missing code field in data point for key %s", key)
		}
		return StringDataPoint{
			Data: dataStr,
			Code: int(codeFloat),
		}, nil
	}

	serialNumberDP, err := getStringDataPoint("/deviceinfo/serialnumber/")
	if err != nil {
		s.logger.Errorf("Error extracting serial number: %v", err)
		return DeviceInformation{}, err
	}

	productCodeDP, err := getStringDataPoint("/deviceinfo/productcode/")
	if err != nil {
		s.logger.Errorf("Error extracting product code: %v", err)
		return DeviceInformation{}, err
	}

	firmwareVersionCodeDP, err := getStringDataPoint("/deviceinfo/swrevision/")
	if err != nil {
		s.logger.Debugf("Error extracting firmware: %v", err)
		firmwareVersionCodeDP = StringDataPoint{Data: "Unknown", Code: 200}
	}

	if productCodeDP.Code != 200 {
		s.logger.Errorf("Error extracting product code, responded with code %v: %v", GetDiagnosticMessage(productCodeDP.Code), productCodeDP.Data)
		return DeviceInformation{}, fmt.Errorf("error extracting product code, responded with code %v: %v", GetDiagnosticMessage(productCodeDP.Code), productCodeDP.Data)
	}

	if serialNumberDP.Code != 200 {
		s.logger.Errorf("Error extracting serial number, responded with code %v: %v", GetDiagnosticMessage(serialNumberDP.Code), serialNumberDP.Data)
		return DeviceInformation{}, fmt.Errorf("error extracting serial number, responded with code %v: %v", GetDiagnosticMessage(serialNumberDP.Code), serialNumberDP.Data)
	}

	if firmwareVersionCodeDP.Code != 200 {
		s.logger.Errorf("Error extracting firmware version, responded with code %v: %v", GetDiagnosticMessage(firmwareVersionCodeDP.Code), firmwareVersionCodeDP.Data)
		return DeviceInformation{}, fmt.Errorf("error extracting firmware version, responded with code %v: %v", GetDiagnosticMessage(firmwareVersionCodeDP.Code), firmwareVersionCodeDP.Data)
	}

	buggedFirmware := detectBuggedFirmware(firmwareVersionCodeDP.Data)

	deviceInfo := DeviceInformation{
		ProductCode:     productCodeDP.Data,
		SerialNumber:    serialNumberDP.Data,
		URL:             fmt.Sprintf("http://%s", s.DeviceAddress),
		FirmwareVersion: firmwareVersionCodeDP.Data,
		BuggedFirmware:  buggedFirmware,
	}
	s.logger.Infof(
		"Device detected at %s [Serial Number: %s, Product Code: %s, Firmware Version: %s, Bugged Firmware: %v]",
		deviceInfo.URL,
		deviceInfo.SerialNumber,
		deviceInfo.ProductCode,
		deviceInfo.FirmwareVersion,
		deviceInfo.BuggedFirmware,
	)

	if buggedFirmware {
		s.logger.Infof(
			"You are using a firmware version for your IFM IO-Link master that may crash when handling excessive requests. " +
				"IFM has not yet released an official fix; however, an experimental firmware provided by IFM and available through UMH addresses this issue. " +
				"Please contact us for more information.",
		)
	}

	return deviceInfo, nil
}

// detectBuggedFirmware determines if the firmware is bugged based on the version
// The following firmware versions are not affected by the bug:
// - AL1x5x_cn_it_v3.4.84
// - AL1x4x_cn_it_v3.4.84
// These versions were provided by IFM to UMH in 2021 but were not officially released at that time and have not been released (as of 2024-10-23)
func detectBuggedFirmware(firmware string) bool {
	referenceVersion := []int{3, 4, 84}

	// Regular expression to extract version number after '_v'
	re := regexp.MustCompile(`_v(\d+\.\d+\.\d+)$`)
	matches := re.FindStringSubmatch(firmware)
	if len(matches) != 2 {
		// Unknown firmware format
		return true
	}

	// Split the version into major, minor, patch
	versionParts := strings.Split(matches[1], ".")
	if len(versionParts) != 3 {
		// Incorrect version format
		return true
	}

	// Convert version parts to integers
	versionNums := make([]int, 3)
	for i, part := range versionParts {
		num, err := strconv.Atoi(part)
		if err != nil {
			// Non-integer version part
			return true
		}
		versionNums[i] = num
	}

	// Compare the extracted version with the reference version
	for i := 0; i < 3; i++ {
		if versionNums[i] > referenceVersion[i] {
			// Version is greater than reference
			return false
		} else if versionNums[i] < referenceVersion[i] {
			// Version is less than reference
			return true
		}
		// If equal, continue to next part
	}

	// Version is exactly equal to reference
	return false
}
