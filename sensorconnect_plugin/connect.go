package sensorconnect_plugin

import (
	"context"
	"fmt"
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
	ProductCode  string
	SerialNumber string
	URL          string
}

// GetDeviceInformation connects to the device and retrieves its information
func (s *SensorConnectInput) GetDeviceInformation(ctx context.Context) (DeviceInformation, error) {
	requestData := map[string]interface{}{
		"code": "request",
		"adr":  "/getdatamulti",
		"data": map[string]interface{}{
			"datatosend": []string{
				"/deviceinfo/serialnumber/", "/deviceinfo/productcode/",
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

	if productCodeDP.Code != 200 {
		s.logger.Errorf("Error extracting product code, responded with code %v: %v", GetDiagnosticMessage(productCodeDP.Code), productCodeDP.Data)
		return DeviceInformation{}, fmt.Errorf("error extracting product code, responded with code %v: %v", GetDiagnosticMessage(productCodeDP.Code), productCodeDP.Data)
	}

	if serialNumberDP.Code != 200 {
		s.logger.Errorf("Error extracting serial number, responded with code %v: %v", GetDiagnosticMessage(serialNumberDP.Code), serialNumberDP.Data)
		return DeviceInformation{}, fmt.Errorf("error extracting serial number, responded with code %v: %v", GetDiagnosticMessage(serialNumberDP.Code), serialNumberDP.Data)
	}

	deviceInfo := DeviceInformation{
		ProductCode:  productCodeDP.Data,
		SerialNumber: serialNumberDP.Data,
		URL:          fmt.Sprintf("http://%s", s.DeviceAddress),
	}
	s.logger.Infof("Found device at %s (SN: %s, PN: %s)", deviceInfo.URL, deviceInfo.SerialNumber, deviceInfo.ProductCode)

	return deviceInfo, nil
}
