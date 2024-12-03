package sensorconnect_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redpanda-data/benthos/v4/public/service"
	"math"
	"math/big"
	"reflect"
	"strconv"
)

// ProcessSensorData processes the downloaded information from one IO-Link master
// and returns a message batch with one message per sensor (active port).
func (s *SensorConnectInput) ProcessSensorData(ctx context.Context, connectedDevices []ConnectedDeviceInfo, sensorDataMap map[string]interface{}) (service.MessageBatch, error) {
	// Initialize an empty MessageBatch to collect results from all ports
	var batch service.MessageBatch

	// Loop over the ports
	for _, device := range connectedDevices {
		if !device.Connected {
			s.logger.Debugf("%s is not connected, skipping.", device.Uri)
			continue
		}

		// Process based on port mode
		switch device.Mode {
		case 1: // Digital Input
			s.logger.Debugf("Processing sensor data for port %v, mode %v", device.Uri, device.Mode)
			key := device.Uri + "/pin2in"
			dataPin2In, err := s.extractByteArrayFromSensorDataMap(key, "data", sensorDataMap)
			if err != nil {
				s.logger.Debugf("Error extracting dataPin2In from sensorDataMap: %v for key %s", err, key)
				continue
			}

			// Create message
			message := service.NewMessage(dataPin2In)

			message.MetaSet("sensorconnect_port_mode", "digital-input")
			message.MetaSet("sensorconnect_port_number", device.Port)

			message.MetaSet("sensorconnect_device_url", s.DeviceInfo.URL)
			message.MetaSet("sensorconnect_device_product_code", s.DeviceInfo.ProductCode)
			message.MetaSet("sensorconnect_device_serial_number", s.DeviceInfo.SerialNumber)
			message.MetaSet("sensorconnect_bluetooth_meshadapter", device.BtAdapter)

			// Add message to batch
			batch = append(batch, message)

		case 3: // IO-Link
			s.logger.Debugf("Processing IO-Link data for port %v, mode %v", device.Uri, device.Mode)
			// Get value from sensorDataMap
			keyPdin := device.Uri + "/iolinkdevice/pdin"
			connectionCode, err := s.extractIntFromSensorDataMap(keyPdin, "code", sensorDataMap)
			if err != nil {
				s.logger.Warnf("Failed to extract connection code %s: %v", device.Uri, err)
				continue
			}

			if connectionCode != 200 {
				s.logger.Debugf("%s is not connected", device.Uri)
				continue
			}

			rawSensorOutput, err := s.extractByteArrayFromSensorDataMap(keyPdin, "data", sensorDataMap)
			if err != nil {
				s.logger.Errorf("Failed to extract byte array from sensorDataMap: %v", err)
				continue
			}

			// create IoddFilemapKey
			var ioddFilemapKey IoddFilemapKey
			ioddFilemapKey.DeviceId = int(device.DeviceID)
			ioddFilemapKey.VendorId = int64(device.VendorID)
			var payload map[string]interface{}

			if !device.UseRawData {
				payload, err = s.GetProcessedSensorDataFromRawSensorOutput(string(rawSensorOutput), device)
			}

			if err != nil {
				s.logger.Errorf("Failed to process sensor data: %v", err)
			}

			if err != nil || device.UseRawData { // if above did not work or UseRawData is set
				payload = make(map[string]interface{})
				payload["raw_sensor_output"] = rawSensorOutput
			}

			// Create message
			b := make([]byte, 0)
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				s.logger.Errorf("Error marshaling to JSON: %v", err)
				return nil, err
			}
			b = append(b, jsonBytes...)

			message := service.NewMessage(b)

			message.MetaSet("sensorconnect_port_mode", "io-link")
			message.MetaSet("sensorconnect_port_number", device.Port)
			message.MetaSet("sensorconnect_port_iolink_vendor_id", strconv.Itoa(int(device.VendorID)))
			message.MetaSet("sensorconnect_port_iolink_device_id", strconv.Itoa(int(device.DeviceID)))
			message.MetaSet("sensorconnect_port_iolink_product_name", device.ProductName)
			message.MetaSet("sensorconnect_port_iolink_serial", device.Serial)

			message.MetaSet("sensorconnect_device_url", s.DeviceInfo.URL)
			message.MetaSet("sensorconnect_device_product_code", s.DeviceInfo.ProductCode)
			message.MetaSet("sensorconnect_device_serial_number", s.DeviceInfo.SerialNumber)

			message.MetaSet("sensorconnect_bluetooth_meshadapter", device.BtAdapter)

			// Add message to batch
			batch = append(batch, message)
		default:
			s.logger.Warnf("Unsupported port mode %v on port %v", device.Mode, device.Port)
			continue
		}
	}

	return batch, nil
}

// Helper functions

func (s *SensorConnectInput) GetProcessedSensorDataFromRawSensorOutput(rawSensorOutput string, portMode ConnectedDeviceInfo) (map[string]interface{}, error) {
	// create IoddFilemapKey
	var ioddFilemapKey IoddFilemapKey
	ioddFilemapKey.DeviceId = int(portMode.DeviceID)
	ioddFilemapKey.VendorId = int64(portMode.VendorID)

	// check if entry for IoddFilemapKey exists in ioddIoDeviceMap
	ioddFile, ok := s.IoDeviceMap.Load(ioddFilemapKey)
	if !ok {
		s.logger.Errorf("IoddFilemapKey %v not in IodddeviceMap", ioddFilemapKey)
		return nil, fmt.Errorf("IoddFilemapKey %v not in IodddeviceMap", ioddFilemapKey)
	}

	cidm, ok := ioddFile.(IoDevice)
	if !ok {
		s.logger.Errorf("Failed to cast idm to IoDevice")
		return nil, fmt.Errorf("failed to cast idm to IoDevice")
	}

	// create padded binary raw sensor output

	rawSensorOutputLength := len(rawSensorOutput)

	outputBitLength := rawSensorOutputLength * 4
	rawSensorOutputString := string(rawSensorOutput)
	rawSensorOutputBinary, err := s.HexToBin(rawSensorOutputString)
	if err != nil {
		return nil, err
	}
	rawSensorOutputBinaryPadded := s.ZeroPadding(rawSensorOutputBinary, outputBitLength)

	// Extract important IoddStruct parts for better readability
	processDataIn := cidm.ProfileBody.DeviceFunction.ProcessDataCollection.ProcessData.ProcessDataIn
	datatypeReferenceArray := cidm.ProfileBody.DeviceFunction.DatatypeCollection.DatatypeArray
	var emptySimpleDatatype SimpleDatatype
	primLangExternalTextCollection := cidm.ExternalTextCollection.PrimaryLanguage.Text

	// Process the data
	payload, err := s.processData(
		processDataIn.Datatype,
		processDataIn.DatatypeRef,
		emptySimpleDatatype,
		0,
		outputBitLength,
		rawSensorOutputBinaryPadded,
		datatypeReferenceArray,
		processDataIn.Name.TextId,
		primLangExternalTextCollection)
	if err != nil {
		s.logger.Errorf("Failed to process data: %v", err)
		return nil, err
	}
	return payload, nil
}

func (s *SensorConnectInput) extractByteArrayFromSensorDataMap(key string, tag string, sensorDataMap map[string]interface{}) ([]byte, error) {
	element, ok := sensorDataMap[key]
	if !ok {
		return nil, fmt.Errorf("key %s not in sensorDataMap", key)
	}
	elementMap, ok := element.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("element %v is not a map", element)
	}
	dataValue, ok := elementMap[tag]
	if !ok {
		return nil, fmt.Errorf("tag %s not found in element map", tag)
	}
	returnValue := fmt.Sprintf("%v", dataValue)
	return []byte(returnValue), nil
}

func (s *SensorConnectInput) extractIntFromSensorDataMap(key string, tag string, sensorDataMap map[string]interface{}) (int, error) {
	element, ok := sensorDataMap[key]
	if !ok {
		return 0, fmt.Errorf("key %s not in sensorDataMap", key)
	}
	elementMap, ok := element.(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("element %v is not a map", element)
	}
	val, ok := elementMap[tag].(float64)
	if !ok {
		return 0, fmt.Errorf("failed to cast elementMap[%s] for key %s to float64", tag, key)
	}
	return int(val), nil
}

// ConvertBinaryValue converts a binary string to its corresponding value based on the datatype.
// It handles both string and numeric data types and logs errors using the Benthos logger.
func (s *SensorConnectInput) ConvertBinaryValue(binaryValue string, datatype string) (interface{}, error) {

	if binaryValue == "" {
		s.logger.Errorf("binaryValue is empty for datatype %s", datatype)
		return nil, fmt.Errorf("binaryValue is empty")
	}

	bitLen := len(binaryValue)

	if datatype == "OctetStringT" {
		output, err := s.BinToHex(binaryValue)
		if err != nil {
			return nil, err
		}
		return output, nil
	}

	raw, err := strconv.ParseUint(binaryValue, 2, bitLen)
	if err != nil {
		s.logger.Errorf("Error while converting binary value to %v: %v", datatype, err)
		return nil, fmt.Errorf("failed to parse binary value: %w", err)
	}

	var output interface{}

	switch datatype {
	case "UIntegerT":
		output = raw
	case "IntegerT":
		switch bitLen {
		case 8:
			output = int(int8(raw))
		case 16:
			output = int(int16(raw))
		case 32:
			output = int(int32(raw))
		case 64:
			output = int(int64(raw))
		default:
			s.logger.Debugf("Unsupported bit length for IntegerT: %d", bitLen)
			output = int(raw) // still need to convert it to int, the ifm KQ6005 has for example 12 bit as IntegerT lol
		}
	case "Float32T":
		if bitLen != 32 {
			s.logger.Errorf("Invalid binary length for Float32T: expected 32 bits, got %d bits", bitLen)
			return nil, fmt.Errorf("invalid binary length for Float32T")
		}
		raw, err = strconv.ParseUint(binaryValue, 2, 32)
		if err != nil {
			s.logger.Errorf("Error while parsing binary value for Float32T: %v", err)
			return nil, fmt.Errorf("failed to parse binary value for Float32T: %w", err)
		}
		output = math.Float32frombits(uint32(raw))
	case "BooleanT":
		output = raw == 1
	default:
		s.logger.Warnf("Datatype %s not supported", datatype)
		output, err = s.BinToHex(binaryValue)
		if err != nil {
			return nil, err
		}
	}

	return output, nil
}

// BinToHex converts a binary string to its hexadecimal representation.
// It preserves leading zeros based on the length of the input binary string.
// Returns an error if the binary string is invalid.
func (s *SensorConnectInput) BinToHex(bin string) (string, error) {
	// Validate that the binary string contains only '0' and '1'
	for _, char := range bin {
		if char != '0' && char != '1' {
			s.logger.Errorf("Invalid character '%c' in binary string: %s", char, bin)
			return "", fmt.Errorf("invalid character '%c' in binary string", char)
		}
	}

	// Convert binary string to big.Int
	i := new(big.Int)
	_, ok := i.SetString(bin, 2)
	if !ok {
		s.logger.Errorf("Failed to parse binary string: %s", bin)
		return "", fmt.Errorf("invalid binary string: %s", bin)
	}

	// Calculate the number of hexadecimal digits required
	// Each hex digit represents 4 bits
	hexDigits := (len(bin) + 3) / 4 // Ceiling division to account for any remaining bits

	// Convert to hexadecimal string with leading zeros
	hex := fmt.Sprintf("%0*x", hexDigits, i)

	return hex, nil
}

// HexToBin converts a hexadecimal string to its binary representation.
// It preserves leading zeros based on the length of the input hexadecimal string.
// Returns an error if the hexadecimal string is invalid.
func (s *SensorConnectInput) HexToBin(hex string) (string, error) {
	// Convert hex string to big.Int
	i := new(big.Int)
	_, ok := i.SetString(hex, 16)
	if !ok {
		s.logger.Errorf("Failed to parse hexadecimal string: %s", hex)
		return "", fmt.Errorf("invalid hexadecimal string: %s", hex)
	}

	// Calculate the number of bits required
	// Each hex digit represents 4 bits
	bitLength := len(hex) * 4

	// Convert to binary string with leading zeros
	bin := fmt.Sprintf("%0*b", bitLength, i)

	return bin, nil
}

// ZeroPadding pads the input string with leading zeros until it reaches the desired length.
func (s *SensorConnectInput) ZeroPadding(input string, desiredLength int) string {
	if len(input) >= desiredLength {
		return input
	}
	padding := make([]byte, desiredLength-len(input))
	for i := range padding {
		padding[i] = '0'
	}
	return string(padding) + input
}

// processData turns raw sensor data into human-readable data.
// It handles the input of Datatype, DatatypeRef, and SimpleDatatype structures.
// It determines which one is provided and delegates processing accordingly.
func (s *SensorConnectInput) processData(
	datatype Datatype,
	datatypeRef DatatypeRef,
	simpleDatatype SimpleDatatype,
	bitOffset int,
	outputBitLength int,
	rawSensorOutputBinaryPadded string,
	datatypeReferenceArray []Datatype,
	nameTextId string,
	primLangExternalTextCollection []Text) (map[string]interface{}, error) {

	var payload map[string]interface{}
	var err error

	if !s.isEmpty(simpleDatatype) {
		payload, err = s.ProcessSimpleDatatype(
			simpleDatatype,
			outputBitLength,
			rawSensorOutputBinaryPadded,
			bitOffset,
			nameTextId,
			primLangExternalTextCollection)
		if err != nil {
			s.logger.Errorf("Error with processSimpleDatatype: %v", err)
			return nil, err
		}
		return payload, nil
	} else if !s.isEmpty(datatype) {
		payload, err = s.processDatatype(
			datatype,
			outputBitLength,
			rawSensorOutputBinaryPadded,
			bitOffset,
			datatypeReferenceArray,
			nameTextId,
			primLangExternalTextCollection)
		if err != nil {
			s.logger.Errorf("Error with processDatatype: %v", err)
			return nil, err
		}
		return payload, nil
	} else if !s.isEmpty(datatypeRef) {
		datatype, err = s.getDatatypeFromDatatypeRef(datatypeRef, datatypeReferenceArray)
		if err != nil {
			s.logger.Errorf("Error with getDatatypeFromDatatypeRef: %v", err)
			return nil, err
		}
		payload, err = s.processDatatype(
			datatype,
			outputBitLength,
			rawSensorOutputBinaryPadded,
			bitOffset,
			datatypeReferenceArray,
			nameTextId,
			primLangExternalTextCollection)
		if err != nil {
			s.logger.Errorf("Error with processDatatype: %v", err)
			return nil, err
		}
		return payload, nil
	} else {
		s.logger.Errorf("Missing input, neither SimpleDatatype, Datatype, nor DatatypeRef provided.")
		return nil, fmt.Errorf("missing input, neither SimpleDatatype, Datatype, nor DatatypeRef provided")
	}
}

// getDatatypeFromDatatypeRef finds the actual Datatype description in the datatypeReferenceArray using the given DatatypeRef.
func (s *SensorConnectInput) getDatatypeFromDatatypeRef(datatypeRef DatatypeRef, datatypeReferenceArray []Datatype) (Datatype, error) {
	for _, datatypeElement := range datatypeReferenceArray {
		if datatypeElement.Id == datatypeRef.DatatypeId {
			return datatypeElement, nil
		}
	}
	s.logger.Errorf("DatatypeRef.DatatypeId %s is not in DatatypeCollection of IODD file.", datatypeRef.DatatypeId)
	return Datatype{}, fmt.Errorf("did not find Datatype structure for given datatype reference id: %v", datatypeRef.DatatypeId)
}

// ProcessSimpleDatatype processes the given SimpleDatatype and returns the payload.
func (s *SensorConnectInput) ProcessSimpleDatatype(
	simpleDatatype SimpleDatatype,
	outputBitLength int,
	rawSensorOutputBinaryPadded string,
	bitOffset int,
	nameTextId string,
	primLangExternalTextCollection []Text) (map[string]interface{}, error) {

	payload := make(map[string]interface{})

	binaryValue := s.extractBinaryValueFromRawSensorOutput(
		rawSensorOutputBinaryPadded,
		simpleDatatype.Type,
		simpleDatatype.BitLength,
		simpleDatatype.FixedLength,
		outputBitLength,
		bitOffset)
	valueString, err := s.ConvertBinaryValue(binaryValue, simpleDatatype.Type)
	if err != nil {
		return nil, err
	}
	valueName := s.getNameFromExternalTextCollection(nameTextId, primLangExternalTextCollection)
	payload[valueName] = valueString
	return payload, nil
}

// extractBinaryValueFromRawSensorOutput handles the extraction of the actual raw sensor data.
func (s *SensorConnectInput) extractBinaryValueFromRawSensorOutput(
	rawSensorOutputBinaryPadded string,
	typeString string,
	bitLength uint,
	fixedLength uint,
	outputBitLength int,
	bitOffset int) string {

	valueBitLength := s.DetermineValueBitLength(typeString, bitLength, fixedLength)

	leftIndex := outputBitLength - int(valueBitLength) - bitOffset
	rightIndex := outputBitLength - bitOffset
	binaryValue := rawSensorOutputBinaryPadded[leftIndex:rightIndex]
	return binaryValue
}

// processDatatype processes a Datatype structure and returns the payload.
func (s *SensorConnectInput) processDatatype(
	datatype Datatype,
	outputBitLength int,
	rawSensorOutputBinaryPadded string,
	bitOffset int,
	datatypeReferenceArray []Datatype,
	nameTextId string,
	primLangExternalTextCollection []Text) (map[string]interface{}, error) {

	if datatype.Type == "RecordT" {
		payload, err := s.processRecordType(
			datatype.RecordItemArray,
			outputBitLength,
			rawSensorOutputBinaryPadded,
			datatypeReferenceArray,
			primLangExternalTextCollection)
		if err != nil {
			s.logger.Errorf("Error with processRecordType: %v", err)
			return nil, err
		}
		return payload, nil
	} else {
		binaryValue := s.extractBinaryValueFromRawSensorOutput(
			rawSensorOutputBinaryPadded,
			datatype.Type,
			datatype.BitLength,
			datatype.FixedLength,
			outputBitLength,
			bitOffset)
		valueString, err := s.ConvertBinaryValue(binaryValue, datatype.Type)
		if err != nil {
			return nil, err
		}
		valueName := s.getNameFromExternalTextCollection(nameTextId, primLangExternalTextCollection)
		payload := make(map[string]interface{})
		payload[valueName] = valueString
		return payload, nil
	}
}

// processRecordType iterates through the given RecordItemArray and processes each RecordItem.
func (s *SensorConnectInput) processRecordType(
	recordItemArray []RecordItem,
	outputBitLength int,
	rawSensorOutputBinaryPadded string,
	datatypeReferenceArray []Datatype,
	primLangExternalTextCollection []Text) (map[string]interface{}, error) {

	payload := make(map[string]interface{})

	for _, element := range recordItemArray {
		var datatypeEmpty Datatype
		itemPayload, err := s.processData(
			datatypeEmpty,
			element.DatatypeRef,
			element.SimpleDatatype,
			element.BitOffset,
			outputBitLength,
			rawSensorOutputBinaryPadded,
			datatypeReferenceArray,
			element.Name.TextId,
			primLangExternalTextCollection)
		if err != nil {
			s.logger.Errorf("Processing of RecordItem failed: %v", err)
			continue
		}
		// Merge itemPayload into payload
		for k, v := range itemPayload {
			payload[k] = v
		}
	}
	return payload, nil
}

// isEmpty determines if a field of a struct is empty.
func (s *SensorConnectInput) isEmpty(object interface{}) bool {
	if object == nil || object == "" || object == false {
		return true
	}

	val := reflect.ValueOf(object)
	if val.Kind() == reflect.Struct {
		empty := reflect.New(val.Type()).Elem().Interface()
		return reflect.DeepEqual(object, empty)
	}
	return false
}

// DetermineValueBitLength returns the bit length of a value based on its datatype.
func (s *SensorConnectInput) DetermineValueBitLength(datatype string, bitLength uint, fixedLength uint) uint {
	switch datatype {
	case "BooleanT":
		return 1
	case "OctetStringT":
		return fixedLength * 8
	case "Float32T":
		return 32
	case "IntegerT", "UIntegerT":
		return bitLength
	default:
		s.logger.Warnf("Unsupported datatype %s; using bitLength %d", datatype, bitLength)
		return bitLength
	}
}

// getNameFromExternalTextCollection retrieves the name corresponding to a textId from the IODD TextCollection.
func (s *SensorConnectInput) getNameFromExternalTextCollection(textId string, texts []Text) string {
	for _, element := range texts {
		if textId == element.Id {
			return element.Value
		}
	}
	return "error: translation not found"
}
