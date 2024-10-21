package sensorconnect_plugin

import (
	"context"
	"fmt"
	"github.com/redpanda-data/benthos/v4/public/service"
	"go.uber.org/zap"
	"math"
	"math/big"
	"strconv"
)

// ProcessSensorData processes the downloaded information from one IO-Link master
// and returns a message batch with one message per sensor (active port).
func (s *SensorConnectInput) ProcessSensorData(ctx context.Context, portModeMap map[int]ConnectedDeviceInfo, sensorDataMap map[string]interface{}) (service.MessageBatch, error) {
	// Initialize an empty MessageBatch to collect results from all ports
	var batch service.MessageBatch

	// Loop over the ports
	for portNumber, portMode := range portModeMap {
		if !portMode.Connected {
			s.logger.Debugf("Port %v is not connected, skipping.", portNumber)
			continue
		}

		// Process based on port mode
		switch portMode.Mode {
		case 1: // Digital Input
			s.logger.Debugf("Processing sensor data for port %v, mode %v", portNumber, portMode.Mode)
			// Get value from sensorDataMap
			portNumberString := strconv.Itoa(portNumber)
			key := "/iolinkmaster/port[" + portNumberString + "]/pin2in"
			dataPin2In, err := s.extractByteArrayFromSensorDataMap(key, "data", sensorDataMap)
			if err != nil {
				s.logger.Debugf("Error extracting dataPin2In from sensorDataMap: %v for key %s", err, key)
				continue
			}

			// Create message
			message := service.NewMessage(dataPin2In)

			message.MetaSet("sensorconnect_port_mode", "digital-input")
			message.MetaSet("sensorconnect_port_number", portNumberString)

			// Add message to batch
			batch = append(batch, message)

		case 3: // IO-Link
			s.logger.Debugf("Processing IO-Link data for port %v, mode %v", portNumber, portMode.Mode)
			// Get value from sensorDataMap
			portNumberString := strconv.Itoa(portNumber)
			keyPdin := "/iolinkmaster/port[" + portNumberString + "]/iolinkdevice/pdin"
			connectionCode, err := s.extractIntFromSensorDataMap(keyPdin, "code", sensorDataMap)
			if err != nil {
				s.logger.Warnf("Failed to extract connection code for port %v: %v", portNumber, err)
				continue
			}

			if connectionCode != 200 {
				s.logger.Debugf("Port %d is not connected", portNumber)
				continue
			}

			rawSensorOutput, err := s.extractByteArrayFromSensorDataMap(keyPdin, "data", sensorDataMap)
			if err != nil {
				s.logger.Errorf("Failed to extract byte array from sensorDataMap: %v", err)
				continue
			}

			// create IoddFilemapKey
			var ioddFilemapKey IoddFilemapKey
			ioddFilemapKey.DeviceId = int(portMode.DeviceID)
			ioddFilemapKey.VendorId = int64(portMode.VendorID)

			// check if entry for IoddFilemapKey exists in ioddIoDeviceMap
			if _, ok := s.IoDeviceMap.Load(ioddFilemapKey); !ok {
				zap.S().Debugf("IoddFilemapKey %v not in IodddeviceMap", ioddFilemapKey)
				continue
			}

			// create padded binary raw sensor output
			/*
				rawSensorOutputLength := len(rawSensorOutput)

					outputBitLength := rawSensorOutputLength * 4
					rawSensorOutputString := string(rawSensorOutput)
					rawSensorOutputBinary := HexToBin(rawSensorOutputString)
					rawSensorOutputBinaryPadded := zeroPadding(rawSensorOutputBinary, outputBitLength)

					cidm, ok := idm.(IoDevice)
					if !ok {
						zap.S().Errorf("Failed to cast idm to IoDevice")
						continue
					}

					// Extract important IoddStruct parts for better readability
					processDataIn := cidm.ProfileBody.DeviceFunction.ProcessDataCollection.ProcessData.ProcessDataIn
					datatypeReferenceArray := cidm.ProfileBody.DeviceFunction.DatatypeCollection.DatatypeArray
					var emptySimpleDatatype SimpleDatatype
					primLangExternalTextCollection := cidm.ExternalTextCollection.PrimaryLanguage.Text

			*/
			// Create message
			message := service.NewMessage(rawSensorOutput)

			message.MetaSet("sensorconnect_port_mode", "io-link")
			message.MetaSet("sensorconnect_port_number", portNumberString)

			// Add message to batch
			batch = append(batch, message)
		default:
			s.logger.Warnf("Unsupported port mode %v on port %v", portMode.Mode, portNumber)
			continue
		}
	}

	return batch, nil
}

// Helper functions

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

// convertBinaryValue converts a binary string to its corresponding value based on the datatype.
// It handles both string and numeric data types and logs errors using the Benthos logger.
func (s *SensorConnectInput) convertBinaryValue(binaryValue string, datatype string) (interface{}, error) {
	bitLen := len(binaryValue)
	raw, err := strconv.ParseUint(binaryValue, 2, bitLen)
	if err != nil {
		s.logger.Errorf("Error while converting binary value to %v: %v", datatype, err)
		return nil, fmt.Errorf("failed to parse binary value: %w", err)
	}

	var output interface{}

	switch datatype {
	case "OctetStringT":
		output = BinToHex(binaryValue)
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
			s.logger.Errorf("Unsupported bit length for IntegerT: %d", bitLen)
			return nil, fmt.Errorf("unsupported bit length for IntegerT: %d", bitLen)
		}
	case "Float32T":
		output = math.Float32frombits(uint32(raw))
	case "BooleanT":
		output = raw == 1
	default:
		s.logger.Warnf("Datatype %s not supported", datatype)
		output = BinToHex(binaryValue)
	}

	return output, nil
}

// BinToHex converts a binary string to a hex string
func BinToHex(bin string) (hex string) {
	i := new(big.Int)
	i.SetString(bin, 2)
	hex = fmt.Sprintf("%x", i)
	return
}

// zeroPadding adds zeros on the left side of a string until the lengt of the string equals the requested length
func zeroPadding(input string, length int) (output string) {
	output = fmt.Sprintf("%0*v", length, input)
	return
}

// HexToBin converts a hex string into a binary string
func HexToBin(hex string) (bin string) {
	i := new(big.Int)
	i.SetString(hex, 16)
	bin = fmt.Sprintf("%b", i)
	return
}

// determineValueBitLength returns the bitlength of a value
func determineValueBitLength(datatype string, bitLength uint, fixedLength uint) (length uint) {
	if datatype == "BooleanT" {
		return 1
	} else if datatype == "octetStringT" {
		return fixedLength * 8
	} else {
		return bitLength
	}
}
