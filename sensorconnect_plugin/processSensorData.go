package sensorconnect_plugin

import (
	"context"
	"fmt"
	"github.com/redpanda-data/benthos/v4/public/service"
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
