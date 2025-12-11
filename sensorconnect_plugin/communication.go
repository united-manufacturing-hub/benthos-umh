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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const CONNECTION_TIMEOUT_SECONDS = 10

// SendRequestToDevice sends a request to the device and checks the response CID
func (s *SensorConnectInput) SendRequestToDevice(ctx context.Context, requestData map[string]interface{}) (map[string]interface{}, error) {
	cid := s.getNextCid() // Use the safe increment method

	requestData["cid"] = cid

	payloadBytes, err := json.Marshal(requestData)
	if err != nil {
		s.logger.Errorf("Failed to marshal payload: %v", err)
		return nil, err
	}

	url := fmt.Sprintf("http://%s", s.DeviceAddress)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		s.logger.Warnf("Failed to create request for %s: %v", url, err)
		return nil, err
	}

	client := &http.Client{
		Timeout: time.Duration(CONNECTION_TIMEOUT_SECONDS) * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		s.logger.Debugf("No response from %s: %v", url, err)
		return nil, err
	}
	defer resp.Body.Close()

	// Check HTTP status code
	if resp.StatusCode != http.StatusOK {
		diagnosticMessage := GetDiagnosticMessage(resp.StatusCode)
		s.logger.Errorf("Unexpected status code %d from %s: %s", resp.StatusCode, url, diagnosticMessage)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, diagnosticMessage)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.logger.Errorf("Failed to read response from %s: %v", url, err)
		return nil, err
	}

	var response map[string]interface{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		s.logger.Errorf("Failed to parse response from %s: %v", url, err)
		return nil, err
	}

	// Check CID in response
	if responseCid, ok := response["cid"].(float64); ok { // JSON numbers are float64
		if int16(responseCid) != cid {
			s.logger.Errorf("Unexpected correlation ID in response from %s: %d (expected %d)", url, int(responseCid), cid)
			return nil, fmt.Errorf("unexpected correlation ID in response")
		}
	} else {
		s.logger.Errorf("No cid in response from %s", url)
		return nil, fmt.Errorf("no cid in response")
	}

	// Check diagnostic code in response
	if responseCode, ok := response["code"].(float64); ok { // JSON numbers are float64
		if int(responseCode) != 200 {
			diagnosticMessage := GetDiagnosticMessage(resp.StatusCode)
			s.logger.Errorf("Unexpected status code %d in payload from %s: %s", resp.StatusCode, url, diagnosticMessage)
			return nil, fmt.Errorf("unexpected status code in payload %d: %s", resp.StatusCode, diagnosticMessage)
		}
	}

	return response, nil
}

// GetDiagnosticMessage returns a diagnostic message based on the code
func GetDiagnosticMessage(code int) string {
	messages := map[int]string{
		200: "OK: Request successfully processed",
		230: "OK but needs reboot: Request successfully processed; IO-Link master must be restarted",
		231: "OK but block request not finished: Request successfully processed; block wise request, but not yet finished",
		232: "Data has been accepted, but internally modified: New values have been accepted, but were adjusted by the IO-Link master (Master cycle time)",
		233: "IP settings have been updated: IP settings have been successfully changed, IO-Link master will be reloaded; wait for at least 1 second",
		400: "Bad request: Invalid request",
		401: "Unauthorized: Non-authorized request",
		403: "Forbidden: Forbidden request",
		500: "Internal Server Error: Internal fault",
		503: "Service Unavailable: The service is not available (e.g., IO-Link port in wrong operating mode; no IO-Link device at IO-Link port)",
		530: "The requested data is invalid: Invalid process data",
		531: "IO-Link error: Error in IO-Link Master/device",
		532: "PLC connected Error: Error while setting data, because IO-Link master is still connected to fieldbus PLC",
	}

	if msg, ok := messages[code]; ok {
		return msg
	}
	return "Unknown error code"
}

// getNextCid safely increments and retrieves the next CID within the allowed range
func (s *SensorConnectInput) getNextCid() int16 {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Increment CurrentCid
	s.CurrentCid++

	// Check upper bound
	// the actual allowed range is between -2 to 32,764
	if s.CurrentCid > 32760 || s.CurrentCid < 0 {
		s.CurrentCid = 1
	}

	return s.CurrentCid
}
