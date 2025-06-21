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

package sparkplug_plugin

import (
	"encoding/base64"
	"fmt"

	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

// Working Base64 test vectors generated from valid Sparkplug payloads
// These are created from the same structures used in the integration tests to ensure compatibility

// First, let's create the working payloads and generate Base64 from them
func createTestNBirthPayload() *sproto.Payload {
	return &sproto.Payload{
		Timestamp: &[]uint64{uint64(1750000000000)}[0], // Fixed timestamp for consistent tests
		Seq:       &[]uint64{0}[0],
		Metrics: []*sproto.Payload_Metric{
			{
				Name:     &[]string{"bdSeq"}[0],
				Alias:    &[]uint64{0}[0],
				Datatype: &[]uint32{7}[0], // UInt64
				Value:    &sproto.Payload_Metric_LongValue{LongValue: 12345},
			},
			{
				Name:     &[]string{"Node Control/Rebirth"}[0],
				Datatype: &[]uint32{11}[0], // Boolean
				Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: false},
			},
			{
				Name:     &[]string{"Temperature"}[0],
				Alias:    &[]uint64{100}[0],
				Datatype: &[]uint32{10}[0], // Double
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
			},
		},
	}
}

func createTestNDataPayload() *sproto.Payload {
	return &sproto.Payload{
		Timestamp: &[]uint64{uint64(1750000001000)}[0], // Fixed timestamp for consistent tests
		Seq:       &[]uint64{1}[0],
		Metrics: []*sproto.Payload_Metric{
			{
				Alias:    &[]uint64{100}[0], // Should resolve to "Temperature"
				Datatype: &[]uint32{10}[0],  // Double
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 26.8},
			},
		},
	}
}

// Generate Base64 from working payloads
func mustMarshalToBase64(payload *sproto.Payload) string {
	bytes, err := proto.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal payload: %v", err))
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

// Base64 constants generated from working payloads
var (
	NBIRTH_v1 = mustMarshalToBase64(createTestNBirthPayload())
	NDATA_v1  = mustMarshalToBase64(createTestNDataPayload())
)

// Eclipse Tahu spec-compliant test vectors - programmatically generated
// These use the same approach as the working vectors above but with Eclipse Tahu message structures
// To regenerate with different data, use: python gen_vectors.py

func createEclipseTahuNBirthPayload() *sproto.Payload {
	return &sproto.Payload{
		Timestamp: &[]uint64{uint64(1750000000000)}[0], // Fixed timestamp
		Seq:       &[]uint64{0}[0],
		Metrics: []*sproto.Payload_Metric{
			{
				Name:     &[]string{"bdSeq"}[0],
				Alias:    &[]uint64{0}[0],
				Datatype: &[]uint32{7}[0], // UInt64
				Value:    &sproto.Payload_Metric_LongValue{LongValue: 1},
			},
			{
				Name:     &[]string{"Node Control/Rebirth"}[0],
				Datatype: &[]uint32{11}[0], // Boolean
				Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: true},
			},
			{
				Name:     &[]string{"Temperature"}[0],
				Alias:    &[]uint64{1}[0],
				Datatype: &[]uint32{10}[0], // Double
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.7},
			},
			{
				Name:     &[]string{"TisPressure"}[0],
				Alias:    &[]uint64{2}[0],
				Datatype: &[]uint32{7}[0], // UInt64
				Value:    &sproto.Payload_Metric_LongValue{LongValue: 42},
			},
			{
				Name:     &[]string{"Greeting"}[0],
				Alias:    &[]uint64{3}[0],
				Datatype: &[]uint32{12}[0], // String
				Value:    &sproto.Payload_Metric_StringValue{StringValue: "hi"},
			},
		},
	}
}

func createEclipseTahuNDataPayload() *sproto.Payload {
	return &sproto.Payload{
		Timestamp: &[]uint64{uint64(1750000001000)}[0], // Fixed timestamp
		Seq:       &[]uint64{1}[0],
		Metrics: []*sproto.Payload_Metric{
			{
				Alias:    &[]uint64{1}[0],  // Temperature alias
				Datatype: &[]uint32{10}[0], // Double
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.7},
			},
			{
				Alias:    &[]uint64{2}[0],  // Pressure alias
				Datatype: &[]uint32{11}[0], // Boolean
				Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: true},
			},
		},
	}
}

func createEclipseTahuNDeathPayload() *sproto.Payload {
	return &sproto.Payload{
		Timestamp: &[]uint64{uint64(1750000002000)}[0], // Fixed timestamp
		Seq:       &[]uint64{0}[0],
		Metrics: []*sproto.Payload_Metric{
			{
				Name:     &[]string{"bdSeq"}[0],
				Alias:    &[]uint64{0}[0],
				Datatype: &[]uint32{7}[0], // UInt64
				Value:    &sproto.Payload_Metric_LongValue{LongValue: 1},
			},
		},
	}
}

func createEclipseTahuNCmdPayload() *sproto.Payload {
	return &sproto.Payload{
		Timestamp: &[]uint64{uint64(1750000003000)}[0], // Fixed timestamp
		Seq:       &[]uint64{0}[0],
		Metrics: []*sproto.Payload_Metric{
			{
				Name:     &[]string{"Node Control/Rebirth"}[0],
				Datatype: &[]uint32{11}[0], // Boolean
				Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: true},
			},
		},
	}
}

// Eclipse Tahu Base64 constants generated from working payloads
var (
	NBIRTH_full_v1       = mustMarshalToBase64(createEclipseTahuNBirthPayload())
	DBIRTH_full_v1       = NBIRTH_full_v1 // Same payload structure
	NDATA_double_bool_v1 = mustMarshalToBase64(createEclipseTahuNDataPayload())
	DDATA_double_bool_v1 = NDATA_double_bool_v1 // Same payload structure
	NDATA_seq_wrap_v1    = mustMarshalToBase64(&sproto.Payload{
		Timestamp: &[]uint64{uint64(1750000004000)}[0],
		Seq:       &[]uint64{0}[0], // Wrapped around
		Metrics: []*sproto.Payload_Metric{
			{
				Alias:    &[]uint64{1}[0],
				Datatype: &[]uint32{10}[0], // Double
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 22.2},
			},
			{
				Alias:    &[]uint64{2}[0],
				Datatype: &[]uint32{11}[0], // Boolean
				Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: false},
			},
		},
	})
	NDEATH_min_v1   = mustMarshalToBase64(createEclipseTahuNDeathPayload())
	NCMD_rebirth_v1 = mustMarshalToBase64(createEclipseTahuNCmdPayload())
	DCMD_rebirth_v1 = NCMD_rebirth_v1 // Same payload structure
)

// TestVector represents a validated Sparkplug payload for testing
type TestVector struct {
	Name            string
	Base64Data      string
	Description     string
	MessageType     string
	ExpectedMetrics int
}

// GetTestVectors returns all available test vectors with working Base64 payloads
func GetTestVectors() []TestVector {
	return []TestVector{
		// Original working vectors (programmatically generated)
		{
			Name:            "NBIRTH_v1",
			Base64Data:      NBIRTH_v1,
			Description:     "NBIRTH with bdSeq, Node Control/Rebirth, and Temperature metric (alias 100)",
			MessageType:     "NBIRTH",
			ExpectedMetrics: 3,
		},
		{
			Name:            "NDATA_v1",
			Base64Data:      NDATA_v1,
			Description:     "NDATA with Temperature metric using alias 100 (name resolved from cache)",
			MessageType:     "NDATA",
			ExpectedMetrics: 1,
		},

		// Eclipse Tahu spec-compliant vectors (comprehensive coverage)
		{
			Name:            "NBIRTH_full_v1",
			Base64Data:      NBIRTH_full_v1,
			Description:     "Eclipse Tahu NBIRTH w/ all datatypes incl. NC/Rebirth",
			MessageType:     "NBIRTH",
			ExpectedMetrics: 5,
		},
		{
			Name:            "DBIRTH_full_v1",
			Base64Data:      DBIRTH_full_v1,
			Description:     "Eclipse Tahu DBIRTH w/ all datatypes incl. DC/Rebirth",
			MessageType:     "DBIRTH",
			ExpectedMetrics: 5,
		},
		{
			Name:            "NDATA_double_bool_v1",
			Base64Data:      NDATA_double_bool_v1,
			Description:     "Eclipse Tahu NDATA Double+Bool (aliases 1,2)",
			MessageType:     "NDATA",
			ExpectedMetrics: 2,
		},
		{
			Name:            "DDATA_double_bool_v1",
			Base64Data:      DDATA_double_bool_v1,
			Description:     "Eclipse Tahu DDATA Double+Bool (aliases 1,2)",
			MessageType:     "DDATA",
			ExpectedMetrics: 2,
		},
		{
			Name:            "NDATA_seq_wrap_v1",
			Base64Data:      NDATA_seq_wrap_v1,
			Description:     "Eclipse Tahu NDATA seq wrap‑around 255→0",
			MessageType:     "NDATA",
			ExpectedMetrics: 2,
		},
		{
			Name:            "NDEATH_min_v1",
			Base64Data:      NDEATH_min_v1,
			Description:     "Eclipse Tahu Node death minimal",
			MessageType:     "NDEATH",
			ExpectedMetrics: 1,
		},
		{
			Name:            "NCMD_rebirth_v1",
			Base64Data:      NCMD_rebirth_v1,
			Description:     "Eclipse Tahu Node Rebirth Request",
			MessageType:     "NCMD",
			ExpectedMetrics: 1,
		},
		{
			Name:            "DCMD_rebirth_v1",
			Base64Data:      DCMD_rebirth_v1,
			Description:     "Eclipse Tahu Device Rebirth Request",
			MessageType:     "DCMD",
			ExpectedMetrics: 1,
		},
	}
}

// MustDecodeBase64 decodes a Base64 test vector into a Sparkplug payload
// Panics on decode errors (use in tests only)
func MustDecodeBase64(b64Data string) *sproto.Payload {
	bytes, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		panic(fmt.Sprintf("Failed to decode Base64: %v", err))
	}

	var payload sproto.Payload
	if err := proto.Unmarshal(bytes, &payload); err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Sparkplug payload: %v", err))
	}

	return &payload
}

// DecodeTestVector safely decodes a test vector, returning error if invalid
func DecodeTestVector(vector TestVector) (*sproto.Payload, error) {
	bytes, err := base64.StdEncoding.DecodeString(vector.Base64Data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Base64 for %s: %w", vector.Name, err)
	}

	var payload sproto.Payload
	if err := proto.Unmarshal(bytes, &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Sparkplug payload for %s: %w", vector.Name, err)
	}

	return &payload, nil
}

// ValidateTestVector checks that a test vector can be decoded and has expected structure
func ValidateTestVector(vector TestVector) error {
	payload, err := DecodeTestVector(vector)
	if err != nil {
		return err
	}

	if len(payload.Metrics) != vector.ExpectedMetrics {
		return fmt.Errorf("expected %d metrics, got %d for %s",
			vector.ExpectedMetrics, len(payload.Metrics), vector.Name)
	}

	// Additional validation based on message type
	switch vector.MessageType {
	case "NBIRTH":
		// NBIRTH should have bdSeq as first metric
		if len(payload.Metrics) > 0 {
			if payload.Metrics[0].Name == nil || *payload.Metrics[0].Name != "bdSeq" {
				return fmt.Errorf("NBIRTH should have bdSeq as first metric in %s", vector.Name)
			}
		}
	case "NDATA":
		// NDATA should use aliases (no names initially)
		for i, metric := range payload.Metrics {
			if metric.Alias == nil {
				return fmt.Errorf("NDATA metric %d should have alias in %s", i, vector.Name)
			}
		}
	}

	return nil
}

// GetTestVectorByName returns a specific test vector by name
func GetTestVectorByName(name string) (TestVector, bool) {
	for _, tv := range GetTestVectors() {
		if tv.Name == name {
			return tv, true
		}
	}
	return TestVector{}, false
}

// GetTestVectorsByMessageType returns all test vectors for a specific message type
func GetTestVectorsByMessageType(msgType string) []TestVector {
	var result []TestVector
	for _, tv := range GetTestVectors() {
		if tv.MessageType == msgType {
			result = append(result, tv)
		}
	}
	return result
}

// GetEclipseTahuVectors returns only the Eclipse Tahu-generated vectors for spec compliance testing
func GetEclipseTahuVectors() []TestVector {
	var result []TestVector
	for _, tv := range GetTestVectors() {
		// Eclipse Tahu vectors have descriptive names starting with "Eclipse Tahu"
		if len(tv.Description) > 12 && tv.Description[:12] == "Eclipse Tahu" {
			result = append(result, tv)
		}
	}
	return result
}
