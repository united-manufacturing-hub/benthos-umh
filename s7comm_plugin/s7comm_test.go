package s7comm_plugin

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Define a structure for test cases
type testCase struct {
	address            string
	inputBytesHex      string
	expectedConversion interface{}
}

// TestParseAddresses refactored to include address, bytes, and expected output
func TestParseAddresses(t *testing.T) {
	// Define your test cases here
	tests := []testCase{
		{"DB2.W0", "0000", uint16(0)},
		{"DB2.W1", "0001", uint16(1)},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("Address %s with bytes %s", tc.address, tc.inputBytesHex), func(t *testing.T) {
			addresses := []string{tc.address}
			batchMaxSize := 1

			batches, err := parseAddresses(addresses, batchMaxSize)
			if err != nil {
				t.Errorf("parseAddresses returned an error: %v", err)
				return
			}

			if len(batches) != 1 || len(batches[0]) != 1 {
				t.Fatalf("Expected 1 batch with 1 address, got %d batches with %d addresses", len(batches), len(batches[0]))
			}

			converterFunc := batches[0][0].ConverterFunc

			// Convert inputBytesHex to bytes
			inputBytes, err := hex.DecodeString(tc.inputBytesHex)
			if err != nil {
				t.Fatalf("Failed to decode input bytes hex: %v", err)
			}

			// Execute the converter function with the input bytes
			actualConversionResult := converterFunc(inputBytes)

			// Assert the expected conversion result without checking the type without reflection
			// as the type of the expected conversion result is not known

			if actualConversionResult != tc.expectedConversion {
				t.Errorf("Expected conversion result %v, got %v", tc.expectedConversion, actualConversionResult)
			}
		})
	}
}

func TestAgainstRemoteInstance(t *testing.T) {

	// These information can be found in Bitwarden under Siemens S7-1200
	endpoint := os.Getenv("TEST_S7_TCPDEVICE")

	rackStr := os.Getenv("TEST_S7_RACK")
	slotStr := os.Getenv("TEST_S7_SLOT")

	// Check if environment variables are set
	if endpoint == "" || rackStr == "" || slotStr == "" {
		t.Skip("Skipping test: environment variables not set")
		return
	}

	rack, err := strconv.Atoi(rackStr)
	if err != nil {
		t.Errorf("Failed to convert rack to integer: %v", err)
		return
	}

	slot, err := strconv.Atoi(slotStr)
	if err != nil {
		t.Errorf("Failed to convert slot to integer: %v", err)
		return
	}
	const batchMaxSize = 480 // default

	t.Run("Connect", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var addresses = []string{"DB2.W0"}

		batches, err := parseAddresses(addresses, batchMaxSize)
		if err != nil {
			t.Errorf("Failed to parse addresses: %v", err)
		}

		input := &S7CommInput{
			tcpDevice:    endpoint,
			rack:         rack,
			slot:         slot,
			batchMaxSize: batchMaxSize,
			batches:      batches,
		}

		// Attempt to connect
		err = input.Connect(ctx)
		defer input.Close(ctx)
		assert.NoError(t, err)
	})

	t.Run("Read", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error

		var addresses = []string{"DB2.W0"}

		batches, err := parseAddresses(addresses, batchMaxSize)
		if err != nil {
			t.Errorf("Failed to parse addresses: %v", err)
		}

		input := &S7CommInput{
			tcpDevice:    endpoint,
			rack:         rack,
			slot:         slot,
			batchMaxSize: batchMaxSize,
			batches:      batches,
		}

		// Attempt to connect
		err = input.Connect(ctx)
		defer input.Close(ctx)
		assert.NoError(t, err)

		messageBatch, _, err := input.ReadBatch(ctx)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Received %d messages:", len(messageBatch))

		assert.Equal(t, 1, len(messageBatch))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			if err != nil {
				t.Fatal(message, err)
			}
			var exampleNumber json.Number = "22.565684"
			assert.IsType(t, exampleNumber, message) // it should be a number
			t.Log("Received message: ", message)
		}
	})
}
