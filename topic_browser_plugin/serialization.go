package topic_browser_plugin

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// hasDataToReturn checks if there is any data to include in the output message.
//
// # OUTPUT DECISION ALGORITHM
//
// This function implements the final decision logic for whether to emit a message:
//
// ## Emission Criteria:
//   - Events present: At least one message was successfully processed
//   - Topics present: At least one topic metadata change was detected
//   - Both present: Most common case (new events + topic changes)
//   - Neither present: No output (returns nil to prevent empty protobuf)
//
// ## Performance Optimization:
//   - Prevents creation of empty protobuf messages
//   - Avoids unnecessary serialization and compression overhead
//   - Reduces network traffic by eliminating no-op messages
//   - Saves downstream processing cycles in umh-core
//
// ## Edge Case Handling:
//   - All messages failed processing: len(Events) == 0, returns false
//   - No topic changes but events exist: Returns true (events-only emission)
//   - Topic changes but no events: Returns true (topics-only emission, rare)
//   - Empty input batch: Returns false early (prevents unnecessary work)
//
// This prevents creating empty messages when:
// - No events were successfully processed
// - No topic metadata has changed
//
// Args:
//   - unsBundle: The bundle to check for data content
//
// Returns:
//   - bool: true if bundle contains data worthy of emission, false if empty
func (t *TopicBrowserProcessor) hasDataToReturn(unsBundle *UnsBundle) bool {
	return len(unsBundle.Events.Entries) > 0 || len(unsBundle.UnsMap.Entries) > 0
}

// createFinalMessage converts the UNS bundle into a protobuf-encoded message.
//
// # SERIALIZATION AND COMPRESSION PIPELINE
//
// This function implements the final serialization pipeline with conditional compression:
//
// ## Protobuf Serialization:
//   - Marshals UnsBundle to binary protobuf format
//   - Protobuf chosen for efficiency and forward/backward compatibility
//   - Schema evolution supported via protobuf field numbering
//
// ## LZ4 Compression Decision:
//   - Always applied via LZ4 compression (see BundleToProtobufBytes)
//   - Compression level 0 for fastest processing (latency-optimized)
//   - Typical compression ratio: 84% for UMH data (5MB → 750KB)
//   - Skipped for small payloads to avoid LZ4 frame overhead
//
// ## Wire Format Generation:
//   - Hex-encodes the final bytes (protobuf or LZ4-compressed protobuf)
//   - Wraps in delimiter format for umh-core parsing:
//     STARTSTARTSTART\n<hex-data>\nENDDATAENDDATENDDATA\n<timestamp>\nENDENDENDEND
//   - Includes current timestamp for latency analysis
//
// ## Error Handling:
//   - Protobuf marshaling errors: Returns error (prevents data corruption)
//   - LZ4 compression errors: Returns error (prevents data corruption)
//   - No partial emission on failure (atomic success/failure)
//
// ## Performance Characteristics:
//   - Small bundles (<1KB): ~450ns processing, no compression
//   - Large bundles (~94KB): ~506µs processing, 84.8% compression
//   - Very large bundles (~5MB): ~14.8ms processing, 84.6% compression
//
// This function:
// - Compresses the bundle to minimize traffic (when beneficial)
// - Creates a new message with the encoded data
// - Returns the message in the format expected by the Benthos framework
//
// Args:
//   - unsBundle: The bundle to serialize and potentially compress
//
// Returns:
//   - []service.MessageBatch: Single batch with one message containing encoded data
//   - error: Any serialization or compression error
func (t *TopicBrowserProcessor) createFinalMessage(unsBundle *UnsBundle) ([]service.MessageBatch, error) {
	protoBytes, err := BundleToProtobufBytes(unsBundle)
	if err != nil {
		return nil, err
	}

	message := service.NewMessage(nil)
	message.SetBytes(bytesToMessageWithStartEndBlocksAndTimestamp(protoBytes))

	return []service.MessageBatch{{message}}, nil
}
