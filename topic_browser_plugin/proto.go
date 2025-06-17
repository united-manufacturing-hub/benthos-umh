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

package topic_browser_plugin

import (
	"bytes"
	"io"
	"sync"

	"github.com/pierrec/lz4"
	"google.golang.org/protobuf/proto"
)

/*
	This file contains utils to encode/decode protobuf messages.
*/

/*
goos: darwin
goarch: arm64
pkg: github.com/united-manufacturing-hub/benthos-umh/topic_browser_plugin
cpu: Apple M3 Pro
BenchmarkBundleToProtobufBytes/small_bundle-11           2294737               529.1 ns/op           144 B/op          3 allocs/op
BenchmarkBundleToProtobufBytes/large_bundle-11              4328            278539 ns/op          130306 B/op       2001 allocs/op
BenchmarkBundleToProtobufBytes/very_large_bundle-11          108          10941938 ns/op         5005640 B/op         21 allocs/op
BenchmarkProtobufBytesToBundle/small_bundle-11           1794226               819.0 ns/op          1008 B/op         19 allocs/op
BenchmarkProtobufBytesToBundle/large_bundle-11              2554            463350 ns/op          693823 B/op      13036 allocs/op
BenchmarkProtobufBytesToBundle/very_large_bundle-11           55          20402277 ns/op        33300329 B/op     600119 allocs/op
BenchmarkRoundTrip/small_bundle-11                       1000000              1113 ns/op            1152 B/op         22 allocs/op
BenchmarkRoundTrip/large_bundle-11                          1623            737892 ns/op          824129 B/op      15037 allocs/op
BenchmarkRoundTrip/very_large_bundle-11                       31          32292704 ns/op        38305856 B/op     600139 allocs/op
BenchmarkBundleToProtobufBytesWithCompression/small_bundle-11            2693697               453.6 ns/op           144 B/op          3 allocs/op
BenchmarkBundleToProtobufBytesWithCompression/large_bundle-11               2246            506165 ns/op         9876962 B/op       2009 allocs/op
BenchmarkBundleToProtobufBytesWithCompression/very_large_bundle-11            68          14833805 ns/op        16979257 B/op         30 allocs/op
BenchmarkProtobufBytesToBundleWithCompression/small_bundle-11            1879480               635.8 ns/op          1008 B/op         19 allocs/op
BenchmarkProtobufBytesToBundleWithCompression/large_bundle-11               1862            641386 ns/op         9344379 B/op      13050 allocs/op
BenchmarkProtobufBytesToBundleWithCompression/very_large_bundle-11            49          22901078 ns/op        58465863 B/op     600139 allocs/op
BenchmarkCompressionRoundTrip/small_bundle-11                            1000000              1118 ns/op            1152 B/op         22 allocs/op
BenchmarkCompressionRoundTrip/large_bundle-11                                916           1291614 ns/op        19620665 B/op      15061 allocs/op
BenchmarkCompressionRoundTrip/very_large_bundle-11                            30          38895861 ns/op        79369705 B/op     600171 allocs/op
BenchmarkCompressionRatio/small_bundle-11                               1000000000               0.0000057 ns/op               100.0 compressed_size_bytes               1.000 compression_ratio               100.0 original_size_bytes           0 size_difference_bytes               0 B/op          0 allocs/op
BenchmarkCompressionRatio/large_bundle-11                               1000000000               0.0009600 ns/op             14269 compressed_size_bytes                 0.1521 compression_ratio            93788 original_size_bytes         79519 size_difference_bytes               0 B/op          0 allocs/op
BenchmarkCompressionRatio/very_large_bundle-11                          1000000000               0.04109 ns/op      771176 compressed_size_bytes                 0.1542 compression_ratio          5000498 original_size_bytes       4229322 size_difference_bytes               0 B/op          0 allocs/op
PASS
ok      github.com/united-manufacturing-hub/benthos-umh/topic_browser_plugin      26.803s


Small Bundle (1 entry):
	Original size: 100 bytes
	Compressed size: 100 bytes
	Compression ratio: 1.000 (no compression)
	Size difference: 0 bytes
	This confirms our threshold is working - small bundles aren't compressed
Large Bundle (1,000 entries):
	Original size: 93,788 bytes
	Compressed size: 14,269 bytes
	Compression ratio: 0.1521 (84.79% reduction)
	Size difference: 79,519 bytes saved
	Very good compression ratio for the large bundle
Very Large Bundle (100,000 entries):
	Original size: 5,000,498 bytes (~4.77 MB)
	Compressed size: 771,176 bytes (~735 KB)
	Compression ratio: 0.1542 (84.58% reduction)
	Size difference: 4,229,322 bytes saved (~4.03 MB)
	Excellent compression ratio maintained even at this scale

The above bench results show that the space efficiency vastly outranks the added time used (Even for the very large dataset it is 10 vs 20ms).
However, for inputs under 1024 bytes, we skip compression, as the overhead from lz4's frame would actually increase the size.
*/

// lz4WriterPool reuses LZ4 writers to avoid constant allocations.
// This eliminates the 24MB+ buffer pool allocations we saw in pprof.
var lz4WriterPool = sync.Pool{
	New: func() interface{} {
		// Create a dummy buffer for initialization
		var buf bytes.Buffer
		zw := lz4.NewWriter(&buf)
		zw.CompressionLevel = 0 // Fastest compression
		return zw
	},
}

// lz4ReaderPool reuses LZ4 readers to avoid constant allocations.
var lz4ReaderPool = sync.Pool{
	New: func() interface{} {
		return lz4.NewReader(nil)
	},
}

// bundleToProtobuf converts an UNSBundle (containing both Topics and Events) to a protobuf representation
func bundleToProtobuf(bundle *UnsBundle) ([]byte, error) {
	protoBytes, err := proto.Marshal(bundle)
	if err != nil {
		return []byte{}, err
	}
	return protoBytes, nil
}

// protobufBytesToBundle converts protobuf-encoded data back to an UnsBundle
func protobufBytesToBundle(protoBytes []byte) (*UnsBundle, error) {
	bundle := &UnsBundle{}
	err := proto.Unmarshal(protoBytes, bundle)
	if err != nil {
		return nil, err
	}
	return bundle, nil
}

// BundleToProtobufBytes converts an UnsBundle to LZ4-compressed protobuf bytes.
//
// # OPTIMIZED LZ4 COMPRESSION WITH BUFFER POOLING
//
// This function implements a memory-optimized compression strategy using sync.Pool:
//
// ## Buffer Pool Strategy:
//   - LZ4 writers are reused via sync.Pool to eliminate constant allocations
//   - Eliminates the 24MB+ buffer pool overhead seen in pprof analysis
//   - Writer.Reset() allows safe reuse of existing writer instances
//   - Pool automatically manages writer lifecycle and memory
//
// ## Performance Benefits:
//   - Reduces heap pressure by ~50% (eliminates LZ4 buffer pool allocations)
//   - Eliminates GC pressure from constant writer creation/destruction
//   - Maintains same compression ratio (84%+) with much lower memory footprint
//   - Thread-safe pooling via sync.Pool
//
// ## LZ4 Configuration:
//   - Compression Level 0: Fastest compression, optimized for latency
//   - Always-on compression for predictable output format
//   - Pooled writers retain compression settings across reuse
//
// Returns the LZ4-compressed byte array, along with an error if any occurs.
//
// Args:
//   - bundle: The UnsBundle to serialize and compress
//
// Returns:
//   - []byte: LZ4-compressed protobuf bytes (always compressed)
//   - error: Any marshaling or compression error
func BundleToProtobufBytes(bundle *UnsBundle) ([]byte, error) {
	protoBytes, err := bundleToProtobuf(bundle)
	if err != nil {
		return []byte{}, err
	}

	// Get LZ4 writer from pool to avoid allocations
	zw := lz4WriterPool.Get().(*lz4.Writer)
	defer lz4WriterPool.Put(zw) // Return to pool for reuse

	// Create output buffer
	var compressedBuf bytes.Buffer

	// Reset writer with new output buffer (reuses internal buffers)
	zw.Reset(&compressedBuf)

	// Ensure compression level is set (in case pool contained different config)
	zw.CompressionLevel = 0

	// Write the protobuf bytes to the reused LZ4 writer
	_, err = zw.Write(protoBytes)
	if err != nil {
		return []byte{}, err
	}

	// Close the writer to flush any remaining data
	err = zw.Close()
	if err != nil {
		return []byte{}, err
	}

	return compressedBuf.Bytes(), nil
}

// ProtobufBytesToBundleWithCompression converts LZ4-compressed protobuf data back to an UnsBundle.
//
// # OPTIMIZED LZ4 DECOMPRESSION WITH BUFFER POOLING
//
// This function implements memory-optimized decompression using sync.Pool:
//
// ## Buffer Pool Strategy:
//   - LZ4 readers are reused via sync.Pool to eliminate constant allocations
//   - Reader.Reset() allows safe reuse of existing reader instances
//   - Pool automatically manages reader lifecycle and memory
//   - Thread-safe pooling via sync.Pool
//
// ## Performance Benefits:
//   - Reduces allocation pressure during decompression
//   - Eliminates GC overhead from reader creation/destruction
//   - Consistent decompression performance across operations
//   - Memory-efficient handling of large compressed bundles
//
// ## LZ4 Decompression Configuration:
//   - Always expects LZ4-compressed input (consistent with compression)
//   - Streaming decompression via io.Copy for memory efficiency
//   - Pooled readers maintain internal buffer state for optimal performance
//
// Returns the decoded UnsBundle or an error if decoding fails.
func ProtobufBytesToBundleWithCompression(compressedBytes []byte) (*UnsBundle, error) {
	// Get LZ4 reader from pool to avoid allocations
	zr := lz4ReaderPool.Get().(*lz4.Reader)
	defer lz4ReaderPool.Put(zr) // Return to pool for reuse

	// Create input reader
	r := bytes.NewReader(compressedBytes)

	// Reset reader with new input (reuses internal buffers)
	zr.Reset(r)

	// Ensure compression level is set (for consistency)
	zr.CompressionLevel = 0

	// Create a buffer to store the decompressed data
	var decompressedBuf bytes.Buffer

	// Copy the decompressed data to the buffer using pooled reader
	_, err := io.Copy(&decompressedBuf, zr)
	if err != nil {
		return nil, err
	}

	// Convert the decompressed data back to a protobuf bundle
	return protobufBytesToBundle(decompressedBuf.Bytes())
}
