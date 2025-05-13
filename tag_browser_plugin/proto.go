package tag_browser_plugin

import (
	"bytes"
	"io"

	"github.com/pierrec/lz4"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/proto"
)

/*
goos: darwin
goarch: arm64
pkg: github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin
cpu: Apple M3 Pro
BenchmarkBundleToProtobufBytes/small_bundle-11           2572542               505.4 ns/op           144 B/op          3 allocs/op
BenchmarkBundleToProtobufBytes/large_bundle-11              3853            298807 ns/op          130306 B/op       2001 allocs/op
BenchmarkProtobufBytesToBundle/small_bundle-11           1895758               634.6 ns/op          1008 B/op         19 allocs/op
BenchmarkProtobufBytesToBundle/large_bundle-11              2574            483978 ns/op          693808 B/op      13036 allocs/op
BenchmarkRoundTrip/small_bundle-11                       1000000              1098 ns/op            1152 B/op         22 allocs/op
BenchmarkRoundTrip/large_bundle-11                          1638            735684 ns/op          824119 B/op      15037 allocs/op
BenchmarkBundleToProtobufBytesWithCompression/small_bundle-11            2705947               441.8 ns/op           144 B/op          3 allocs/op
BenchmarkBundleToProtobufBytesWithCompression/large_bundle-11               2101            509239 ns/op         9781171 B/op       2009 allocs/op
BenchmarkProtobufBytesToBundleWithCompression/small_bundle-11            1892642               636.0 ns/op          1008 B/op         19 allocs/op
BenchmarkProtobufBytesToBundleWithCompression/large_bundle-11               1798            655106 ns/op         9344381 B/op      13050 allocs/op
BenchmarkCompressionRoundTrip/small_bundle-11                            1000000              1111 ns/op            1152 B/op         22 allocs/op
BenchmarkCompressionRoundTrip/large_bundle-11                                915           1277903 ns/op        19906191 B/op      15061 allocs/op
BenchmarkCompressionRatio/small_bundle-11                               1000000000               0.0000082 ns/op                 1.000 compression_ratio               0 B/op          0 allocs/op
BenchmarkCompressionRatio/large_bundle-11                               1000000000               0.001156 ns/op          0.1525 compression_ratio              0 B/op          0 allocs/op
PASS
ok      github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin      17.065s


The above bench results show that the space efficiency vastly outranks the added time used.
However, for inputs under 1024 bytes, we skip compression, as the overhead from lz4's frame would actually increase the size.
*/

func BundleToProtobuf(bundle *tagbrowserpluginprotobuf.UnsBundle) ([]byte, error) {
	protoBytes, err := proto.Marshal(bundle)
	if err != nil {
		return []byte{}, err
	}
	return protoBytes, nil
}

func ProtobufBytesToBundle(protoBytes []byte) (*tagbrowserpluginprotobuf.UnsBundle, error) {
	bundle := &tagbrowserpluginprotobuf.UnsBundle{}
	err := proto.Unmarshal(protoBytes, bundle)
	if err != nil {
		return nil, err
	}
	return bundle, nil
}

func BundleToProtobufBytesWithCompression(bundle *tagbrowserpluginprotobuf.UnsBundle) ([]byte, error) {
	protoBytes, err := BundleToProtobuf(bundle)
	if err != nil {
		return []byte{}, err
	}

	if len(protoBytes) < 1024 {
		return protoBytes, nil
	}

	// Create a buffer to store the compressed data
	var compressedBuf bytes.Buffer

	// Create an LZ4 writer
	zw := lz4.NewWriter(&compressedBuf)
	zw.CompressionLevel = 0

	// Write the protobuf bytes to the LZ4 writer
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

func ProtobufBytesToBundleWithCompression(compressedBytes []byte) (*tagbrowserpluginprotobuf.UnsBundle, error) {
	// If the compressedBytes dont start with the LZ4 magic number, return the original bytes
	if !bytes.Equal(compressedBytes[:4], []byte{0x04, 0x22, 0x4d, 0x18}) {
		return ProtobufBytesToBundle(compressedBytes)
	}

	// Create a reader for the compressed data
	r := bytes.NewReader(compressedBytes)

	// Create an LZ4 reader
	zr := lz4.NewReader(r)
	zr.CompressionLevel = 0

	// Create a buffer to store the decompressed data
	var decompressedBuf bytes.Buffer

	// Copy the decompressed data to the buffer
	_, err := io.Copy(&decompressedBuf, zr)
	if err != nil {
		return nil, err
	}

	// Convert the decompressed data back to a protobuf bundle
	return ProtobufBytesToBundle(decompressedBuf.Bytes())
}
