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
BenchmarkBundleToProtobufBytes/small_bundle-11           								 2677615               436.7 ns/op           144 B/op          3 allocs/op
BenchmarkBundleToProtobufBytes/large_bundle-11              								4407            260758 ns/op          130306 B/op       2001 allocs/op
BenchmarkProtobufBytesToBundle/small_bundle-11           								 1935970               620.1 ns/op          1008 B/op         19 allocs/op
BenchmarkProtobufBytesToBundle/large_bundle-11              								2644            455011 ns/op          693827 B/op      13036 allocs/op
BenchmarkRoundTrip/small_bundle-11                      								 1000000              1073 ns/op            1152 B/op         22 allocs/op
BenchmarkRoundTrip/large_bundle-11                          								1634            725312 ns/op          824123 B/op      15037 allocs/op
BenchmarkBundleToProtobufBytesWithCompression/small_bundle-11               5044            211144 ns/op        10605152 B/op         11 allocs/op
BenchmarkBundleToProtobufBytesWithCompression/large_bundle-11               2182            512726 ns/op        10865698 B/op       2009 allocs/op
BenchmarkProtobufBytesToBundleWithCompression/small_bundle-11               5582            212213 ns/op         8391482 B/op         26 allocs/op
BenchmarkProtobufBytesToBundleWithCompression/large_bundle-11               1873            633713 ns/op         9344402 B/op      13050 allocs/op
BenchmarkCompressionRoundTrip/small_bundle-11                               3100            367288 ns/op        18859201 B/op         38 allocs/op
BenchmarkCompressionRoundTrip/large_bundle-11                                920           1283569 ns/op        19816002 B/op      15061 allocs/op
BenchmarkCompressionRatio/small_bundle-11                               1000000000               0.0000883 ns/op         1.190 compression_ratio               0 B/op          0 allocs/op
BenchmarkCompressionRatio/large_bundle-11                               1000000000               0.001061 ns/op          0.1524 compression_ratio              0 B/op          0 allocs/op
PASS
ok      github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin      15.729s


The above bench results show, that the space efficiency vastly outranks the added time used.
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

	// Create a buffer to store the compressed data
	var compressedBuf bytes.Buffer

	// Create an LZ4 writer
	zw := lz4.NewWriter(&compressedBuf)

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
	// Create a reader for the compressed data
	r := bytes.NewReader(compressedBytes)

	// Create an LZ4 reader
	zr := lz4.NewReader(r)

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
