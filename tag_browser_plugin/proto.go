package tag_browser_plugin

import (
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/proto"
)

func BundleToProtobufBytes(bundle *tagbrowserpluginprotobuf.UnsBundle) ([]byte, error) {
	protoBytes, err := proto.Marshal(bundle)
	if err != nil {
		return []byte{}, err
	}
	return protoBytes, nil
}
