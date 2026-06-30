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
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

const (
	extSnippetVirtualPath = "extensions.proto"
	// extSparkplugImportPath must equal the path recorded in the embedded descriptor, or the
	// resolver cannot satisfy the injected import.
	extSparkplugImportPath = "proto/sparkplug_b_official.proto"
	// extPreamble is prepended so the snippet compiles as proto2 and the Sparkplug extendees
	// resolve.
	extPreamble = `syntax = "proto2"; import "` + extSparkplugImportPath + `";` + "\n"
)

// extPreambleLines is the number of lines extPreamble adds ahead of the customer's snippet, so
// remapCompileError can subtract them and map diagnostics back to the customer's line. Derived
// from extPreamble so it can't drift if the preamble changes.
var extPreambleLines = strings.Count(extPreamble, "\n")

// extensionDecoder decodes the proto2 extension bytes a Sparkplug metric carries but the
// standard decode ignores. It is built once and is read-only afterwards.
type extensionDecoder struct {
	types      *protoregistry.Types
	metricDesc protoreflect.MessageDescriptor
}

// newExtensionDecoder compiles the snippet and fails (so the bridge fails to start) on an
// unparseable snippet, one declaring no extensions, or two scalar extensions whose leaf names
// collide into the same spb_ext_* key.
func newExtensionDecoder(snippet string) (*extensionDecoder, error) {
	if err := validateExtensionSnippet(snippet); err != nil {
		return nil, err
	}

	assembled := extPreamble + snippet
	resolver := protocompile.ResolverFunc(func(path string) (protocompile.SearchResult, error) {
		switch path {
		case extSnippetVirtualPath:
			return protocompile.SearchResult{Source: strings.NewReader(assembled)}, nil
		case extSparkplugImportPath:
			return protocompile.SearchResult{Desc: sparkplugb.File_proto_sparkplug_b_official_proto}, nil
		}
		return protocompile.SearchResult{}, fmt.Errorf("import %q is not available", path)
	})

	// WithStandardImports lets the snippet import well-known types (google/protobuf/*).
	compiler := protocompile.Compiler{Resolver: protocompile.WithStandardImports(resolver)}
	files, err := compiler.Compile(context.Background(), extSnippetVirtualPath)
	if err != nil {
		return nil, remapCompileError(err)
	}

	types := new(protoregistry.Types)
	if err := registerFileExtensions(types, files[0]); err != nil {
		return nil, fmt.Errorf("register extensions: %w", err)
	}
	if err := checkExtensions(types); err != nil {
		return nil, err
	}

	return &extensionDecoder{
		types:      types,
		metricDesc: (&sparkplugb.Payload_Metric{}).ProtoReflect().Descriptor(),
	}, nil
}

// carriesExtension reports whether the standard decode left unrecognized bytes on a message
// decode resolves extensions against. proto2 retains extension fields as unknown bytes, so an
// empty result means there is definitely nothing to decode. The check is a necessary, not
// sufficient, condition: any registered extension always lands here, but unrelated unknown
// fields can too, so decode still confirms before emitting. It must cover the same messages as
// supportedExtendees.
func carriesExtension(metric *sparkplugb.Payload_Metric) bool {
	if md := metric.GetMetadata(); md != nil && len(md.ProtoReflect().GetUnknown()) > 0 {
		return true
	}
	if ve := metric.GetExtensionValue(); ve != nil && len(ve.ProtoReflect().GetUnknown()) > 0 {
		return true
	}
	return false
}

// decode returns the metric's scalar extensions as leaf->value pairs and the full decoded
// metric as JSON; present is false when the metric carries no extension. Re-marshaling
// recovers the extension bytes that proto2 retained through the standard decode.
func (d *extensionDecoder) decode(metric *sparkplugb.Payload_Metric) (map[string]string, string, bool, error) {
	// Skip the re-marshal/decode for the common case of a metric with no extension bytes.
	if !carriesExtension(metric) {
		return nil, "", false, nil
	}

	raw, err := proto.Marshal(metric)
	if err != nil {
		return nil, "", false, fmt.Errorf("re-marshal metric: %w", err)
	}
	m := dynamicpb.NewMessage(d.metricDesc)
	if err = (proto.UnmarshalOptions{Resolver: d.types}).Unmarshal(raw, m); err != nil {
		return nil, "", false, fmt.Errorf("decode metric against extension schema: %w", err)
	}

	flat := map[string]string{}
	if !collectExtensions(m, flat) {
		return nil, "", false, nil
	}

	jb, err := (protojson.MarshalOptions{Resolver: d.types}).Marshal(m)
	if err != nil {
		return nil, "", false, fmt.Errorf("marshal decoded metric to json: %w", err)
	}
	return flat, string(jb), true, nil
}

// collectExtensions records every scalar extension as a leaf->value pair and reports whether
// any extension is set. It recurses because the extensions live on the nested MetaData and
// MetricValueExtension, not on the metric itself.
func collectExtensions(m protoreflect.Message, flat map[string]string) bool {
	present := false
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsExtension() {
			present = true
			if !fd.IsList() && !fd.IsMap() && isScalarExtKind(fd.Kind()) {
				flat[sanitizeExtLeaf(string(fd.Name()))] = scalarToString(fd, v)
			}
		}
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			switch {
			case fd.IsMap():
				// Maps don't carry extensions in the Sparkplug schema, so there's nothing to recurse into.
			case fd.IsList():
				l := v.List()
				for i := 0; i < l.Len(); i++ {
					if collectExtensions(l.Get(i).Message(), flat) {
						present = true
					}
				}
			default:
				if collectExtensions(v.Message(), flat) {
					present = true
				}
			}
		}
		return true
	})
	return present
}

func isScalarExtKind(k protoreflect.Kind) bool {
	switch k {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return false
	default:
		return true
	}
}

// scalarToString formats the value as protojson would, so a spb_ext_* value matches the
// corresponding key inside spb_metric_decoded.
func scalarToString(fd protoreflect.FieldDescriptor, v protoreflect.Value) string {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return strconv.FormatBool(v.Bool())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(v.Int(), 10)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10)
	case protoreflect.FloatKind:
		return strconv.FormatFloat(v.Float(), 'g', -1, 32)
	case protoreflect.DoubleKind:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64)
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case protoreflect.EnumKind:
		if ev := fd.Enum().Values().ByNumber(v.Enum()); ev != nil {
			return string(ev.Name())
		}
		return strconv.FormatInt(int64(v.Enum()), 10)
	default:
		return v.String()
	}
}

// sanitizeExtLeaf maps an extension's leaf field name to the suffix of its spb_ext_* metadata
// key. It shares sanitizeRunes with sanitizeForTopic but, unlike a topic segment, a flat key
// keeps no dots — though proto field names are already valid identifiers, so this only guards
// against unexpected input.
func sanitizeExtLeaf(name string) string {
	return sanitizeRunes(name, isASCIIAlnum)
}

// validateExtensionSnippet rejects a customer-declared syntax (proto3 is unsupported; the
// plugin owns the proto2 line) and a self-import of the Sparkplug schema (a second import is a
// duplicate-import compile error) before either reaches the compiler.
func validateExtensionSnippet(snippet string) error {
	for i, line := range strings.Split(snippet, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "syntax") && strings.Contains(t, "=") {
			return fmt.Errorf("line %d: remove the syntax declaration — the plugin compiles the snippet as proto2 (Sparkplug extensions require proto2)", i+1)
		}
		if strings.HasPrefix(t, "import") && strings.Contains(t, extSparkplugImportPath) {
			return fmt.Errorf("line %d: do not import %q — the plugin adds the Sparkplug schema automatically", i+1, extSparkplugImportPath)
		}
	}
	return nil
}

func registerFileExtensions(types *protoregistry.Types, fd protoreflect.FileDescriptor) error {
	if err := registerExtensions(types, fd.Extensions()); err != nil {
		return err
	}
	return registerMessageExtensions(types, fd.Messages())
}

func registerExtensions(types *protoregistry.Types, xs protoreflect.ExtensionDescriptors) error {
	for i := 0; i < xs.Len(); i++ {
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(xs.Get(i))); err != nil {
			return err
		}
	}
	return nil
}

func registerMessageExtensions(types *protoregistry.Types, ms protoreflect.MessageDescriptors) error {
	for i := 0; i < ms.Len(); i++ {
		m := ms.Get(i)
		if err := registerExtensions(types, m.Extensions()); err != nil {
			return err
		}
		if err := registerMessageExtensions(types, m.Messages()); err != nil {
			return err
		}
	}
	return nil
}

// supportedExtendees are the only messages decode resolves extensions against (Payload.MetaData
// and Payload.MetricValueExtension). An extension of any other message compiles fine but can
// never produce spb_ext_* or spb_metric_decoded, so checkExtensions ignores those and rejects a
// schema that targets only unsupported messages. Derived from the descriptors so the FQNs can't
// drift from the embedded schema.
var supportedExtendees = map[protoreflect.FullName]struct{}{
	(&sparkplugb.Payload_MetaData{}).ProtoReflect().Descriptor().FullName():             {},
	(&sparkplugb.Payload_MetricValueExtension{}).ProtoReflect().Descriptor().FullName(): {},
}

// checkExtensions requires at least one extension of a supported message and rejects two scalar
// extensions whose leaf names map to the same spb_ext_* key (message-typed extensions are not
// flattened, so they never collide).
func checkExtensions(types *protoregistry.Types) error {
	count := 0
	seen := map[string]protoreflect.FullName{}
	var collisionErr error
	types.RangeExtensions(func(xt protoreflect.ExtensionType) bool {
		etd := xt.TypeDescriptor()
		if _, ok := supportedExtendees[etd.ContainingMessage().FullName()]; !ok {
			// Extends a message decode never walks; it can't yield output, so ignore it.
			return true
		}
		count++
		if etd.IsList() || etd.IsMap() || !isScalarExtKind(etd.Kind()) {
			return true
		}
		leaf := sanitizeExtLeaf(string(etd.Name()))
		if prev, ok := seen[leaf]; ok {
			collisionErr = fmt.Errorf("extensions %q and %q both map to metadata key spb_ext_%s; rename one field (decoding is by field number, so the name is free to change)", prev, etd.FullName(), leaf)
			return false
		}
		seen[leaf] = etd.FullName()
		return true
	})
	if collisionErr != nil {
		return collisionErr
	}
	if count == 0 {
		return fmt.Errorf("extension schema declares no extensions of the Sparkplug Payload.MetaData or Payload.MetricValueExtension messages")
	}
	return nil
}

var extPosRe = regexp.MustCompile(regexp.QuoteMeta(extSnippetVirtualPath) + `:(\d+):(\d+)`)

// remapCompileError points protocompile's position back at the customer's snippet line by
// subtracting the injected preamble.
func remapCompileError(err error) error {
	s := extPosRe.ReplaceAllStringFunc(err.Error(), func(m string) string {
		sub := extPosRe.FindStringSubmatch(m)
		ln, _ := strconv.Atoi(sub[1])
		ln -= extPreambleLines
		if ln < 1 {
			ln = 1
		}
		return fmt.Sprintf("extensions:%d:%s", ln, sub[2])
	})
	return fmt.Errorf("compile extension schema: %s", s)
}
