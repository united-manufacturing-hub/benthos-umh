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
	// extSnippetVirtualPath is the synthetic filename the customer's snippet compiles under;
	// it appears in compile diagnostics.
	extSnippetVirtualPath = "extensions.proto"
	// extSparkplugImportPath must equal the path recorded in the embedded Sparkplug descriptor.
	extSparkplugImportPath = "proto/sparkplug_b_official.proto"
	// extPreamble is prepended to the snippet: syntax must be the first statement, and the
	// import makes the Sparkplug extendees (Payload.MetaData / Payload.MetricValueExtension)
	// resolvable. One line, so compile diagnostics are offset by exactly extPreambleLines.
	extPreamble      = `syntax = "proto2"; import "` + extSparkplugImportPath + `";` + "\n"
	extPreambleLines = 1
)

// extensionDecoder turns the proto2 extension bytes a Sparkplug metric carries (but the
// standard decode ignores) into metadata. It compiles the customer's inline extension schema
// once, against the embedded Sparkplug descriptor, and is read-only — safe for concurrent use
// by Read after newExtensionDecoder returns.
type extensionDecoder struct {
	types      *protoregistry.Types
	metricDesc protoreflect.MessageDescriptor
}

// newExtensionDecoder compiles the proto2 snippet and builds the resolver. It fails (so the
// bridge fails to start) on an unparseable snippet, a snippet that declares no extensions, or
// two scalar extensions whose leaf names would collide into the same spb_ext_* key.
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

// decode re-marshals the metric (proto2 retains the extension bytes the standard decode kept),
// decodes it against the generated Payload.Metric descriptor with the extension resolver, and
// returns the scalar extensions as flat leaf->value pairs plus the full decoded metric as JSON.
// present is false (and the strings nil/empty) when the metric carries no extension at all, so
// the caller adds no metadata.
func (d *extensionDecoder) decode(metric *sparkplugb.Payload_Metric) (flat map[string]string, decodedJSON string, present bool, err error) {
	raw, err := proto.Marshal(metric)
	if err != nil {
		return nil, "", false, fmt.Errorf("re-marshal metric: %w", err)
	}
	m := dynamicpb.NewMessage(d.metricDesc)
	if err := (proto.UnmarshalOptions{Resolver: d.types}).Unmarshal(raw, m); err != nil {
		return nil, "", false, fmt.Errorf("decode metric against extension schema: %w", err)
	}

	flat = map[string]string{}
	present = collectExtensions(m, flat)
	if !present {
		return nil, "", false, nil
	}

	jb, err := (protojson.MarshalOptions{Resolver: d.types}).Marshal(m)
	if err != nil {
		return nil, "", false, fmt.Errorf("marshal decoded metric to json: %w", err)
	}
	return flat, string(jb), true, nil
}

// collectExtensions walks the decoded message, recursing into sub-messages (extensions live on
// the nested MetaData and MetricValueExtension, not on the metric itself). It records every
// scalar extension as a flat leaf->value pair and reports whether any extension is set.
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
				// Sparkplug extendees have no map fields; nothing to recurse.
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

// scalarToString formats a scalar extension value the way protojson would render it, so a
// spb_ext_* metadata value matches the corresponding key inside spb_metric_decoded.
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

// sanitizeExtLeaf maps an extension's leaf field name to its spb_ext_* suffix. Proto field
// names are already [A-Za-z0-9_], so this is effectively identity, but it guards the contract.
func sanitizeExtLeaf(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// validateExtensionSnippet rejects the two preamble collisions: a customer-declared syntax
// (proto3 is unsupported; we own the proto2 line) and a self-import of the Sparkplug schema
// (the plugin injects it — a second import is a duplicate-import compile error).
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

// checkExtensions enforces two startup invariants: at least one extension exists, and no two
// scalar extensions collapse to the same spb_ext_* leaf key (message-typed extensions are not
// flattened, so they never collide).
func checkExtensions(types *protoregistry.Types) error {
	count := 0
	seen := map[string]protoreflect.FullName{}
	var collisionErr error
	types.RangeExtensions(func(xt protoreflect.ExtensionType) bool {
		count++
		etd := xt.TypeDescriptor()
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

// remapCompileError rewrites protocompile's position prefix so it points at the customer's
// snippet line (subtracting the injected preamble) under a friendlier filename.
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
