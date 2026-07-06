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

// Package protobuf decodes and encodes arbitrary protobuf messages against an
// inline base64 FileDescriptorSet, for use from the nodered_js / tag_processor JS
// runtime. Extensions are registered so they surface in protojson as "[pkg.ext]"
// keys (ENG-5243).
package protobuf

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// registry holds the descriptors and extension types built from one descriptor set.
type registry struct {
	files *protoregistry.Files
	types *protoregistry.Types
}

// maxCachedRegistries bounds the memo cache. The descriptor set is untrusted input
// from a user script, so an unbounded cache would be a memory-exhaustion vector if a
// script decoded with many distinct descriptor sets. Realistic use is one or a few
// static per-bridge descriptors, well under this cap.
const maxCachedRegistries = 128

// registryCache memoizes built registries keyed by the base64 descriptor set, so the
// hot path skips decoding and building. Building the registry dominates per-call cost;
// a cached registry is read-only (Find* reads are concurrency-safe).
var (
	registryMu    sync.Mutex
	registryCache = map[string]*registry{}
)

// Decode decodes base64 proto bytes of message msgName against an inline base64
// FileDescriptorSet, returning the message in protojson shape. Extensions surface
// as "[pkg.ext]" keys.
func Decode(dataB64 string, descriptorSetB64 string, msgName string) (map[string]any, error) {
	reg, err := loadRegistry(descriptorSetB64)
	if err != nil {
		return nil, err
	}
	md, err := reg.messageDescriptor(msgName)
	if err != nil {
		return nil, err
	}
	data, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		return nil, fmt.Errorf("decode data base64: %w", err)
	}
	msg := dynamicpb.NewMessage(md)
	if err = (proto.UnmarshalOptions{Resolver: reg.types}).Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("unmarshal proto: %w", err)
	}
	jsonBytes, err := (protojson.MarshalOptions{Resolver: reg.types}).Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal protojson: %w", err)
	}
	out := map[string]any{}
	if err = json.Unmarshal(jsonBytes, &out); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}
	return out, nil
}

// Encode is the inverse of Decode: it encodes a protojson-shaped object to base64
// proto bytes of message msgName against an inline base64 FileDescriptorSet.
func Encode(obj any, descriptorSetB64 string, msgName string) (string, error) {
	m, ok := obj.(map[string]any)
	if !ok {
		return "", fmt.Errorf("encode input must be an object, got %T", obj)
	}
	reg, err := loadRegistry(descriptorSetB64)
	if err != nil {
		return "", err
	}
	md, err := reg.messageDescriptor(msgName)
	if err != nil {
		return "", err
	}
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode json: %w", err)
	}
	msg := dynamicpb.NewMessage(md)
	if err = (protojson.UnmarshalOptions{Resolver: reg.types}).Unmarshal(jsonBytes, msg); err != nil {
		return "", fmt.Errorf("unmarshal protojson: %w", err)
	}
	out, err := proto.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("marshal proto: %w", err)
	}
	return base64.StdEncoding.EncodeToString(out), nil
}

func loadRegistry(descriptorSetB64 string) (*registry, error) {
	registryMu.Lock()
	if reg, ok := registryCache[descriptorSetB64]; ok {
		registryMu.Unlock()
		return reg, nil
	}
	registryMu.Unlock()

	// Build outside the lock — parsing is the expensive part, and a concurrent
	// double-build is harmless because the result is identical (idempotent).
	reg, err := buildRegistry(descriptorSetB64)
	if err != nil {
		return nil, err
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	if existing, ok := registryCache[descriptorSetB64]; ok {
		return existing, nil // a concurrent caller won the race; reuse its registry
	}
	if len(registryCache) >= maxCachedRegistries {
		// The cache is a pure optimization, so evicting an arbitrary entry to stay
		// bounded never affects correctness — the next call rebuilds it.
		for k := range registryCache {
			delete(registryCache, k)
			break
		}
	}
	registryCache[descriptorSetB64] = reg
	return reg, nil
}

func buildRegistry(descriptorSetB64 string) (*registry, error) {
	descBytes, err := base64.StdEncoding.DecodeString(descriptorSetB64)
	if err != nil {
		return nil, fmt.Errorf("decode descriptor set base64: %w", err)
	}
	var fds descriptorpb.FileDescriptorSet
	if err = proto.Unmarshal(descBytes, &fds); err != nil {
		return nil, fmt.Errorf("parse FileDescriptorSet: %w", err)
	}
	files, err := protodesc.NewFiles(&fds)
	if err != nil {
		return nil, fmt.Errorf("build descriptor files: %w", err)
	}

	types := new(protoregistry.Types)
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		registerExtensions(types, fd.Extensions())
		registerMessageExtensions(types, fd.Messages())
		return true
	})
	return &registry{files: files, types: types}, nil
}

func (r *registry) messageDescriptor(msgName string) (protoreflect.MessageDescriptor, error) {
	d, err := r.files.FindDescriptorByName(protoreflect.FullName(msgName))
	if err != nil {
		return nil, fmt.Errorf("message %q not found in descriptor set: %w", msgName, err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a message", msgName)
	}
	return md, nil
}

func registerExtensions(types *protoregistry.Types, xs protoreflect.ExtensionDescriptors) {
	for i := 0; i < xs.Len(); i++ {
		// A duplicate-registration error can't occur within a single fresh build; ignore.
		_ = types.RegisterExtension(dynamicpb.NewExtensionType(xs.Get(i)))
	}
}

func registerMessageExtensions(types *protoregistry.Types, ms protoreflect.MessageDescriptors) {
	for i := 0; i < ms.Len(); i++ {
		m := ms.Get(i)
		registerExtensions(types, m.Extensions())
		registerMessageExtensions(types, m.Messages())
	}
}
