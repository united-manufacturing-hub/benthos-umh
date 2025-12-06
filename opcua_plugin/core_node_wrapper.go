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

package opcua_plugin

import (
	"context"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

type NodeBrowser interface {
	// Attributes retrieves multiple attributes of the node
	Attributes(ctx context.Context, attrs ...ua.AttributeID) ([]*ua.DataValue, error)

	// BrowseName retrieves the browse name of the node
	BrowseName(ctx context.Context) (*ua.QualifiedName, error)

	// ReferencedNodes retrieves nodes referenced by this node based on specified criteria
	ReferencedNodes(ctx context.Context, refType uint32, browseDir ua.BrowseDirection, nodeClassMask ua.NodeClass, includeSubtypes bool) ([]NodeBrowser, error)

	// Children retrieves nodes referenced by this node based on specified criteria
	Children(ctx context.Context, refType uint32, nodeClassMask ua.NodeClass) ([]NodeBrowser, error)

	// ID returns the node identifier
	ID() *ua.NodeID
}

type OpcuaNodeWrapper struct {
	n *opcua.Node
}

func NewOpcuaNodeWrapper(n *opcua.Node) *OpcuaNodeWrapper {
	return &OpcuaNodeWrapper{n: n}
}

func (n *OpcuaNodeWrapper) Attributes(ctx context.Context, attrs ...ua.AttributeID) ([]*ua.DataValue, error) {
	return n.n.Attributes(ctx, attrs...)
}

func (n *OpcuaNodeWrapper) ReferencedNodes(ctx context.Context, refType uint32, browseDir ua.BrowseDirection, nodeClassMask ua.NodeClass, includeSubtypes bool) ([]NodeBrowser, error) {
	references, err := n.n.ReferencedNodes(ctx, refType, browseDir, nodeClassMask, includeSubtypes)
	if err != nil {
		return nil, err
	}
	var result []NodeBrowser
	for _, ref := range references {
		result = append(result, NewOpcuaNodeWrapper(ref))
	}
	return result, nil
}

func (n *OpcuaNodeWrapper) BrowseName(ctx context.Context) (*ua.QualifiedName, error) {
	return n.n.BrowseName(ctx)
}

func (n *OpcuaNodeWrapper) ID() *ua.NodeID {
	return n.n.ID
}

func (n *OpcuaNodeWrapper) Children(ctx context.Context, refType uint32, nodeClassMask ua.NodeClass) ([]NodeBrowser, error) {
	children, err := n.n.Children(ctx, refType, nodeClassMask)
	if err != nil {
		return nil, err
	}
	var result []NodeBrowser
	for _, child := range children {
		result = append(result, NewOpcuaNodeWrapper(child))
	}
	return result, nil
}
