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
	refrences, err := n.n.ReferencedNodes(ctx, refType, browseDir, nodeClassMask, includeSubtypes)
	if err != nil {
		return nil, err
	}
	var result []NodeBrowser
	for _, ref := range refrences {
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
