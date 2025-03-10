package opcua_plugin

import (
	"fmt"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

// AttributeHandler is the struct that handles the attributes of a node
type AttributeHandler struct {
	// Using more idiomatic names
	handleOK          func(value *ua.Variant) error // handleOK is the function that handles the OK status
	handleNotReadable func()                        // handleNotReadable is the function that handles the NotReadable status
	ignoreInvalidAttr bool                          // ignoreInvalidAttr is the flag that determines if invalid attributes should be ignored
	requiresValue     bool                          // requiresValue is the flag that determines if a value is required
	affectsNodeClass  bool                          // affectsNodeClass is the flag that determines if the node class should be affected
}

// handleAttributeStatus is the function that handles the attributes of a node
func handleAttributeStatus(
	name string,
	attr *ua.DataValue,
	def *NodeDef,
	path string,
	logger Logger,
	handler AttributeHandler,
) error {
	if attr == nil {
		return fmt.Errorf("attribute is nil for node: %s and attribute: %s", def.NodeID, name)
	}

	if attr.Status == ua.StatusOK {
		if attr.Value == nil && handler.requiresValue {
			return fmt.Errorf("attribute value is nil for node: %s and attribute: %s", def.NodeID, name)
		}
		return handleOKStatus(attr.Value, handler)
	}

	if attr.Status == ua.StatusBadSecurityModeInsufficient {
		return fmt.Errorf("got insufficient security mode for node: %s and attribute: %s", def.NodeID, name)
	}

	if attr.Status == ua.StatusBadAttributeIDInvalid {
		if handler.ignoreInvalidAttr {
			return nil
		}
		return fmt.Errorf("invalid attribute ID for node: %s and attribute: %s", def.NodeID, name)
	}

	if attr.Status == ua.StatusBadNotReadable {
		handleNotReadable(def, handler, path, logger)
		return nil
	}

	return attr.Status
}

// handleOKStatus processes successful attribute reads
func handleOKStatus(value *ua.Variant, handler AttributeHandler) error {
	if handler.handleOK != nil && value != nil {
		return handler.handleOK(value)
	}
	return nil
}

// handleNotReadable processes non-readable attributes
func handleNotReadable(def *NodeDef, handler AttributeHandler, path string, logger Logger) {
	if handler.affectsNodeClass {
		def.NodeClass = ua.NodeClassObject
	}
	if handler.handleNotReadable != nil {
		handler.handleNotReadable()
	}
	logger.Warnf("Access denied for node: %s, continuing...\n", path)
}

// processNodeAttributes processes all attributes for a node
// This function is used to process the attributes of a node and set the NodeDef struct
// def.NodeClass, def.BrowseName, def.Description, def.AccessLevel, def.DataType are set in the processNodeAttributes function
func processNodeAttributes(attrs []*ua.DataValue, def *NodeDef, path string, logger Logger) error {
	// NodeClass (attrs[0])
	nodeClassHandler := AttributeHandler{
		handleOK: func(value *ua.Variant) error {
			def.NodeClass = ua.NodeClass(value.Int())
			return nil
		},
		requiresValue:    true, // NodeClass is required else the node is not valid
		affectsNodeClass: true, // If the node class could not be determined, it is set to Object type and browsing can be continued
	}
	if err := handleAttributeStatus("NodeClass", attrs[0], def, path, logger, nodeClassHandler); err != nil {
		return err
	}

	// BrowseName (attrs[1])
	browseNameHandler := AttributeHandler{
		handleOK: func(value *ua.Variant) error {
			def.BrowseName = value.String()
			return nil
		},
		requiresValue: true, // BrowseName is required else the node is not valid
	}
	if err := handleAttributeStatus("BrowseName", attrs[1], def, path, logger, browseNameHandler); err != nil {
		return err
	}

	// Description (attrs[2])
	descriptionHandler := AttributeHandler{
		handleOK: func(value *ua.Variant) error {
			if value != nil {
				def.Description = value.String()
				return nil
			}
			// Description for Kepware v6 is OPCUATYPE_NULL. Set to empty string in such cases
			def.Description = ""
			return nil
		},
		ignoreInvalidAttr: true, // For some node, Description is not available and this is not an error and can be ignored
		affectsNodeClass:  true, // If the node class could not be determined, it is set to Object type and browsing can be continued. By setting it to object, we will not subscribe to this node
	}
	if err := handleAttributeStatus("Description", attrs[2], def, path, logger, descriptionHandler); err != nil {
		return err
	}

	// AccessLevel (attrs[3])
	accessLevelHandler := AttributeHandler{
		handleOK: func(value *ua.Variant) error {
			def.AccessLevel = ua.AccessLevelType(value.Int())
			return nil
		},
		ignoreInvalidAttr: true, // For some node, AccessLevel is not available and this is not an error and can be ignored
	}
	if err := handleAttributeStatus("AccessLevel", attrs[3], def, path, logger, accessLevelHandler); err != nil {
		return err
	}

	// DataType (attrs[4])
	dataTypeHandler := AttributeHandler{
		handleOK: func(value *ua.Variant) error {
			if value == nil {
				logger.Debugf("ignoring node: %s as its datatype is nil...\n", path)
				return fmt.Errorf("datatype is nil for node: %s and attribute: %s", def.NodeID, "DataType")
			}
			def.DataType = getDataTypeString(value.NodeID().IntID())
			return nil
		},
		ignoreInvalidAttr: true, // For some node, DataType is not available and this is not an error and can be ignored
		affectsNodeClass:  true, // If the node class could not be determined, it is set to Object type and browsing can be continued. By setting it to object, we will not subscribe to this node
	}
	if err := handleAttributeStatus("DataType", attrs[4], def, path, logger, dataTypeHandler); err != nil {
		return err
	}

	return nil
}

var dataTypeMap = map[uint32]string{
	id.DateTime: "time.Time",
	id.Boolean:  "bool",
	id.SByte:    "int8",
	id.Int16:    "int16",
	id.Int32:    "int32",
	id.Byte:     "byte",
	id.UInt16:   "uint16",
	id.UInt32:   "uint32",
	id.UtcTime:  "time.Time",
	id.String:   "string",
	id.Float:    "float32",
	id.Double:   "float64",
}

func getDataTypeString(typeID uint32) string {
	if dtype, ok := dataTypeMap[typeID]; ok {
		return dtype
	}
	return fmt.Sprintf("ns=%d;i=%d", 0, typeID)
}
