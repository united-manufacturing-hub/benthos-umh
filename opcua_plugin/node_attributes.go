package opcua_plugin

import (
	"errors"
	"fmt"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

// AttributeHandler can be simplified by using a more focused structure
type AttributeHandler struct {
	// Using more idiomatic names
	handleOK          func(value *ua.Variant) error // handleOK is the function that handles the OK status
	handleNotReadable func()                        // handleNotReadable is the function that handles the NotReadable status
	ignoreInvalidAttr bool                          // ignoreInvalidAttr is the flag that determines if invalid attributes should be ignored
	requiresValue     bool                          // requiresValue is the flag that determines if a value is required
	affectsNodeClass  bool                          // affectsNodeClass is the flag that determines if the node class should be affected
}

// handleAttributeStatus can be simplified by using early returns
func handleAttributeStatus(
	name string,
	attr *ua.DataValue,
	def *NodeDef,
	path string,
	logger Logger,
	handler AttributeHandler,
) error {
	if attr == nil {
		return fmt.Errorf("attribute is nil")
	}

	if attr.Status == ua.StatusOK {
		if attr.Value == nil && handler.requiresValue {
			return fmt.Errorf("attribute value is nil for %s", name)
		}
		return handleOKStatus(attr.Value, handler)
	}

	if attr.Status == ua.StatusBadSecurityModeInsufficient {
		return errors.New("insufficient security mode")
	}

	if attr.Status == ua.StatusBadAttributeIDInvalid {
		if handler.ignoreInvalidAttr {
			return nil
		}
		return fmt.Errorf("invalid attribute ID")
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
		requiresValue:    true,
		affectsNodeClass: true,
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
		requiresValue: true,
	}
	if err := handleAttributeStatus("BrowseName", attrs[1], def, path, logger, browseNameHandler); err != nil {
		return err
	}

	// Description (attrs[2])
	descriptionHandler := AttributeHandler{
		handleOK: func(value *ua.Variant) error {
			if value != nil {
				def.Description = value.String()
			} else {
				def.Description = ""
			}
			return nil
		},
		ignoreInvalidAttr: true,
		affectsNodeClass:  true,
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
		ignoreInvalidAttr: true,
	}
	if err := handleAttributeStatus("AccessLevel", attrs[3], def, path, logger, accessLevelHandler); err != nil {
		return err
	}

	// Check AccessLevel None
	if def.AccessLevel == ua.AccessLevelTypeNone {
		logger.Warnf("Node %s has AccessLevel None, marking as Object\n", path)
		def.NodeClass = ua.NodeClassObject
	}

	// DataType (attrs[4])
	dataTypeHandler := AttributeHandler{
		handleOK: func(value *ua.Variant) error {
			if value == nil {
				logger.Debugf("ignoring node: %s as its datatype is nil...\n", path)
				return fmt.Errorf("datatype is nil")
			}
			def.DataType = getDataTypeString(value.NodeID().IntID())
			return nil
		},
		ignoreInvalidAttr: true,
		affectsNodeClass:  true,
	}
	if err := handleAttributeStatus("DataType", attrs[4], def, path, logger, dataTypeHandler); err != nil {
		return err
	}

	return nil
}

// getDataTypeString can be improved by using a constant map
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
