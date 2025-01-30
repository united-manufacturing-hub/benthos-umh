package opcua_plugin

import (
	"errors"
	"fmt"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

// AttributeHandler defines how to handle different attribute statuses and values
type AttributeHandler struct {
	onOK             func(value *ua.Variant) error
	onNotReadable    func()
	onInvalidAttr    bool // whether to ignore invalid attribute errors
	requiresValue    bool // whether a nil value is acceptable
	affectsNodeClass bool // whether errors should mark node as Object
}

// handleAttributeStatus processes an attribute's status and value according to defined handlers
func handleAttributeStatus(
	attr *ua.DataValue,
	def *NodeDef,
	path string,
	logger Logger,
	handler AttributeHandler,
) error {
	// Early validation for nil value when required
	if attr.Status == ua.StatusOK && attr.Value == nil && handler.requiresValue {
		return fmt.Errorf("attribute value is nil")
	}

	switch attr.Status {
	case ua.StatusOK:
		return handleOKStatus(attr.Value, handler)

	case ua.StatusBadSecurityModeInsufficient:
		return errors.New("insufficient security mode")

	case ua.StatusBadAttributeIDInvalid:
		if handler.onInvalidAttr {
			return nil // Skip invalid attributes if allowed
		}
		return fmt.Errorf("invalid attribute ID")

	case ua.StatusBadNotReadable:
		handleNotReadable(def, handler, path, logger)
		return nil

	default:
		return attr.Status
	}
}

// handleOKStatus processes successful attribute reads
func handleOKStatus(value *ua.Variant, handler AttributeHandler) error {
	if handler.onOK != nil && value != nil {
		return handler.onOK(value)
	}
	return nil
}

// handleNotReadable processes non-readable attributes
func handleNotReadable(def *NodeDef, handler AttributeHandler, path string, logger Logger) {
	if handler.affectsNodeClass {
		def.NodeClass = ua.NodeClassObject
	}
	if handler.onNotReadable != nil {
		handler.onNotReadable()
	}
	logger.Warnf("Access denied for node: %s, continuing...\n", path)
}

// processNodeAttributes processes all attributes for a node
// This function is used to process the attributes of a node and set the NodeDef struct
func processNodeAttributes(attrs []*ua.DataValue, def *NodeDef, path string, logger Logger) error {
	// NodeClass (attrs[0])
	nodeClassHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			def.NodeClass = ua.NodeClass(value.Int())
			return nil
		},
		requiresValue:    true,
		affectsNodeClass: true,
	}
	if err := handleAttributeStatus(attrs[0], def, path, logger, nodeClassHandler); err != nil {
		return err
	}

	// BrowseName (attrs[1])
	browseNameHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			def.BrowseName = value.String()
			return nil
		},
		requiresValue: true,
	}
	if err := handleAttributeStatus(attrs[1], def, path, logger, browseNameHandler); err != nil {
		return err
	}

	// Description (attrs[2])
	descriptionHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			if value != nil {
				def.Description = value.String()
			} else {
				def.Description = ""
			}
			return nil
		},
		onInvalidAttr:    true,
		affectsNodeClass: true,
	}
	if err := handleAttributeStatus(attrs[2], def, path, logger, descriptionHandler); err != nil {
		return err
	}

	// AccessLevel (attrs[3])
	accessLevelHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			def.AccessLevel = ua.AccessLevelType(value.Int())
			return nil
		},
		onInvalidAttr: true,
	}
	if err := handleAttributeStatus(attrs[3], def, path, logger, accessLevelHandler); err != nil {
		return err
	}

	// Check AccessLevel None
	if def.AccessLevel == ua.AccessLevelTypeNone {
		logger.Warnf("Node %s has AccessLevel None, marking as Object\n", path)
		def.NodeClass = ua.NodeClassObject
	}

	// DataType (attrs[4])
	dataTypeHandler := AttributeHandler{
		onOK: func(value *ua.Variant) error {
			if value == nil {
				logger.Debugf("ignoring node: %s as its datatype is nil...\n", path)
				return fmt.Errorf("datatype is nil")
			}
			def.DataType = getDataTypeString(value.NodeID().IntID())
			return nil
		},
		onInvalidAttr:    true,
		affectsNodeClass: true,
	}
	if err := handleAttributeStatus(attrs[4], def, path, logger, dataTypeHandler); err != nil {
		return err
	}

	return nil
}

// getDataTypeString maps OPC UA data type IDs to Go type strings
func getDataTypeString(typeID uint32) string {
	dataTypeMap := map[uint32]string{
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

	if dtype, ok := dataTypeMap[typeID]; ok {
		return dtype
	}
	return fmt.Sprintf("ns=%d;i=%d", 0, typeID)
}
