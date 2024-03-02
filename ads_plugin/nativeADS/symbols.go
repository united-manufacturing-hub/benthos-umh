package ads

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
)

type datatypeEntry struct {
	EntryLength   uint32
	Version       uint32
	HashValue     uint32
	TypeHashValue uint32
	Size          uint32
	Offs          uint32
	DataType      uint32
	Flags         uint32
	NameLength    uint16
	TypeLength    uint16
	CommentLength uint16
	ArrayDim      uint16
	SubItems      uint16
}

type datatypeArrayInfo struct {
	LBound   uint32
	Elements uint32
}

type SymbolUploadDataType struct {
	DatatypeEntry datatypeEntry
	Name          string
	DataType      string
	Comment       string
	Childs        map[string]*SymbolUploadDataType
}

type symbolEntry struct {
	EntryLength   uint32
	IGroup        uint32
	IOffs         uint32
	Size          uint32
	DataType      uint32
	Flags         uint32
	NameLength    uint16
	TypeLength    uint16
	CommentLength uint16
}

type symbolUploadSymbol struct {
	SymbolEntry symbolEntry
	Name        string
	DataType    string
	Comment     string
	Childs      map[string]*symbolUploadSymbol
}

type SymbolUploadInfo struct {
	SymbolCount    uint32
	SymbolLength   uint32
	DataTypeCount  uint32
	DataTypeLength uint32
	ExtraCount     uint32
	ExtraLength    uint32
}

type Symbol struct {
	FullName          string
	LastUpdateTime    time.Time
	MinUpdateInterval time.Duration
	Name              string
	DataType          string
	Comment           string
	Handle            uint32
	Group             uint32
	Offset            uint32
	Length            uint32
	Changed           bool

	Value string
	Valid bool

	Notification chan<- *Update

	Parent *Symbol
	Childs map[string]*Symbol
}

func ParseUploadSymbolInfoSymbols(data []byte, datatypes map[string]SymbolUploadDataType) (symbols map[string]*Symbol, err error) {
	symbols = map[string]*Symbol{}
	buff := bytes.NewBuffer(data)

	for buff.Len() > 0 {
		begBuff := buff.Len()
		result := symbolEntry{}
		binary.Read(buff, binary.LittleEndian, &result)

		name := make([]byte, result.NameLength)
		dt := make([]byte, result.TypeLength)
		comment := make([]byte, result.CommentLength)
		err := binary.Read(buff, binary.LittleEndian, name)
		if err != nil {
			log.Error().Err(err).Msg("error during binary read")
		}
		buff.Next(1)
		binary.Read(buff, binary.LittleEndian, dt)
		if err != nil {
			log.Error().Err(err).Msg("error during binary read")
		}
		buff.Next(1)
		binary.Read(buff, binary.LittleEndian, comment)
		if err != nil {
			log.Error().Err(err).Msg("error during binary read")
		}
		buff.Next(1)
		if err != nil {
			log.Error().
				Err(err).
				Msg("Error during parse")
		}
		item := symbolUploadSymbol{}
		item.Name = string(name)
		item.DataType = string(dt)
		if len(item.DataType) >= 6 {
			if item.DataType[:6] == "STRING" {
				item.DataType = "STRING"
			}
		}
		item.Comment = string(comment)
		item.SymbolEntry = result
		endBuff := buff.Len()
		symbol := addSymbol(item, datatypes)

		symbols[item.Name] = symbol
		addChilds(symbol, symbols)

		buff.Next(int(item.SymbolEntry.EntryLength) - (begBuff - endBuff))
	}
	return
}

func addChilds(symbol *Symbol, symbols map[string]*Symbol) {
	for _, child := range symbol.Childs {
		if _, ok := symbols[child.FullName]; !ok {
			symbols[child.FullName] = child
			addChilds(child, symbols)
		}
	}
}

func addSymbol(symbol symbolUploadSymbol, datatypes map[string]SymbolUploadDataType) *Symbol {
	sym := &Symbol{}
	sym.Name = symbol.Name
	sym.LastUpdateTime = time.Now()
	sym.MinUpdateInterval = time.Millisecond * 50
	sym.FullName = symbol.Name
	sym.DataType = symbol.DataType
	sym.Comment = symbol.Comment
	sym.Length = symbol.SymbolEntry.Size

	sym.Group = symbol.SymbolEntry.IGroup
	sym.Offset = symbol.SymbolEntry.IOffs

	dt, ok := datatypes[symbol.DataType]
	if ok {
		sym.Childs = dt.addOffset(sym, datatypes, sym.Group, sym.Offset)
	}

	return sym
}

func (data *SymbolUploadDataType) addOffset(parent *Symbol, datatypes map[string]SymbolUploadDataType, group uint32, offset uint32) (childs map[string]*Symbol) {
	childs = map[string]*Symbol{}

	var path string

	for key, segment := range data.Childs {

		if segment.Name[0:1] != "[" {
			path = fmt.Sprint(parent.FullName, ".", segment.Name)
		} else {
			path = fmt.Sprint(parent.Name, segment.Name)
		}

		child := Symbol{}
		child.Name = segment.Name
		child.LastUpdateTime = time.Now()
		child.MinUpdateInterval = time.Millisecond * 50
		child.FullName = path
		child.DataType = segment.DataType
		child.Comment = segment.Comment
		child.Length = segment.DatatypeEntry.Size
		// Uppdate with area and offset
		child.Group = group
		child.Offset = segment.DatatypeEntry.Offs

		child.Parent = parent

		// Check if subitems exist
		dt, ok := datatypes[segment.DataType]
		if segment.DataType == "AnalogueValue" {
			log.Error().Msg("test")
		}
		if ok {
			//log.Warn("Found sub ",segment.DataType);
			child.Childs = dt.addOffset(&child, datatypes, child.Group, child.Offset)

		}

		childs[key] = &child
	}

	return
}

func ParseUploadSymbolInfoDataTypes(data []byte) (datatypes map[string]SymbolUploadDataType, err error) {
	buff := bytes.NewBuffer(data)
	datatypes = make(map[string]SymbolUploadDataType)
	for buff.Len() > 0 {
		header, _ := decodeSymbolUploadDataType(buff, "")
		datatypes[header.Name] = header
	}
	return
}

func decodeSymbolUploadDataType(data *bytes.Buffer, parent string) (header SymbolUploadDataType, err error) {

	result := datatypeEntry{}
	header = SymbolUploadDataType{}

	totalSize := data.Len()

	if totalSize < 48 {
		err = fmt.Errorf(parent, " - Wrong size < 48 byte")
		log.Error().Err(err).Hex("data", data.Bytes()).Msg("error during binary read")
	}

	err = binary.Read(data, binary.LittleEndian, &result)
	if err != nil {
		log.Error().Err(err).Msg("error during binary read")
	}
	name := make([]byte, result.NameLength)
	dt := make([]byte, result.TypeLength)
	comment := make([]byte, result.CommentLength)

	err = binary.Read(data, binary.LittleEndian, name)
	if err != nil {
		log.Error().Err(err).Msg("error during binary read")
	}
	data.Next(1)
	err = binary.Read(data, binary.LittleEndian, dt)
	if err != nil {
		log.Error().Err(err).Msg("error during binary read")
	}
	data.Next(1)
	err = binary.Read(data, binary.LittleEndian, comment)
	if err != nil {
		log.Error().Err(err).Msg("error during binary read")
	}
	data.Next(1)

	header.Name = string(name)
	header.DataType = string(dt)
	header.Comment = string(comment)

	header.DatatypeEntry = result

	if len(header.DataType) > 6 {
		if header.DataType[:6] == "STRING" {
			header.DataType = "STRING"
		}
	}

	childLen := int(result.EntryLength) - (totalSize - data.Len())
	if childLen <= 0 {
		return
	}

	childs := make([]byte, childLen)
	n, err := data.Read(childs)
	if err != nil {
		log.Error().
			Int("read bytes", n).
			Int("expected bytes", childLen).
			Err(err).
			Msg("error reading childs")
	}

	if len(childs) == 0 {
		return
	}

	buff := bytes.NewBuffer(childs)
	if header.Childs == nil {
		header.Childs = map[string]*SymbolUploadDataType{}
	}
	if header.DatatypeEntry.ArrayDim > 0 {
		// Childs is an array
		var result datatypeArrayInfo
		arrayLevels := []datatypeArrayInfo{}

		for i := 0; i < int(header.DatatypeEntry.ArrayDim); i++ {
			err = binary.Read(buff, binary.LittleEndian, &result)
			if err != nil {
				log.Error().
					Err(err).
					Msg("error reading array")
			}
			arrayLevels = append(arrayLevels, result)
		}
		header.Childs = makeArrayChilds(arrayLevels, header.DataType, header.DatatypeEntry.Size)
	} else {
		// Childs is standard variables
		for j := 0; j < (int)(result.SubItems); j++ {
			child, err := decodeSymbolUploadDataType(buff, header.Name)
			if err != nil {
				log.Error().
					Err(err).
					Msg("error reading array")
			}
			header.Childs[child.Name] = &child
		}
	}

	return
}

func makeArrayChilds(levels []datatypeArrayInfo, dt string, size uint32) (childs map[string]*SymbolUploadDataType) {
	childs = map[string]*SymbolUploadDataType{}

	if len(levels) < 1 {
		return
	}

	level := levels[:1][0]
	subChilds := makeArrayChilds(levels[1:], dt, size)

	var offset uint32

	for i := level.LBound; i < level.LBound+level.Elements; i++ {
		name := fmt.Sprint("[", i, "]")

		child := SymbolUploadDataType{}
		child.Name = name
		child.DataType = dt
		child.DatatypeEntry.Offs = offset
		child.DatatypeEntry.Size = size / level.Elements
		child.Childs = subChilds

		//child.Walk("")

		childs[name] = &child
		offset += size / level.Elements
	}

	return
}

// GetJSON (onlyChanged bool) string
func (symbol *Symbol) GetJSON(onlyChanged bool) string {
	data := symbol.parseSymbol(onlyChanged)
	if jsonData, err := json.Marshal(data); err == nil {
		return string(jsonData)
	}
	return ""
}

var openBracketRegex = regexp.MustCompile(`\[`)
var closeBracketRegex = regexp.MustCompile(`\]`)
var stringsList = map[string]struct{}{"STRING": {}, "TIME": {}, "TOD": {}, "DATE": {}, "DT": {}}

// parseSymbol returns JSON interface for symbol
func (symbol *Symbol) parseSymbol(onlyChanged bool) (rData interface{}) {
	if len(symbol.Childs) == 0 {
		if symbol.DataType == "BOOL" {
			rData, _ = strconv.ParseBool(symbol.Value)
		} else if _, ok := stringsList[symbol.DataType]; ok {
			rData = symbol.Value
		} else {
			rData, _ = strconv.ParseFloat(symbol.Value, 64)
		}
	} else {
		localMap := make(map[string]interface{})
		for _, child := range symbol.Childs {
			s := openBracketRegex.ReplaceAllString(child.Name, `"[`)
			s = closeBracketRegex.ReplaceAllString(s, `]"`)
			if onlyChanged {
				if child.Changed {
					localMap[s] = child.parseSymbol(true)
					// child.Changed = false
				}
			} else {
				localMap[s] = child.parseSymbol(false)
			}
		}
		rData = localMap
	}
	return
}

// func (symbol *Symbol) clearChanged() {
// 	for _, localsymbol := range symbol.Childs {
// 		localsymbol.clearChanged()
// 	}
// 	symbol.Changed = false
// }
