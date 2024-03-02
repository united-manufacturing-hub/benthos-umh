package ads

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

type amsTCPHeader struct {
	Unknown1 uint8
	System   uint8
	Length   uint32
}

type amsHeader struct {
	Target    AmsAddress
	Source    AmsAddress
	Command   CommandID
	State     uint16
	Length    uint32
	ErrorCode uint32
	InvokeID  uint32
}

func stringToNetID(source string) (result [6]byte) {
	splitLocalhost := strings.Split(source, ".")

	for i, a := range splitLocalhost {
		value, _ := strconv.ParseUint(a, 10, 8)
		result[i] = byte(value)
	}
	return
}

func (conn *Connection) encode(command CommandID, data []byte, invokeID uint32) ([]byte, error) {
	log.Trace().
		Interface("command", command).
		Interface("target", conn.target).
		Interface("source", conn.source).
		Uint32("ID", invokeID).
		Int("length of data", len(data)).
		Msg("Starting encoding of AMS header")
	tcpHeader := &amsTCPHeader{
		0,
		0,
		uint32(32 + len(data)),
	}
	header := &amsHeader{
		conn.target,
		conn.source,
		command,
		uint16(4),
		uint32(len(data)),
		uint32(0),
		invokeID,
	}

	buff := &bytes.Buffer{}
	err := binary.Write(buff, binary.LittleEndian, tcpHeader)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.LittleEndian, header)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.LittleEndian, data)
	log.Trace().
		Bytes("data", data).
		Msg("data to transmit")
	if err != nil {
		log.Error().
			Err(err).
			Msg("binary.Write failed: %s")
		return nil, err
	}

	log.Trace().
		Hex("bytes", buff.Bytes()).
		Msg("The encoded AMS header:")

	return buff.Bytes(), nil
}
