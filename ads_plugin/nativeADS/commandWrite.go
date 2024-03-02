package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

// Write - ADS command id: 3
func (conn *Connection) Write(group uint32, offset uint32, data []byte) error {
	conn.waitGroup.Add(1)
	defer conn.waitGroup.Done()
	type writeCommandPacket struct {
		Group  uint32
		Offset uint32
		Length uint32
	}
	request := new(bytes.Buffer)
	writeRequest := writeCommandPacket{
		group,
		offset,
		uint32(binary.Size(data)),
	}

	err := binary.Write(request, binary.LittleEndian, writeRequest)
	if err != nil {
		log.Error().
			Msgf("binary.Write failed: %s", err)
		return err
	}
	binary.Write(request, binary.LittleEndian, data)
	if err != nil {
		log.Error().
			Msgf("binary.Write failed: %s", err)
		return err
	}

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDWrite, request.Bytes())
	if err != nil {
		log.Error().
			Err(err).
			Msg("error during send request for write")
		return err
	}
	respBuffer := bytes.NewBuffer(resp)
	var respCode ReturnCode
	// Check the result error code
	err = binary.Read(respBuffer, binary.LittleEndian, &respCode)
	if respCode > 0 {
		log.Error().
			Err(err).
			Msg("error during write")
		err = fmt.Errorf("got ADS error number %v in Write", respCode)
		return err
	}

	return nil
}
