package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

func (conn *Connection) Read(group uint32, offset uint32, length uint32) (data []byte, err error) {
	conn.waitGroup.Add(1)
	defer conn.waitGroup.Done()
	request := bytes.NewBuffer([]byte{})
	type readCommandPacket struct {
		Group  uint32
		Offset uint32
		Length uint32
	}
	var content = readCommandPacket{
		group,
		offset,
		length,
	}

	// Read	- ADS command id: 2
	err = binary.Write(request, binary.LittleEndian, content)

	log.Trace().
		Interface("request", content).
		Msgf("Request")

	if err != nil {
		log.Error().
			Msgf("binary.Write failed: %s", err)
		return nil, err
	}

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDRead, request.Bytes())
	if err != nil {
		log.Error().
			Err(err).
			Msgf("send request failed: %s", err)
		return
	}

	// Check the result error code
	type readResponse struct {
		Error  ReturnCode
		Length uint32
	}
	respBuff := bytes.NewBuffer(resp)
	response := &readResponse{}
	err = binary.Read(respBuff, binary.LittleEndian, response)
	if err != nil {
		return
	}
	if response.Error > 0 {
		err = fmt.Errorf("got ADS error number %v in Read", response.Error)
		return
	}
	// data = make([]byte, response.Length)
	// err = binary.Read(respBuff, binary.LittleEndian, data)
	return respBuff.Bytes(), nil
}
