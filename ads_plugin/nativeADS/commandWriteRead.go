package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

func (conn *Connection) WriteRead(group uint32, offset uint32, readLength uint32, send []byte) (data []byte, err error) {
	conn.waitGroup.Add(1)
	defer conn.waitGroup.Done()
	request := bytes.NewBuffer([]byte{})
	type writeReadCommandPacket struct {
		Group       uint32
		Offset      uint32
		ReadLength  uint32
		WriteLength uint32
	}
	var content = writeReadCommandPacket{
		group,
		offset,
		readLength,
		uint32(len(send)),
	}

	// Read	- ADS command id: 2
	type readResponse struct {
		Error  ReturnCode
		Length uint32
	}

	err = binary.Write(request, binary.LittleEndian, content)
	binary.Write(request, binary.LittleEndian, send)

	log.Trace().
		Interface("request", request).
		Msgf("Request")

	if err != nil {
		log.Error().
			Err(err).
			Msgf("binary.Write failed: %s", err)
	}

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDReadWrite, request.Bytes())
	if err != nil {
		return
	}

	// Check the result error code
	respBuff := bytes.NewBuffer(resp)
	response := &readResponse{}
	binary.Read(respBuff, binary.LittleEndian, response)
	if response.Error > 0 {
		err = fmt.Errorf("got ADS error number %v in Read", response.Error)
		return
	}
	// data = make([]byte, response.Length)
	// err = binary.Read(respBuff, binary.LittleEndian, data)
	return respBuff.Bytes(), err
}
