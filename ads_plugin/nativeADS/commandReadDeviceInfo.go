package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

// ReadDeviceInfoResponse connected device info

// DeviceInfo connected device info
type DeviceInfo struct {
	Major      uint8
	Minor      uint8
	Version    uint16
	DeviceName [16]byte
}

func (conn *Connection) ReadDeviceInfo() (response DeviceInfo, err error) {
	conn.waitGroup.Add(1)
	defer conn.waitGroup.Done()
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDReadDeviceInfo, []byte{})
	if err != nil {
		return
	}

	// Check the response length
	if len(resp) != 24 {
		return response, fmt.Errorf("wrong length of response! Got %d bytes and it should be 24", len(resp))
	}
	type readDeviceInfoResponse struct {
		Error      ReturnCode
		DeviceInfo DeviceInfo
	}
	respBuffer := bytes.NewBuffer(resp)
	deviceInfoResponse := readDeviceInfoResponse{}
	binary.Read(respBuffer, binary.LittleEndian, deviceInfoResponse)
	if deviceInfoResponse.Error > 0 {
		err = fmt.Errorf("got ADS error number %d in ReadDeviceInfo", deviceInfoResponse.Error)
		return
	}

	log.Debug().
		Msg("Device Info")

	return deviceInfoResponse.DeviceInfo, err
}
