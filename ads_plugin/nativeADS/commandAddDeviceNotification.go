package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

func (conn *Connection) AddDeviceNotification(
	group uint32,
	offset uint32,
	length uint32,
	transmissionMode TransMode,
	maxDelay time.Duration,
	cycleTime time.Duration) (handle uint32, err error) {
	conn.waitGroup.Add(1)
	defer conn.waitGroup.Done()
	request := new(bytes.Buffer)
	type addDeviceNotificationCommandPacket struct {
		Group            uint32
		Offset           uint32
		Length           uint32
		TransmissionMode uint32
		MaxDelay         uint32
		CycleTime        uint32
		Reserved         [16]byte
	}

	var content = addDeviceNotificationCommandPacket{
		group,
		offset,
		length,
		uint32(transmissionMode),
		uint32(maxDelay.Nanoseconds() / 100),  // 1 = 1ms (alt 100ns?)
		uint32(cycleTime.Nanoseconds() / 100), // 1 = 1ms
		[16]byte{},
	}
	err = binary.Write(request, binary.LittleEndian, content)
	if err != nil {
		log.Error().
			Msgf("binary.Write failed: %v", err)
	}
	type addDeviceNotificationResponse struct {
		Error  ReturnCode
		Handle uint32
	}
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDAddDeviceNotification, request.Bytes())
	if err != nil {
		return
	}
	respBuffer := bytes.NewBuffer(resp)
	notificationResponse := addDeviceNotificationResponse{}
	err = binary.Read(respBuffer, binary.LittleEndian, &notificationResponse)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Added notification handler, FAILED: ")
		return 0, err
	}
	if notificationResponse.Error != 0 {
		log.Error().
			Uint32("Error Number", uint32(notificationResponse.Error)).
			Msg("Added notification handler, FAILED: ")
		return 0, fmt.Errorf("unable to create notification worker %v", err)
	}
	log.Trace().
		Uint32("handle", handle).
		Msg("Added notification handler: ")

	return notificationResponse.Handle, nil
}
