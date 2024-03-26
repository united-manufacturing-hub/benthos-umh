package ads

import (
	"bytes"
	"encoding/binary"

	"github.com/rs/zerolog/log"
)

// ReadStateResponse - ADS command id: 4
type states struct {
	AdsState    AdsState
	DeviceState uint16
}

func (conn *Connection) ReadState() (response states, err error) {
	conn.waitGroup.Add(1)
	defer conn.waitGroup.Done()
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDReadState, []byte{})
	log.Trace().
		Bytes("data", resp).
		Msg("response from plc for state")
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error during read state")
		return
	}
	type readStateResponse struct {
		Error ReturnCode
		states
	}
	stateResponse := &readStateResponse{}
	buff := bytes.NewBuffer(resp)
	binary.Read(buff, binary.LittleEndian, stateResponse)
	log.Debug().
		Interface("AdsState", stateResponse.AdsState).
		Interface("DeviceState", stateResponse.DeviceState).
		Msg("response.ADSState")

	return stateResponse.states, err
}
