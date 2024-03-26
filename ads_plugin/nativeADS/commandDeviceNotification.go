package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const windowsTick int64 = 10000000
const secToUnixEpoch int64 = 11644473600

type NotificationStream struct {
	Length uint32
	Stamps uint32
}
type StampHeader struct {
	Timestamp uint64
	Samples   uint32
}
type NotificationSample struct {
	Handle uint32
	Size   uint32
}

// DeviceNotification - ADS command id: 8
func (conn *Connection) DeviceNotification(ctx context.Context, in []byte) error {
	conn.waitGroup.Add(1)
	defer conn.waitGroup.Done()

	var stream NotificationStream
	var header StampHeader
	var sample NotificationSample
	var content []byte

	data := bytes.NewBuffer(in)

	// Read stream header

	err := binary.Read(data, binary.LittleEndian, &stream)
	if err != nil {
		return fmt.Errorf("unable to read notification %v", err)
	}
	for i := uint32(0); i < stream.Stamps; i++ {
		// Read stamp header
		binary.Read(data, binary.LittleEndian, &header)

		for j := uint32(0); j < header.Samples; j++ {
			err := binary.Read(data, binary.LittleEndian, &sample)
			if err != nil {
				log.Error().
					Err(err).
					Msg("Error during notification read")
				break
			}
			content = make([]byte, sample.Size)
			data.Read(content)
			conn.handleNotification(ctx, sample.Handle, header.Timestamp, content)
		}
	}
	return err
}

func (conn *Connection) handleNotification(ctx context.Context, handle uint32, timestamp uint64, content []byte) error {
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()
	symbol, ok := conn.activeNotifications[handle]
	if !ok {
		log.Error().
			Int("handle", int(handle)).
			Msg("Can't find notification handle")
		return nil
	}
	timeStamp := int64(timestamp)/windowsTick - secToUnixEpoch
	notificationTime := time.Unix(timeStamp, int64(timestamp)%(windowsTick)*100)
	value, err := symbol.parse(content, 0)
	if err != nil {
		log.Error().
			Err(err).
			Msg("error during parse of notification")
		return nil
	}
	symbol.Value = value
	log.Trace().
		Str("update", symbol.Value).
		Msgf("update received")
	updateStruct := &Update{
		Variable:  symbol.FullName,
		Value:     value,
		TimeStamp: notificationTime,
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Try to send the response to the waiting request function
	select {
	case <-ctx.Done():
	case symbol.Notification <- updateStruct:
		log.Debug().
			Msgf("Successfully delivered notification for handle %d", handle)
	}
	return nil
}
