package opcua_plugin

import (
	"crypto/rand"
	"github.com/redpanda-data/benthos/v4/public/service"
	"math/big"
)

func randomString(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		randInt, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		result[i] = letters[randInt.Int64()]
	}
	return string(result)
}

// removeUnordered removes the element at index i from s without preserving the order.
func removeUnordered(s service.MessageBatch, i int) service.MessageBatch {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}
