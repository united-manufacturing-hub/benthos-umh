package plugin

import (
	"crypto/rand"
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
