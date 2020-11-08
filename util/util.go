package util

import "encoding/hex"

func RevHashBytes(hash []byte) []byte {
	if len(hash) < 32 {
		return hash
	}
	newHash := make([]byte, 0)
	for i := 28; i >= 0; i -= 4 {
		newHash = append(newHash, hash[i:i+4]...)
	}
	return newHash
}

func RevHash(hash string) string {
	hashBytes, _ := hex.DecodeString(hash)
	return hex.EncodeToString(RevHashBytes(hashBytes))
}

func ReverseByteArray(b []byte) []byte {
	for i := len(b)/2 - 1; i >= 0; i-- {
		opp := len(b) - 1 - i
		b[i], b[opp] = b[opp], b[i]
	}
	return b
}
