package util

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gertjaap/p2proxy/logging"
)

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

var jsonClient = &http.Client{Timeout: 60 * time.Second}

func GetJson(url string, target interface{}) error {
	r, err := jsonClient.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

func PostJson(url string, payload interface{}, target interface{}) error {
	var b bytes.Buffer
	json.NewEncoder(&b).Encode(payload)
	r, err := jsonClient.Post(url, "application/json", bytes.NewBuffer(b.Bytes()))
	if err != nil {
		return err
	}
	defer r.Body.Close()

	bodyBytes, err := ioutil.ReadAll(r.Body)
	logging.Infof("POST JSON response: %s", string(bodyBytes))

	buf := bytes.NewBuffer(bodyBytes)
	return json.NewDecoder(buf).Decode(target)
}
