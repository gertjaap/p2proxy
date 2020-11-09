package networks

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Network struct {
	Base58P2PKHVersion byte   `json:"base58P2PKHVersion"`
	Base58P2SHVersion  byte   `json:"base58P2SHVersion"`
	InsightURL         string `json:"insightUrl"`
	Bech32Prefix       string `json:"bech32Prefix"`
}

func GetNetwork(network string) (Network, error) {
	var n Network
	b, err := ioutil.ReadFile(fmt.Sprintf("networks/%s.json", network))
	if err != nil {
		return n, err
	}

	err = json.Unmarshal(b, &n)
	return n, err
}
