package config

import (
	"encoding/json"
	"io/ioutil"
)

type MinerConfig struct {
	UpstreamStratumHost     string `json:"upstreamStratumHost"`
	UpstreamStratumUser     string `json:"upstreamStratumUser"`
	UpstreamStratumPassword string `json:"upstreamStratumPassword"`
	StratumPort             int    `json:"port"`
}

func GetConfig() (MinerConfig, error) {
	var m MinerConfig
	b, err := ioutil.ReadFile("./p2proxy.json")
	if err != nil {
		return m, err
	}

	err = json.Unmarshal(b, &m)
	if err != nil {
		return m, err
	}
	return m, nil
}
