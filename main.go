package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/gertjaap/p2proxy/config"
	"github.com/gertjaap/p2proxy/logging"
	"github.com/gertjaap/p2proxy/stratum"
	"github.com/gertjaap/p2proxy/util"
	"github.com/gertjaap/verthash-go"
)

var clients = sync.Map{}
var genesisDiff *big.Int

type StratumClient struct {
	ID                     int32
	conn                   *stratum.StratumConnection
	Difficulty             float64
	VarDiff                float64
	Username               string
	ExtraNonce1            []byte
	ExtraNonce2Size        int8
	Target                 []byte
	SubscribedToExtraNonce bool
	Subscribed             bool
	Authorized             bool
	CurrentJob             []interface{}
}

var nextClientID int32
var upstreamClient *stratum.StratumConnection
var upstreamConnected bool
var upstreamExtraNonce1 []byte
var upstreamExtraNonce2Size int8
var upstreamDiff float64
var upstreamJob []interface{}
var nextUpstreamMessageID int32

var vh *verthash.Verthash

var cfg config.MinerConfig
var msgQueue chan stratum.StratumMessage

func main() {
	var err error

	logging.SetLogLevel(int(logging.LogLevelDebug))

	msgQueue = make(chan stratum.StratumMessage, 1000)
	upstreamJob = []interface{}{}

	cfg, err = config.GetConfig()
	if err != nil {
		logging.Warnf("Could not find config. Make sure it exists in the executable path (solominer.json)\n")
		os.Exit(-1)
	}

	vh, err = verthash.NewVerthash("verthash.dat", true)
	if err != nil {
		panic(err)
	}

	srv, err := stratum.NewStratumListener(cfg.StratumPort)
	if err != nil {
		panic(err)
	}

	nextUpstreamMessageID = 4
	upstreamClient, err = stratum.NewStratumClient(cfg.UpstreamStratumHost)
	if err != nil {
		panic(err)
	}

	go processUpstream()
	go processUpstreamQueue()

	for {
		for !upstreamConnected || len(upstreamJob) == 0 {
			time.Sleep(time.Millisecond * 250)
		}

		conn, err := srv.Accept()

		clientID := atomic.AddInt32(&nextClientID, 1)

		if err != nil {
			panic(err)
		}
		clientExtraNonceSuffix := make([]byte, 4)
		binary.BigEndian.PutUint32(clientExtraNonceSuffix, uint32(clientID))
		clientExtraNonceSuffix = clientExtraNonceSuffix[1:]
		clientExtraNonce1 := append(upstreamExtraNonce1, clientExtraNonceSuffix...)
		clt := StratumClient{
			ID:                     clientID,
			conn:                   conn,
			Difficulty:             -1,
			VarDiff:                0.5,
			ExtraNonce2Size:        upstreamExtraNonce2Size - 3,
			ExtraNonce1:            clientExtraNonce1,
			SubscribedToExtraNonce: false,
		}

		clients.Store(clientID, &clt)
		//configureLogOutput(clt.conn, fmt.Sprintf("CLT %03d", clt.ID))

		go serveClient(&clt)
	}
}

func configureLogOutput(c *stratum.StratumConnection, prefix string) {
	c.LogOutput = func(ces []stratum.CommEvent) {
		for _, ce := range ces {
			dir := ">"
			if ce.In {
				dir = "<"
			}
			logging.Infof("[%s] %s %s\n", prefix, dir, ce.Message.String())
		}
	}
}

func configureUpstream() {
	//configureLogOutput(upstreamClient, "UPSTRM")

	upstreamClient.Outgoing <- stratum.StratumMessage{
		MessageID:    1,
		RemoteMethod: "mining.subscribe",
		Parameters:   []string{"Miner/1.0"},
	}

	upstreamClient.Outgoing <- stratum.StratumMessage{
		MessageID:    2,
		RemoteMethod: "mining.authorize",
		Parameters: []string{
			cfg.UpstreamStratumUser,
			cfg.UpstreamStratumPassword,
		},
	}
}

func processUpstreamQueue() {
	for msg := range msgQueue {
		for !upstreamConnected {
			time.Sleep(time.Millisecond * 250)
		}
		upstreamClient.Outgoing <- msg
	}
}

func processUpstream() {
	logging.Infof("Upstream stratum connected\n")

	configureUpstream()

	for {
		close := false
		select {
		case msg := <-upstreamClient.Incoming:
			processUpstreamStratumMessage(msg)
		case <-upstreamClient.Disconnected:
			logging.Infof("Upstream stratum disconnected, reconnecting")
			close = true
		}

		if close {
			upstreamConnected = false
			upstreamJob = []interface{}{}
			upstreamClient.Stop()
			go func() {
				var err error
				upstreamClient, err = stratum.NewStratumClient(cfg.UpstreamStratumHost)
				if err != nil {
					panic(err)
				}
				go processUpstream()
			}()
			break
		}
	}

}

func serveClient(client *StratumClient) {
	logging.Infof("New stratum client connected: %d", client.ID)
	for {
		close := false
		select {
		case msg := <-client.conn.Incoming:
			processStratumMessage(client, msg)
		case <-client.conn.Disconnected:
			logging.Infof("Stratum client %d disconnected", client.ID)
			close = true
		}

		if close {
			clients.Delete(client.ID)
			break
		}
	}
	client.conn.Stop()
}

func (client *StratumClient) SendWork() {
	if !upstreamConnected || len(upstreamJob) == 0 {
		return
	}
	if !client.Authorized || !client.Subscribed {
		return
	}

	client.SendDifficulty()
	client.SendExtraNonce()

	client.conn.Outgoing <- stratum.StratumMessage{
		RemoteMethod: "mining.notify",
		Parameters:   upstreamJob,
	}

	client.CurrentJob = make([]interface{}, len(upstreamJob))
	copy(client.CurrentJob, upstreamJob)
}

func (client *StratumClient) SendDifficulty() {
	if !client.Authorized || !client.Subscribed {
		return
	}

	clientDiff := upstreamDiff * client.VarDiff
	if client.Difficulty != clientDiff {
		client.conn.Outgoing <- stratum.StratumMessage{
			RemoteMethod: "mining.set_difficulty",
			Parameters:   []interface{}{clientDiff},
		}
		client.Difficulty = clientDiff
	}
}
func (client *StratumClient) SendExtraNonce() {
	if !client.Authorized || !client.Subscribed || !client.SubscribedToExtraNonce {
		return
	}
	clientExtraNonceSuffix := make([]byte, 4)
	binary.BigEndian.PutUint32(clientExtraNonceSuffix, uint32(client.ID))
	clientExtraNonceSuffix = clientExtraNonceSuffix[1:]
	clientExtraNonce1 := append(upstreamExtraNonce1, clientExtraNonceSuffix...)
	clientExtraNonce2Size := upstreamExtraNonce2Size - 3

	if !bytes.Equal(client.ExtraNonce1, clientExtraNonce1) || client.ExtraNonce2Size != clientExtraNonce2Size {
		client.conn.Outgoing <- stratum.StratumMessage{
			RemoteMethod: "mining.set_extranonce",
			Parameters: []interface{}{
				fmt.Sprintf("%x", clientExtraNonce1),
				clientExtraNonce2Size,
			},
		}
		client.ExtraNonce1 = clientExtraNonce1
		client.ExtraNonce2Size = clientExtraNonce2Size
	}
}

func processStratumMessage(client *StratumClient, msg stratum.StratumMessage) {
	err := msg.Error
	if err != nil {
		logging.Errorf("Error response received: %v\n", err)
	}

	switch msg.RemoteMethod {
	case "mining.authorize":
		params := msg.Parameters.([]interface{})
		client.Username = params[0].(string)

		client.conn.Outgoing <- stratum.StratumMessage{
			MessageID: msg.Id(),
			Result:    true,
		}
		client.Authorized = true
		client.SendWork()
	case "mining.extranonce.subscribe":
		client.SubscribedToExtraNonce = true
		client.conn.Outgoing <- stratum.StratumMessage{
			MessageID: msg.Id(),
			Result:    true,
		}
		client.conn.Outgoing <- stratum.StratumMessage{
			RemoteMethod: "mining.set_extranonce",
			Parameters: []interface{}{
				fmt.Sprintf("%x", client.ExtraNonce1),
				client.ExtraNonce2Size,
			},
		}

	case "mining.subscribe":
		b := make([]byte, 8)
		rand.Read(b)
		clientID := fmt.Sprintf("%x", b)

		client.conn.Outgoing <- stratum.StratumMessage{
			MessageID: msg.Id(),
			Result: []interface{}{
				[]string{"mining.notify", clientID},
				fmt.Sprintf("%x", client.ExtraNonce1),
				client.ExtraNonce2Size,
			},
		}
		client.Subscribed = true
		client.SendWork()
	case "mining.configure":
		client.conn.Outgoing <- stratum.StratumMessage{
			MessageID: msg.Id(),
			Result:    nil,
		}
	case "mining.get_transactions":
		client.conn.Outgoing <- stratum.StratumMessage{
			MessageID: msg.Id(),
			Result:    []interface{}{},
		}
	case "mining.submit":
		var err error

		params := msg.Parameters.([]interface{})

		en2, err := hex.DecodeString(params[2].(string))
		if err != nil {
			logging.Errorf("Error parsing extranonce2: %s", err.Error())
		}

		nonceBytes, _ := hex.DecodeString(params[4].(string))
		nonce := binary.BigEndian.Uint32(nonceBytes)
		timeint, _ := strconv.ParseInt(params[3].(string), 16, 64)
		timestamp := uint32(timeint)
		currentCoinbase1, _ := hex.DecodeString(client.CurrentJob[2].(string))
		currentCoinbase2, _ := hex.DecodeString(client.CurrentJob[3].(string))
		prevBlockBytes, _ := hex.DecodeString(client.CurrentJob[1].(string))
		prevBlockBytes = util.RevHashBytes(prevBlockBytes)
		prevBlockHash, _ := chainhash.NewHashFromStr(hex.EncodeToString(prevBlockBytes))

		versionBytes, _ := hex.DecodeString(client.CurrentJob[5].(string))
		version := binary.BigEndian.Uint32(versionBytes)

		bitsBytes, _ := hex.DecodeString(client.CurrentJob[6].(string))
		bits := binary.BigEndian.Uint32(bitsBytes)

		//logging.Infof("Share submit:\n\n  Time: [%d]\n  Nonce: [%x / %d]\n  prev: [%s]\n  bits: [%x / %d]\n  version: [%x / %d]", timestamp, nonceBytes, nonce, prevBlockHash.String(), bitsBytes, bits, versionBytes, version)

		coinbaseTx := wire.NewMsgTx(wire.TxVersion)
		coinbaseBytes := make([]byte, len(currentCoinbase1)+len(currentCoinbase2)+8)
		copy(coinbaseBytes, currentCoinbase1)
		copy(coinbaseBytes[len(currentCoinbase1):], client.ExtraNonce1)
		copy(coinbaseBytes[len(currentCoinbase1)+len(client.ExtraNonce1):], en2)
		copy(coinbaseBytes[len(currentCoinbase1)+len(client.ExtraNonce1)+len(en2):], currentCoinbase2)

		err = coinbaseTx.Deserialize(bytes.NewReader(coinbaseBytes))
		if err != nil {
			logging.Errorf("Error deserializing TX: %s", err.Error())
		}
		h := coinbaseTx.TxHash()
		merkles := client.CurrentJob[4].([]interface{})

		merkleRoot := &h
		for _, m := range merkles {
			merkleHash, _ := chainhash.NewHashFromStr(m.(string))
			merkleRoot = blockchain.HashMerkleBranches(merkleRoot, merkleHash)
		}

		hdr := &wire.BlockHeader{
			Nonce:      nonce,
			Timestamp:  time.Unix(int64(timestamp), 0),
			MerkleRoot: *merkleRoot,
			PrevBlock:  *prevBlockHash,
			Bits:       bits,
			Version:    int32(version),
		}

		var headerBuf bytes.Buffer
		hdr.Serialize(&headerBuf)
		powHash, _ := vh.SumVerthash(headerBuf.Bytes())
		upstreamTarget := diffToTarget(upstreamDiff)
		shareTarget := diffToTarget(upstreamDiff * client.VarDiff)
		ch, _ := chainhash.NewHash(powHash[:])
		bnHash := blockchain.HashToBig(ch)
		off := bnHash.Cmp(shareTarget)
		if off == -1 {
			client.conn.Outgoing <- stratum.StratumMessage{
				MessageID: msg.Id(),
				Result:    true,
			}
			logging.Infof("Client %d submitted valid share\n\nHash   : %x\nTarget : %x\n", client.ID, padTo32(bnHash.Bytes()), padTo32(shareTarget.Bytes()))

		} else {
			client.conn.Outgoing <- stratum.StratumMessage{
				MessageID: msg.Id(),
				Result:    false,
			}
			logging.Infof("Client %d submitted invalid share\n\nHash   : %x\nTarget : %x\n", client.ID, padTo32(bnHash.Bytes()), padTo32(shareTarget.Bytes()))

		}

		off = bnHash.Cmp(upstreamTarget)
		if off == -1 {
			logging.Infof("Client %d submitted valid upstream share\n\nHash   : %x\nTarget : %x\n", client.ID, padTo32(bnHash.Bytes()), padTo32(upstreamTarget.Bytes()))

			// Submit upstream
			extraNoncePrefix := make([]byte, 4)
			binary.BigEndian.PutUint32(extraNoncePrefix, uint32(client.ID))
			extraNoncePrefix = extraNoncePrefix[1:]
			msg.MessageID = atomic.AddInt32(&nextUpstreamMessageID, 1)

			msg.Parameters = []interface{}{
				cfg.UpstreamStratumUser,
				params[1].(string),
				hex.EncodeToString(append(extraNoncePrefix, en2...)),
				params[3].(string),
				params[4].(string),
			}

			logging.Infof("Submitting share upstream: %s", msg.String())
			msgQueue <- msg
		}
	default:
		logging.Warnf("Received unknown message [%s]\n", msg.RemoteMethod)
	}
}

func processUpstreamStratumMessage(msg stratum.StratumMessage) {
	err := msg.Error
	if err != nil {
		logging.Warnf("Error response received: %v\n", err)
	}

	switch msg.Id() {
	case 1:
		if err == nil {
			processUpstreamSubscriptionResponse(msg)
		}
	case 2:
		if err == nil {
			resultBool, ok := msg.Result.(bool)
			if !(ok && !resultBool) {
				logging.Infof("Succesfully authorized\n")
				upstreamConnected = true
			} else {

				logging.Errorf("Upstream stratum authorization failed: %b %b %v", ok, resultBool, msg.Error)
			}
		}
	default:
		ok := processUpstreamRemoteInstruction(msg)
		if !ok && msg.Id() >= 4 {
			// Response to a submitted share
			result, ok := msg.Result.(bool)
			if result && ok {
				logging.Info("Share accepted\n")
			} else {
				if !result {
					logging.Info("Share declined\n")
				} else {
					logging.Info("Incorrect response to mining.submit\n")
				}
			}
		}
	}
}

func processUpstreamSubscriptionResponse(msg stratum.StratumMessage) {
	arr, ok := msg.Result.([]interface{})
	if !ok {
		logging.Warnf("Result of subscription response is not an []interface{}\n")
		return
	}

	if len(arr) > 2 {
		logging.Infof("Setting extranonce1 [%s], extranonce2_size: [%f] (from subscription response)\n", arr[1].(string), arr[2].(float64))
		upstreamExtraNonce1, _ = hex.DecodeString(arr[1].(string))
		upstreamExtraNonce2Size = int8(arr[2].(float64))
	}
}

func UpdateDifficultyDownstream() {
	clients.Range(func(i, c interface{}) bool {
		clt := c.(*StratumClient)
		clt.SendDifficulty()
		return true
	})
}

func UpdateExtraNonceDownstream() {
	clients.Range(func(i, c interface{}) bool {
		clt := c.(*StratumClient)
		clt.SendExtraNonce()
		return true
	})
}

func UpdateJobDownstream() {
	clients.Range(func(i, c interface{}) bool {
		clt := c.(*StratumClient)
		clt.SendWork()
		return true
	})
}

func processUpstreamRemoteInstruction(msg stratum.StratumMessage) bool {
	switch msg.RemoteMethod {
	case "mining.set_difficulty":
		// Adjusted difficulty
		params, ok := msg.Parameters.([]interface{})
		upstreamDiff, ok = params[0].(float64)
		if ok {
			logging.Infof("New difficulty received: %f", upstreamDiff)
		} else {
			logging.Errorf("Could not determine difficulty from stratum: [%v]\n", params[0])
		}
		go UpdateDifficultyDownstream()
	case "mining.set_extranonce":
		// Adjusted extranonce
		upstreamExtraNonce1, _ := hex.DecodeString(msg.Parameters.([]interface{})[0].(string))
		upstreamExtraNonce2Size = int8(msg.Parameters.([]interface{})[1].(float64))

		logging.Infof("Setting extranonce1 [%x], extranonce2_size: [%d] (from set_extranonce)\n", upstreamExtraNonce1, upstreamExtraNonce2Size)
		go UpdateExtraNonceDownstream()
	case "mining.notify":
		logging.Infof("Received new job from upstream")
		upstreamJob = msg.Parameters.([]interface{})
		go UpdateJobDownstream()
	default:
		return false
	}
	return true
}

func init() {
	genesisDiff = blockchain.CompactToBig(0x1e00ffff)
}

func targetToDiff(target *big.Int) float64 {
	f, _ := big.NewFloat(0).Quo(big.NewFloat(0).SetInt(genesisDiff), big.NewFloat(0).SetInt(target)).Float64()
	return f
}

func diffToTarget(diff float64) *big.Int {
	t := new(big.Int)
	big.NewFloat(0).Quo(big.NewFloat(0).SetInt(genesisDiff), big.NewFloat(diff)).Int(t)
	return t
}

func padTo32(b []byte) []byte {
	b2 := make([]byte, 32)
	copy(b2[32-len(b):], b)
	return b2
}
