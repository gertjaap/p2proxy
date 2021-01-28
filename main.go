package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/base58"
	"github.com/gertjaap/p2proxy/logging"
	"github.com/gertjaap/p2proxy/networks"
	"github.com/gertjaap/p2proxy/stratum"
	"github.com/gertjaap/p2proxy/util"
	"github.com/gertjaap/p2proxy/web"
	verthash "github.com/gertjaap/verthash-go"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mit-dci/lit/bech32"
)

const MaxShareCacheLength = 10000

var clients = sync.Map{}
var genesisDiff *big.Int
var shareCache = [][]byte{}
var shareCacheLock = sync.Mutex{}
var db *sql.DB
var srv *web.Server
var rpc *rpcclient.Client
var diffCache = map[string]float64{}
var diffCacheLock = sync.Mutex{}

type Share struct {
	shareTarget    *big.Int
	blockTarget    *big.Int
	upstreamTarget *big.Int
	height         int
	address        string
}

type Utxo struct {
	TxID          string `json:"txid"`
	Vout          uint   `json:"vout"`
	ScriptPubKey  string `json:"scriptPubKey"`
	Amount        uint64 `json:"satoshis"`
	Height        uint   `json:"height"`
	Confirmations uint   `json:"confirmations"`
	IsCoinbase    bool
	Spent         bool
}

type StratumClient struct {
	ID                     int32
	conn                   *stratum.StratumConnection
	Difficulty             float64
	VarDiff                float64
	Username               string
	ExtraNonce1            []byte
	ExtraNonce2Size        int8
	SubscribedToExtraNonce bool
	Subscribed             bool
	Authorized             bool
	StartShareCount        time.Time
	StartFailShareCount    time.Time
	ShareCount             int
	FailedShareCount       int
	CurrentJob             []interface{}
}

type UpstreamStatus struct {
	SocketConnected              bool
	Authorized                   bool
	SubscriptionResponseReceived bool
	JobReceived                  bool
	DifficultyReceived           bool
}

func (u UpstreamStatus) Ready() bool {
	return u.SocketConnected && u.Authorized && u.SubscriptionResponseReceived && u.JobReceived && u.DifficultyReceived
}

var nextClientID int32
var upstreamClient *stratum.StratumConnection
var upstreamStatus UpstreamStatus
var upstreamExtraNonce1 []byte
var upstreamExtraNonce2Size int8
var upstreamDiff float64
var upstreamJob []interface{}
var upstreamStratum string
var upstreamSubmitted int64
var upstreamDumped int
var nextUpstreamMessageID int32
var unpaidShares = map[string]int64{}
var upstreamDeclinedShares int64
var vh *verthash.Verthash

var msgQueue chan stratum.StratumMessage
var shareProcessQueue chan Share
var updateDownstreamJobsLock = sync.Mutex{}
var connectionLock = sync.Mutex{}
var network networks.Network
var priv *btcec.PrivateKey
var pub *btcec.PublicKey
var pkh []byte
var myAddress string

func main() {
	var err error

	logging.SetLogLevel(int(logging.LogLevelDebug))

	network, err = networks.GetNetwork(os.Getenv("NETWORK"))
	if err != nil {
		panic(err)
	}

	privKeyString := os.Getenv("WALLET_PRIVATEKEY")
	if len(privKeyString) != 64 {
		panic("Wallet not set")
	}
	privKeyBytes, _ := hex.DecodeString(privKeyString)
	// Derive pubkey
	priv, pub = btcec.PrivKeyFromBytes(btcec.S256(), privKeyBytes)
	pkh = btcutil.Hash160(pub.SerializeCompressed())
	myAddress = base58.CheckEncode(pkh, network.Base58P2PKHVersion)

	logging.Debugf("P2Proxy's wallet address: %s\n", myAddress)

	connCfg := &rpcclient.ConnConfig{
		Host:         os.Getenv("RPCHOST"),
		User:         os.Getenv("RPCUSER"),
		Pass:         os.Getenv("RPCPASS"),
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	rpc, err = rpcclient.New(connCfg, nil)
	if err != nil {
		panic(err)
	}

	// Ensure wallet is imported
	rpc.ImportAddressRescan(myAddress, "", false)

	err = openDatabase()
	if err != nil {
		panic(err)
	}

	shareProcessQueue = make(chan Share, 1000)
	go processShares()
	go processPayouts()

	msgQueue = make(chan stratum.StratumMessage, 1000)
	upstreamJob = []interface{}{}

	rows, _ := db.Query("SELECT address, SUM(value) FROM unpaid_shares GROUP BY address")
	var address string
	var value int64
	for rows.Next() {
		rows.Scan(&address, &value)
		unpaidShares[address] = value
	}

	logging.Infof("Loaded outstanding balances:\r\n")
	for k, v := range unpaidShares {
		logging.Infof("%s : %d\n", k, v)
	}

	if !(os.Getenv("SKIPVERTHASHVERIFY") == "1") {
		logging.Infof("Verifying Verthash file, this can take a few moments...")
		err = verthash.EnsureVerthashDatafile("verthash.dat")
		if err != nil {
			panic(err)
		}
	}

	vh, err = verthash.NewVerthash("verthash.dat", true)
	if err != nil {
		panic(err)
	}

	port := os.Getenv("STRATUMPORT")
	if port == "" {
		port = "9171"
	}

	portInt, err := strconv.Atoi(port)
	if err != nil {
		panic(err)
	}

	stratumSrv, err := stratum.NewStratumListener(portInt)
	if err != nil {
		panic(err)
	}

	upstreamStratum = os.Getenv("UPSTREAM_STRATUM")
	if upstreamStratum == "" {
		panic("Upstream stratum host not set")
	}

	nextUpstreamMessageID = 4

	go processUpstreamQueue()

	srv = web.StartServer()
	srv.UnpaidShares = unpaidShares
	for {
		conn, err := stratumSrv.Accept()

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
			VarDiff:                1,
			ExtraNonce2Size:        -1,
			ExtraNonce1:            clientExtraNonce1,
			StartShareCount:        time.Now(),
			StartFailShareCount:    time.Now(),
			SubscribedToExtraNonce: false,
		}

		clients.Store(clientID, &clt)
		//configureLogOutput(clt.conn, fmt.Sprintf("CLT %03d", clt.ID))

		reconnectUpstream()

		clt.ExtraNonce2Size = upstreamExtraNonce2Size - 3

		go serveClient(&clt)

	}
}

func disconnectUpstream() {
	logging.Infof("Ensuring upstream is disconnected")
	connectionLock.Lock()
	upstreamStatus = UpstreamStatus{}
	upstreamJob = []interface{}{}
	upstreamClient.Stop()
	connectionLock.Unlock()
}

func reconnectUpstream() {
	var err error
	logging.Infof("Checking if we need to reconnect to upstream")

	if !upstreamStatus.SocketConnected {
		logging.Infof("Looks like we need to reconnect to upstream")
		connectionLock.Lock()
		if !upstreamStatus.SocketConnected {
			logging.Infof("Reconnecting to upstream")
			upstreamClient, err = stratum.NewStratumClient(upstreamStratum)
			if err != nil {
				panic(err)
			}
			upstreamStatus.SocketConnected = true
			go processUpstream()
			logging.Infof("Waiting for connection to be active")
		} else {
			logging.Infof("Looks like another thread already initiated reconnection")
		}
		for !upstreamStatus.Ready() {
			time.Sleep(time.Millisecond * 250)
			if !upstreamStatus.SocketConnected {
				logging.Infof("Connection failed")
				connectionLock.Unlock()
				disconnectUpstream()
				go reconnectUpstream()
			}
		}
		logging.Infof("Connected")
		connectionLock.Unlock()
		return
	}
	for !upstreamStatus.Ready() {
		time.Sleep(time.Millisecond * 250)
	}
	logging.Infof("Connection (already) active")
}

func openDatabase() error {
	var err error

	db, err = sql.Open("sqlite3", "./p2proxy.db")
	if err != nil {
		return err
	}
	statement, err := db.Prepare("CREATE TABLE IF NOT EXISTS unpaid_shares (time int, address text, value bigint, block_target text, share_target text, upstream_target text, reward_share float, subsidy int)")
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	statement, err = db.Prepare("CREATE TABLE IF NOT EXISTS paid_shares (time int, address text, value bigint, block_target text, share_target text, upstream_target text, reward_share float, subsidy int)")
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	statement, err = db.Prepare("CREATE INDEX IF NOT EXISTS unpaid_shares_idx1 ON unpaid_shares(time)")
	if err != nil {
		return err
	}
	statement.Exec()
	if err != nil {
		return err
	}
	statement, err = db.Prepare("CREATE INDEX IF NOT EXISTS unpaid_shares_idx2 ON unpaid_shares(time, address)")
	if err != nil {
		return err
	}
	statement.Exec()
	if err != nil {
		return err
	}

	statement, err = db.Prepare("CREATE TABLE IF NOT EXISTS coinbase_tx (txid text primary key, coinbase bit)")
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}

	statement, err = db.Prepare("CREATE TABLE IF NOT EXISTS utxo_spent (outpoint text primary key)")
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}

	return nil
}

func subsidy(height int) int64 {
	shifts := (height + 1) / 840000
	if shifts > 63 {
		return 0
	}
	return int64(5000000000) >> shifts
}

func processShares() {
	for s := range shareProcessQueue {
		rewardShare, _ := big.NewFloat(0).Quo(big.NewFloat(0).SetInt(s.blockTarget), big.NewFloat(0).SetInt(s.shareTarget)).Float64()

		if rewardShare > 1 {
			rewardShare = 1
		}

		rewardShare *= 0.975 // 2.5% safety margin to avoid running out of funds

		sub := subsidy(s.height)

		reward := int64(float64(sub) * rewardShare)
		logging.Infof("Subsidy      : %d", sub)
		logging.Infof("Block target : %x", padTo32(s.blockTarget.Bytes()))
		logging.Infof("Upstream target : %x", padTo32(s.upstreamTarget.Bytes()))
		logging.Infof("Share target : %x", padTo32(s.shareTarget.Bytes()))
		logging.Infof("Reward share : %.9f", rewardShare)
		logging.Infof("Reward       : %d", reward)

		statement, err := db.Prepare("INSERT INTO unpaid_shares (address, value, time, block_target, share_target, upstream_target, reward_share, subsidy) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			panic(err)
		}

		_, err = statement.Exec(
			s.address,
			reward,
			time.Now().Unix(),
			fmt.Sprintf("%x", padTo32(s.blockTarget.Bytes())),
			fmt.Sprintf("%x", padTo32(s.shareTarget.Bytes())),
			fmt.Sprintf("%x", padTo32(s.upstreamTarget.Bytes())),
			rewardShare,
			sub,
		)
		if err != nil {
			panic(err)
		}

		curUnpaid := unpaidShares[s.address]
		newUnpaid := curUnpaid + reward
		unpaidShares[s.address] = newUnpaid
		srv.UnpaidShares = unpaidShares
		//		logging.Infof("Processed share for %s - balance now %0.8f coins", s.address, float64(newUnpaid)/float64(100000000))
	}
}

func checkShareDuplicate(shareParams []interface{}) bool {
	shareHash := sha256.New()
	for i := 1; i < len(shareParams); i++ {
		shareHash.Write([]byte(shareParams[i].(string)))
	}
	sh := shareHash.Sum(nil)
	shareCacheLock.Lock()
	defer shareCacheLock.Unlock()
	for _, s := range shareCache {
		if bytes.Equal(s, sh) {
			return true
		}
	}
	shareCache = append(shareCache, sh)
	if len(shareCache) > MaxShareCacheLength {
		shareCache = shareCache[len(shareCache)-MaxShareCacheLength:]
	}
	return false
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

	userName := fmt.Sprintf("%s%s", myAddress, os.Getenv("STRATUM_USER_SUFFIX"))
	logging.Infof("Logging into Stratum with %s", userName)
	upstreamClient.Outgoing <- stratum.StratumMessage{
		MessageID:    2,
		RemoteMethod: "mining.authorize",
		Parameters: []string{
			userName,
			"x",
		},
	}
}

func processUpstreamQueue() {
	lastStatus := time.Now()
	for msg := range msgQueue {
		if time.Now().Sub(lastStatus).Seconds() > 30 {
			lastStatus = time.Now()
			logging.Infof("Upstream shares submitted: [%d] - Dumped: [%d]", upstreamSubmitted, upstreamDumped)
		}
		if upstreamStatus.Ready() {
			if msg.RemoteMethod == "mining.submit" && msg.Parameters.([]interface{})[1].(string) != upstreamJob[0].(string) {
				upstreamDumped++
				continue
			}

			connectionLock.Lock()
			if upstreamStatus.Ready() {
				upstreamClient.Outgoing <- msg
				upstreamSubmitted++
			} else {
				msgQueue <- msg
			}
			connectionLock.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
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
			logging.Infof("Upstream stratum disconnected")
			close = true
		}

		if close {
			disconnectUpstream()
			if numClients() > 0 {
				reconnectUpstream()
			}
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

	if numClients() == 0 {
		logging.Infof("No more clients, so disconnecting from upstream")
		disconnectUpstream()
	}
}

func numClients() int {
	count := 0
	clients.Range(func(k, v interface{}) bool {
		count++
		return false
	})
	return count
}

func randomJobID() string {
	r := make([]byte, 8)
	rand.Read(r)
	return hex.EncodeToString(r)
}

func (client *StratumClient) SendWork() {
	if !upstreamStatus.Ready() {
		return
	}
	if !client.Authorized || !client.Subscribed {
		return
	}

	logging.Debugf("Sending client %d new job %s", client.ID, upstreamJob[0].(string))

	client.SendDifficulty()
	client.SendExtraNonce()

	newJob := make([]interface{}, len(upstreamJob))
	copy(newJob, upstreamJob)
	// Make random job ID to force restarting
	newJob[0] = randomJobID()
	// Set cleanJobs to true
	newJob[8] = true

	client.conn.Outgoing <- stratum.StratumMessage{
		RemoteMethod: "mining.notify",
		Parameters:   newJob,
	}

	client.CurrentJob = newJob

}

func (client *StratumClient) SendDifficulty() {
	if !client.Authorized || !client.Subscribed {
		return
	}

	clientDiff := upstreamDiff * client.VarDiff
	clientDiff = math.Max(0.01, clientDiff) // Don't allow diff to drop below 0.01
	if client.Difficulty != clientDiff {
		logging.Infof("Setting difficulty for client %d to %0.9f", client.ID, clientDiff)
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

func (client *StratumClient) AdjustDiffIfNeeded() {
	mins := time.Now().Sub(client.StartShareCount).Minutes()
	if client.ShareCount > 100 || mins > 0.5 {
		// Goal: 10 shares per minute?
		spm := float64(client.ShareCount) / mins
		if spm < 6 || spm > 14 {
			client.VarDiff = math.Min(1, client.VarDiff*(spm/float64(10))) // Don't make downstream more difficult than upstream ever
			logging.Infof("Adjusting difficulty for client %d to %0.9f. spm: %0.9f", client.ID, client.VarDiff, spm)
			diffCacheLock.Lock()
			diffCache[client.Username] = client.VarDiff
			diffCacheLock.Unlock()

			client.SendWork()
		}
		client.StartShareCount = time.Now()
		client.ShareCount = 0
	}
}

func (client *StratumClient) SendWorkOnFrequentFail() {
	mins := time.Now().Sub(client.StartFailShareCount).Minutes()
	if client.FailedShareCount > 100 || mins > 0.5 {
		// Goal: 10 shares per minute?
		spm := float64(client.FailedShareCount) / mins
		if spm > 100 {
			client.SendWork()
		}
		client.StartFailShareCount = time.Now()
		client.FailedShareCount = 0
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

		diffCacheLock.Lock()
		cachedDiff, ok := diffCache[client.Username]
		if ok {
			client.VarDiff = cachedDiff
		}
		diffCacheLock.Unlock()

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
		if !upstreamStatus.Ready() {
			client.conn.Outgoing <- stratum.StratumMessage{
				MessageID: msg.Id(),
				Result:    false,
				Error:     []interface{}{"-26", "Internal failure"},
			}
			return
		}
		upstreamJobID := upstreamJob[0].(string)
		if len(client.CurrentJob) == 0 {
			client.conn.Outgoing <- stratum.StratumMessage{
				MessageID: msg.Id(),
				Result:    false,
				Error:     []interface{}{"-24", "Unknown job"},
			}
			return
		}

		params := msg.Parameters.([]interface{})
		if checkShareDuplicate(params) {
			client.conn.Outgoing <- stratum.StratumMessage{
				MessageID: msg.Id(),
				Result:    false,
				Error:     []interface{}{"-22", "Duplicate"},
			}
			return
		}

		if params[1].(string) != client.CurrentJob[0].(string) {
			client.conn.Outgoing <- stratum.StratumMessage{
				MessageID: msg.Id(),
				Result:    false,
				Error:     []interface{}{"-25", "Stale"},
			}
			return
		}

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

		heightLen := int(coinbaseTx.TxIn[0].SignatureScript[0])
		heightBytes := make([]byte, 8)
		copy(heightBytes, coinbaseTx.TxIn[0].SignatureScript[1:1+heightLen])
		height := binary.LittleEndian.Uint64(heightBytes)

		h := coinbaseTx.TxHash()
		merkles := client.CurrentJob[4].([]interface{})

		merkleRoot := &h
		for _, m := range merkles {
			hashBytes, _ := hex.DecodeString(m.(string))
			merkleHash, _ := chainhash.NewHash(hashBytes)
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
		blockTarget := big.NewInt(0).Set(blockchain.CompactToBig(bits))
		upstreamTarget := diffToTarget(upstreamDiff)
		shareTarget := diffToTarget(upstreamDiff * client.VarDiff)

		//logging.Infof("Height : %d\nShare target     : %x\nUpstream Target  : %x\nBlock target     : %x\n", height, padTo32(shareTarget.Bytes()), padTo32(upstreamTarget.Bytes()), padTo32(blockTarget.Bytes()))

		ch, _ := chainhash.NewHash(powHash[:])
		bnHash := blockchain.HashToBig(ch)
		off := bnHash.Cmp(shareTarget)
		if off == -1 {
			client.conn.Outgoing <- stratum.StratumMessage{
				MessageID: msg.Id(),
				Result:    true,
			}
			//logging.Infof("Client %d submitted valid share\n\nHeight : %d\nHash   : %x\nTarget : %x\n", client.ID, height, padTo32(bnHash.Bytes()), padTo32(shareTarget.Bytes()))
			//logging.Infof("Height : %d\nShare target     : %x\nUpstream Target  : %x\nBlock target     : %x\n", height, padTo32(shareTarget.Bytes()), padTo32(upstreamTarget.Bytes()), padTo32(blockTarget.Bytes()))
			shareProcessQueue <- Share{blockTarget: blockTarget, shareTarget: shareTarget, upstreamTarget: upstreamTarget, address: client.Username, height: int(height)}
			client.ShareCount++
			client.AdjustDiffIfNeeded()
		} else {
			client.conn.Outgoing <- stratum.StratumMessage{
				MessageID: msg.Id(),
				Result:    false,
				Error:     []interface{}{"-23", "Above target"},
			}
			logging.Infof("Client %d submitted invalid share\n\nHash   : %x\nTarget : %x\n", client.ID, padTo32(bnHash.Bytes()), padTo32(shareTarget.Bytes()))
			client.FailedShareCount++
			client.SendWorkOnFrequentFail()
		}

		off = bnHash.Cmp(upstreamTarget)
		off2 := bnHash.Cmp(blockTarget)
		if off == -1 || off2 == -1 {
			//logging.Infof("Client %d submitted valid upstream share\n\nHash   : %x\nTarget : %x\n", client.ID, padTo32(bnHash.Bytes()), padTo32(upstreamTarget.Bytes()))

			// Submit upstream
			extraNoncePrefix := make([]byte, 4)
			binary.BigEndian.PutUint32(extraNoncePrefix, uint32(client.ID))
			extraNoncePrefix = extraNoncePrefix[1:]
			msg.MessageID = atomic.AddInt32(&nextUpstreamMessageID, 1)

			msg.Parameters = []interface{}{
				myAddress,
				upstreamJobID,
				hex.EncodeToString(append(extraNoncePrefix, en2...)),
				params[3].(string),
				params[4].(string),
			}

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
			upstreamStatus.SubscriptionResponseReceived = true
		}
	case 2:
		if err == nil {
			resultBool, ok := msg.Result.(bool)
			if !(ok && !resultBool) {
				logging.Infof("Succesfully authorized\n")
				upstreamStatus.Authorized = true
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
				upstreamDeclinedShares = 0
			} else {
				if !result {
					logging.Info("Share declined: %s\n", msg.String())
					upstreamDeclinedShares++
					if upstreamDeclinedShares > 100 {
						disconnectUpstream()
						go reconnectUpstream()
					}
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
	updateDownstreamJobsLock.Lock()
	defer updateDownstreamJobsLock.Unlock()
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
		upstreamStatus.DifficultyReceived = true
		go UpdateDifficultyDownstream()
	case "mining.set_extranonce":
		// Adjusted extranonce
		upstreamExtraNonce1, _ := hex.DecodeString(msg.Parameters.([]interface{})[0].(string))
		upstreamExtraNonce2Size = int8(msg.Parameters.([]interface{})[1].(float64))
		logging.Infof("Setting extranonce1 [%x], extranonce2_size: [%d] (from set_extranonce)\n", upstreamExtraNonce1, upstreamExtraNonce2Size)
		go UpdateExtraNonceDownstream()
	case "mining.notify":
		params := msg.Parameters.([]interface{})
		newJob := make([]interface{}, len(params))
		copy(newJob, params)
		upstreamJob = newJob
		upstreamStatus.JobReceived = true
		logging.Infof("Received new job from upstream: %s", upstreamJob[0].(string))
		UpdateJobDownstream()
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
	diff /= 256

	target := big.NewInt(0)
	target.Exp(big.NewInt(2), big.NewInt(256-64), nil)
	target.Add(target, big.NewInt(1))
	target.Mul(big.NewInt(0xffff0000), target)

	t := new(big.Int)
	big.NewFloat(0).Quo(big.NewFloat(0).SetInt(target), big.NewFloat(diff)).Int(t)
	return t
}

func padTo32(b []byte) []byte {
	b2 := make([]byte, 32)
	if len(b) < 32 {
		copy(b2[32-len(b):], b)
		return b2
	}

	return b
}

func processPayouts() {
	for {
		tx := wire.NewMsgTx(2)

		payoutTime := time.Now().Unix()

		logging.Debugf("Processing payouts of shares up until %d...", payoutTime)

		rows, err := db.Query("SELECT address, SUM(value) FROM unpaid_shares WHERE time <= ? GROUP BY address", payoutTime)

		if err != nil {
			logging.Errorf("Error querying payouts: %v", err)
		}
		processedAddresses := map[string]int64{}
		var addr string
		var value int64
		for rows.Next() {
			err = rows.Scan(&addr, &value)
			if err != nil {
				logging.Errorf("Error scanning payouts row: %v", err)
				continue
			}

			if value > 1000000000 {
				hash, version, err := base58.CheckDecode(addr)
				if err == nil && version == network.Base58P2PKHVersion {
					pubKeyHash := hash
					if err != nil {
						logging.Warnf("Error creating script for address: %s - %v", addr, err)
						continue
					}
					if len(pubKeyHash) != 20 {
						logging.Warnf("Error creating script for address: %s - pubkeyHash is not 20 bytes", addr)
						continue
					}
					p2pkhScript, err := txscript.NewScriptBuilder().AddOp(txscript.OP_DUP).
						AddOp(txscript.OP_HASH160).AddData(pubKeyHash).
						AddOp(txscript.OP_EQUALVERIFY).AddOp(txscript.OP_CHECKSIG).Script()
					if err != nil {
						logging.Warnf("Error creating script for address: %s - %v", addr, err)
						continue
					}
					tx.AddTxOut(wire.NewTxOut(value, p2pkhScript))
				} else if err == nil && version == network.Base58P2SHVersion {
					scriptHash := hash
					if err != nil {
						logging.Warnf("Error creating script for address: %s - %v", addr, err)
						continue
					}
					if len(scriptHash) != 20 {
						logging.Warnf("Error creating script for address: %s - scriptHash is not 20 bytes", addr)
						continue
					}
					p2shScript, err := txscript.NewScriptBuilder().AddOp(txscript.OP_HASH160).AddData(scriptHash).AddOp(txscript.OP_EQUAL).Script()
					if err != nil {
						logging.Warnf("Error creating script for address: %s - %v", addr, err)
						continue
					}
					tx.AddTxOut(wire.NewTxOut(value, p2shScript))
				} else if strings.HasPrefix(addr, fmt.Sprintf("%s1", network.Bech32Prefix)) {
					script, err := bech32.SegWitAddressDecode(addr)
					if err != nil {
						logging.Warnf("Error creating script for address: %s - %v", addr, err)
						continue
					}
					tx.AddTxOut(wire.NewTxOut(value, script))
				} else {
					logging.Warnf("Invalid address: %s", addr)

					continue
				}
			}
			processedAddresses[addr] = value
		}

		if len(processedAddresses) > 0 {
			logging.Debugf("Created TX with %d payouts", len(processedAddresses))
			err = FundAndSign(tx)
			if err != nil {
				logging.Errorf("Error funding and signing payout: %v", err)
			} else {

				txid, err := SendTx(tx)
				if err != nil {
					logging.Errorf("Error sending transaction: %v", err)
				} else {
					logging.Debugf("Sent payout: %s", txid)
					for addr, val := range processedAddresses {
						tx, err := db.Begin()
						if err != nil {
							panic(err)
						}
						_, err = tx.Exec(`INSERT INTO paid_shares
												(address, value, time, block_target, share_target, upstream_target, reward_share, subsidy) 
										   SELECT 
												address, value, time, block_target, share_target, upstream_target, reward_share, subsidy 
										   FROM unpaid_shares 
										   WHERE address=? AND time <= ?`, addr, payoutTime)

						if err != nil {
							panic(err)
						}

						_, err = tx.Exec(`DELETE FROM unpaid_shares WHERE address=? AND time <= ?`, addr, payoutTime)
						if err != nil {
							panic(err)
						}

						err = tx.Commit()
						if err != nil {
							panic(err)
						}

						unpaidShares[addr] = unpaidShares[addr] - val
					}
				}
			}
		}

		time.Sleep(time.Second * 300)
	}
}

type DummyStringAddress struct {
	Address string
	Pkh     []byte
}

func (d DummyStringAddress) EncodeAddress() string {
	return d.Address
}

func (d DummyStringAddress) String() string {
	return d.Address
}

func (d DummyStringAddress) IsForNet(p *chaincfg.Params) bool {
	return true
}

func (d DummyStringAddress) ScriptAddress() []byte {
	return d.Pkh
}

func GetMySpendableUtxos() ([]Utxo, error) {
	utxos := []Utxo{}
	unspent, err := rpc.ListUnspentMinMaxAddresses(0, 1000000, []btcutil.Address{DummyStringAddress{Address: myAddress, Pkh: pkh}})
	if err != nil {
		return utxos, err
	}

	filtered := 0
	for _, u := range unspent {
		utxos = append(utxos, Utxo{
			TxID:         u.TxID,
			Vout:         uint(u.Vout),
			ScriptPubKey: u.ScriptPubKey,
			Amount:       uint64(math.Round(u.Amount * float64(100000000))),
		})
	}

	logging.Infof("Returning %d spendable UTXOS (%d unspendable filtered)", len(utxos), filtered)

	return utxos, nil
}

func FundAndSign(tx *wire.MsgTx) error {
	utxos, err := GetMySpendableUtxos()
	if err != nil {
		return fmt.Errorf("Error fetching UTXOs: %s", err.Error())
	}

	fundingRequired := int64(0)
	for _, out := range tx.TxOut {
		fundingRequired += out.Value
	}

	fundingRequired += 100000000 // Make sure this can cover the fee

	sort.Slice(utxos, func(i, j int) bool {
		return (utxos[i].Amount < utxos[j].Amount)
	})

	for _, u := range utxos {
		pkScript, _ := hex.DecodeString(u.ScriptPubKey)
		h, _ := chainhash.NewHashFromStr(u.TxID)
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(h, uint32(u.Vout)), pkScript, nil))
		fundingRequired -= int64(u.Amount)
		if fundingRequired <= 0 {
			break
		}

	}

	if fundingRequired > 0 {
		return fmt.Errorf("Insufficient funds - I am %f VTC short", float64(fundingRequired)/float64(100000000))
	}

	p2pkhScript, err := txscript.NewScriptBuilder().AddOp(txscript.OP_DUP).
		AddOp(txscript.OP_HASH160).AddData(pkh).
		AddOp(txscript.OP_EQUALVERIFY).AddOp(txscript.OP_CHECKSIG).Script()
	if err != nil {
		return fmt.Errorf("script_failure")
	}
	changeOutput := wire.NewTxOut(0, p2pkhScript)
	tx.AddTxOut(changeOutput)

	for i := range tx.TxIn {
		tx.TxIn[i].SignatureScript = make([]byte, 107) // add dummy signature to properly calculate size
	}

	txWeight := (tx.SerializeSizeStripped() * 3) + tx.SerializeSize()
	vSize := (float64(txWeight) + float64(3)) / float64(4)
	vSizeInt := uint64(vSize + float64(0.5)) // Round Up
	fee := uint64(vSizeInt * 100)

	changeOutput.Value = 100000000 - int64(fee) - fundingRequired

	// For now using only P2PKH signing - since we generate
	// a legacy address. Will have to use segwit stuff at some point

	// generate tx-wide hashCache for segwit stuff
	// might not be needed (non-witness) but make it anyway
	// hCache := txscript.NewTxSigHashes(tx)

	// make the stashes for signatures / witnesses
	sigStash := make([][]byte, len(tx.TxIn))
	witStash := make([][][]byte, len(tx.TxIn))

	for i := range tx.TxIn {
		var found bool
		var utxo Utxo
		for _, u := range utxos {
			if u.TxID == (tx.TxIn[i].PreviousOutPoint.Hash.String()) && u.Vout == uint(tx.TxIn[i].PreviousOutPoint.Index) {
				utxo = u
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("Cannot sign input %s/%d - Not present in known UTXOs", tx.TxIn[i].PreviousOutPoint.Hash.String(), tx.TxIn[i].PreviousOutPoint.Index)
		}
		pkScript, err := hex.DecodeString(utxo.ScriptPubKey)
		if err != nil {
			return err
		}

		sigStash[i], err = txscript.SignatureScript(tx, i, pkScript, txscript.SigHashAll, priv, true)
		if err != nil {
			return err
		}
	}
	// swap sigs into sigScripts in txins
	for i, txin := range tx.TxIn {
		if sigStash[i] != nil {
			txin.SignatureScript = sigStash[i]
		}
		if witStash[i] != nil {
			txin.Witness = witStash[i]
			txin.SignatureScript = nil
		}
	}

	return nil
}

func SendTx(tx *wire.MsgTx) (string, error) {
	var b bytes.Buffer
	tx.Serialize(&b)
	result, err := rpc.RawRequest("sendrawtransaction", []json.RawMessage{[]byte(fmt.Sprintf("\"%s\"", hex.EncodeToString(b.Bytes())))})
	var txid string
	jsonErr := json.Unmarshal(result, &txid)
	success := (err == nil && jsonErr == nil && len(txid) == 64)
	if !success {
		return "", fmt.Errorf("Error sending tx: %v %v %s", err, jsonErr, txid)
	}
	return txid, nil
}
