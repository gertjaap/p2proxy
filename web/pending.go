package web

import (
	"fmt"
	"net/http"
)

func pendingPayoutHandler(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("address")
	payout := int64(0)

	payout, _ = srv.UnpaidShares[addr]
	result := map[string]interface{}{}
	result[addr] = float64(payout) / 100000000
	writeJson(w, result)
}

func pendingPayoutHtmlHandler(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("address")
	payout := int64(0)

	payout, ok := srv.UnpaidShares[addr]
	if !ok {
		writeHtml(w, "<html><head><title>Address unknown</title></head><body><h1>Address unknown</h1><p>The address you specified is unknown to p2proxy</p><br/><a href=\"/\">&lt; Back</a></body></html>")
	} else {
		writeHtml(w, fmt.Sprintf("<html><head><title>Balance for miner %s</title></head><body><h1>Balance for %s</h1><b>%0.8f VTC</b><br/><a href=\"/\">&lt; Back</a></body></html>", addr, addr, float64(payout)/100000000))
	}
}
