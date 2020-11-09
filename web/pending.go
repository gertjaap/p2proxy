package web

import (
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
