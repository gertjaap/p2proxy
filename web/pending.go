package web

import (
	"net/http"

	"github.com/gorilla/mux"
)

func pendingPayoutHandler(w http.ResponseWriter, r *http.Request) {
	args := mux.Vars(r)
	payout := int64(0)

	payout, _ = srv.UnpaidShares[args["addr"]]
	writeJson(w, payout)
}
