package web

import (
	"github.com/gorilla/mux"
)

func DefineRoutes(r *mux.Router) {

	r.HandleFunc("/api/balance", pendingPayoutHandler)

}
