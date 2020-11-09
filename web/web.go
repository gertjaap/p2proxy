package web

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gertjaap/p2proxy/logging"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

var srv *Server

type Server struct {
	UnpaidShares map[string]int64
}

func StartServer() *Server {
	r := mux.NewRouter()

	var corsOpt = cors.New(cors.Options{
		AllowOriginFunc: func(origin string) bool {
			return true
		},
		AllowCredentials: true,
	})

	DefineRoutes(r)

	port := os.Getenv("WEBPORT")
	if port == "" {
		port = "8000"
	}

	webSrv := &http.Server{
		Handler: corsOpt.Handler(r),
		Addr:    fmt.Sprintf(":%s", port),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	logging.Infoln("[Web] Starting listening...")
	go func() {
		logging.Fatal(webSrv.ListenAndServe())
	}()
	srv = new(Server)
	return srv
}
