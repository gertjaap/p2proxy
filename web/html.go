package web

import (
	"net/http"
)

func writeHtml(w http.ResponseWriter, html string) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(200)
	w.Write([]byte(html))
}
