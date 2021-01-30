package web

import (
	"fmt"
	"net/http"
)

func homeHandler(w http.ResponseWriter, r *http.Request) {
	html := `<html>
	<head>
	<title>Vertcoin P2Proxy</title>
	</head>
	<body>
	<h1>Vertcoin P2Proxy Pool</h1>
	<table>
		<tr><td><b>Active miners:</b></td><td>%d</td></tr>
		<tr><td><b>Total pending payouts:</b></td><td>%0.8f VTC</td></tr>
		<tr><td><b>Last TX:</b></td><td><a href="https://insight.vertcoin.org/tx/%s">%s</a></td></tr>
	</table>
	<h2>Address lookup</h2>
	<form action="/balance" method="get">
	<table><tr><td>Address: </td><td><input type="text" name="address" /></td><td><button type="submit">Find</button></td></tr></table>
	<h2>About</h2>
	<p>P2Proxy Pool is a Capped Pay-Per-Share with Recent Backpay (<a href="http://eligius.st/wiki/index.php/Capped_PPS_with_Recent_Backpay">CPPSRB</a>) proxy for <a href="http://p2pool.org/">P2Pool</a> network 1.</p><p>It's most useful if you're a small miner and are struggling to get shares, but still wish to mine as part of the larger P2Pool network. Essentially, it's a centralized pool that mines like one big miner on P2Pool, then pays out via pay-per-share to its miners.</p><p>Shares pay to balances every 5 minutes. Mine with the following info:</p>
	<table>
		<tr><td><b>Stratum:</b></td><td>stratum+tcp://p2proxy.vertcoin.org:9172</td></tr>
		<tr><td><b>Username:</b></td><td>VTC Payout Address</td></tr>
		<tr><td><b>Password:</b></td><td>anything</td></tr>
		<tr><td><b>Payout threshold:</b></td><td>0.1 VTC</td></tr>
	</table>
	</body>
	</html>`
	total := int64(0)
	for _, v := range srv.UnpaidShares {
		total += v
	}
	if srv.LastTX == "" {
		srv.LastTX = "(none)"
	}
	html = fmt.Sprintf(html, len(srv.UnpaidShares), float64(total)/100000000, srv.LastTX, srv.LastTX)
	writeHtml(w, html)
}
