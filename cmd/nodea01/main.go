package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("connect to nats failed", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	for i := 0; ; i++ {
		err := nc.PublishMsg(&nats.Msg{
			Subject: "dn.org_b.pay",
			Data:    fmt.Appendf(nil, "pay %d", i),
			Header: nats.Header{
				"X-From-ID": []string{"123"},
			},
		})
		if err != nil {
			slog.Error("publish message failed", "error", err)
			continue
		}
		slog.Info("pub", "message", fmt.Sprintf("pay %d", i))
		time.Sleep(time.Second)
	}
}
