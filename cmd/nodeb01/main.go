package main

import (
	"log/slog"
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("connect to nats failed", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	nc.Subscribe("dn.org_b.*", func(m *nats.Msg) {
		slog.Info("recv", "message", string(m.Data), "subject", m.Subject, "header", m.Header)

		m.Ack()
	})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
