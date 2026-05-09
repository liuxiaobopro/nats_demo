package main

import (
	"errors"
	"log/slog"
	"nats_demo/domain"
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

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("get jetstream failed", "error", err)
		os.Exit(1)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: domain.StreamName,
		Subjects: []string{
			"dn.org_b.*",
		},
	},
	); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		slog.Error("add stream failed", "error", err)
		os.Exit(1)
	}

	js.Subscribe("dn.org_b.*", func(m *nats.Msg) {
		slog.Info("recv", "message", string(m.Data), "subject", m.Subject, "header", m.Header)

		// ack 确认消息
		m.Ack()

		// 失败要重投：m.Nak()；不要重投：m.Term()。
	}, nats.ManualAck(), nats.Durable("org_b"))

	js1, err1 := nc.JetStream()
	if err1 != nil {
		slog.Error("get jetstream failed", "error", err)
		os.Exit(1)
	}
	if _, err1 := js1.AddStream(&nats.StreamConfig{
		Name: domain.StreamName,
		Subjects: []string{
			"dn.org_b.*",
		},
	},
	); err1 != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		slog.Error("add stream failed", "error", err)
		os.Exit(1)
	}

	js1.Subscribe("dn1.org_b.*", func(m *nats.Msg) {
		slog.Info("recv", "message", string(m.Data), "subject", m.Subject, "header", m.Header)

		// ack 确认消息
		m.Ack()

		// 失败要重投：m.Nak()；不要重投：m.Term()。
	}, nats.ManualAck(), nats.Durable("org_b1"))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
