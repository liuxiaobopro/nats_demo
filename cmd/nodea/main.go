package main

import (
	"errors"
	"fmt"
	"log/slog"
	"nats_demo/domain"
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

	js, err := nc.JetStream()
	if err != nil {
		slog.Error("get jetstream failed", "error", err)
		os.Exit(1)
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     domain.StreamName,
		Subjects: []string{fmt.Sprintf(domain.SubjectPrefix, "org_b")},
	},
	); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		slog.Error("add stream failed", "error", err)
		os.Exit(1)
	}

	for i := 0; ; i++ {
		ack, err := js.Publish(fmt.Sprintf(domain.SubjectPrefix, "org_b"), fmt.Appendf(nil, "pay %d", i))
		if err != nil {
			slog.Error("publish message failed", "error", err)
			continue
		}
		slog.Info("pub", "message", fmt.Sprintf("pay %d", i), "ack", ack)
		time.Sleep(time.Second)
	}
}
