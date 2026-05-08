package main

import (
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

	for i := 0; ; i++ {
		err := nc.Publish(fmt.Sprintf(domain.SubjectPrefix, "org_b"), fmt.Appendf(nil, "pay %d", i))
		if err != nil {
			slog.Error("publish message failed", "error", err)
			continue
		}
		slog.Info("pub", "message", fmt.Sprintf("pay %d", i))
		time.Sleep(time.Second)
	}
}
