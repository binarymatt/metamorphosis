package main

import (
	"context"
	"log/slog"

	"github.com/binarymatt/metamorphosis"
)

func main() {
	cfg := metamorphosis.NewConfig(
		metamorphosis.WithGroup("group"),
		metamorphosis.WithWorkerID("worker1"),
		metamorphosis.WithStreamArn("arn:aws:kinesis:us-east-1:000000000000:stream/metamorphosis_stream"),
	)
	client := metamorphosis.NewClient(cfg)

	// Init is needed to reserve a shard.
	if err := client.Init(context.Background()); err != nil {
		panic(err)
	}
	// FetchRecord retrieves the first record available
	record, err := client.FetchRecord(context.Background())

	if err != nil {
		panic(err)
	}
	if record == nil {
		return
	}
	slog.Info("retrieved record", "record", record)
	if err := client.CommitRecord(context.Background(), record.Sequence); err != nil {
		slog.Error("error committing record", "error", err)
	}
}
