package main

import (
	"context"
	"log/slog"

	"github.com/binarymatt/metamorphosis"
	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

func processor(ctx context.Context, record *metamorphosisv1.Record) error {
	slog.Info("processing record", "record", record)
	return nil
}
func main() {
	cfg := metamorphosis.NewConfig(
		metamorphosis.WithGroup("group"),
		metamorphosis.WithWorkerPrefix("worker"),
		metamorphosis.WithRecordProcessor(processor),
		metamorphosis.WithStreamArn("arn:aws:kinesis:us-east-1:000000000000:stream/metamorphosis_stream"),
	)
	manager := metamorphosis.New(context.Background(), cfg)
	if err := manager.Start(context.Background()); err != nil {
		panic(err)
	}
}
