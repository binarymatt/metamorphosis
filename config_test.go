package metamorphosis

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/shoenig/test/must"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

func TestValidate_HappyPath(t *testing.T) {
	config := &Config{
		GroupID:              "group",
		WorkerID:             "worker",
		StreamARN:            "arn",
		ReservationTableName: "table",
	}

	err := config.Validate()
	must.NoError(t, err)
}
func TestValidate_MissingInfo(t *testing.T) {
	cases := []struct {
		name   string
		config *Config
		err    error
	}{
		{
			name: "missing group",
			config: &Config{
				WorkerID:             "worker",
				StreamARN:            "arn",
				ReservationTableName: "table",
			},
			err: ErrInvalidConfiguration,
		},
		{
			name: "missing worker id or prefix",
			config: &Config{
				StreamARN:            "arn",
				ReservationTableName: "table",
				GroupID:              "group",
			},
			err: ErrInvalidConfiguration,
		}, {
			name: "missing StreamARN",
			config: &Config{
				ReservationTableName: "table",
				GroupID:              "group",
				WorkerID:             "worker",
			},
			err: ErrInvalidConfiguration,
		}, {
			name: "missing table name",
			config: &Config{
				StreamARN: "arn",
				GroupID:   "group",
				WorkerID:  "worker",
			},
			err: ErrInvalidConfiguration,
		},
	}
	for _, tc := range cases {
		must.True(t, t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.config.Validate()
			must.ErrorIs(t, err, tc.err)
		}))
	}
}

func TestConfigOptions(t *testing.T) {
	lg := slog.Default().With("test", true)
	c := NewConfig(

		WithGroup("testGroup"),

		WithWorkerID("workerID"),

		WithStreamArn("stream:arn"),
		WithReservationTableName("reservation_table"),
		WithShardID("shardOne"),
		WithReservationTimeout(1*time.Millisecond),
		WithRenewTime(1*time.Hour),
		WithRecordProcessor(func(ctx context.Context, record *metamorphosisv1.Record) error {
			return nil
		}),
		WithLogger(lg),
		WithMaxActorCount(2),
		WithWorkerPrefix("prefix"),
		WithSeed(-1),
		WithManagerLoopWaitTime(1*time.Hour),
		WithBatchSize(500),
	)
	must.Eq(t, "testGroup", c.GroupID)
	must.Eq(t, "workerID", c.WorkerID)
	must.Eq(t, "stream:arn", c.StreamARN)
	must.Eq(t, "reservation_table", c.ReservationTableName)
	must.Eq(t, "shardOne", c.ShardID)
	must.Eq(t, 1*time.Millisecond, c.ReservationTimeout)
	must.Eq(t, 1*time.Hour, c.RenewTime)
	must.NotNil(t, c.RecordProcessor)
	must.Eq(t, lg, c.Logger)
	must.Eq(t, 2, c.MaxActorCount)
	must.Eq(t, "prefix", c.WorkerPrefix)
	must.Eq(t, -1, c.Seed)
	must.Eq(t, 500, c.BatchSize)
}
