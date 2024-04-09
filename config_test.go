package metamorphosis

import (
	"context"
	"testing"
	"time"

	"github.com/shoenig/test/must"
)

func TestValidate_HappyPath(t *testing.T) {
	config := &Config{
		GroupID:          "group",
		WorkerID:         "worker",
		StreamARN:        "arn",
		ReservationTable: "table",
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
				WorkerID:         "worker",
				StreamARN:        "arn",
				ReservationTable: "table",
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

func TestBootstrap(t *testing.T) {

	config := &Config{
		GroupID:          "group",
		WorkerID:         "worker",
		StreamARN:        "arn",
		ReservationTable: "table",
	}
	must.Zero(t, config.ReservationTimeout)
	err := config.Bootstrap(context.Background())
	must.NoError(t, err)
	must.Eq(t, 1*time.Minute, config.ReservationTimeout)
	must.NotNil(t, config.kinesisClient)
	must.NotNil(t, config.dynamoClient)
}

func TestConfigAnnotations(t *testing.T) {
	c := NewConfig()

	c.WithGroup("testGroup")
	must.Eq(t, "testGroup", c.GroupID)

	c.WithWorkerID("workerID")
	must.Eq(t, "workerID", c.WorkerID)

	c.WithStreamArn("stream:arn")
	must.Eq(t, "stream:arn", c.StreamARN)

	c.WithTableName("reservation_table")
	must.Eq(t, "reservation_table", c.ReservationTable)

	c.WithShardID("shardOne")
	must.Eq(t, "shardOne", c.ShardID)
}
