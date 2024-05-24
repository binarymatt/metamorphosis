package metamorphosis

import (
	"testing"

	"github.com/shoenig/test/must"
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
	c := NewConfig(

		WithGroup("testGroup"),

		WithWorkerID("workerID"),

		WithStreamArn("stream:arn"),
		WithReservationTableName("reservation_table"),
		WithShardID("shardOne"),
	)
	must.Eq(t, "testGroup", c.GroupID)
	must.Eq(t, "workerID", c.WorkerID)
	must.Eq(t, "stream:arn", c.StreamARN)

	must.Eq(t, "reservation_table", c.ReservationTableName)

	must.Eq(t, "shardOne", c.ShardID)
}
