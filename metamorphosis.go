package metamorphosis

import (
	"context"
	"log/slog"

	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

type API interface {
	CommitRecord(ctx context.Context, record *metamorphosisv1.Record) error
	FetchRecord(ctx context.Context) (*metamorphosisv1.Record, error)
	FetchRecords(ctx context.Context, max int32) ([]*metamorphosisv1.Record, error)
	Init(ctx context.Context) error
	PutRecords(ctx context.Context, request *PutRecordsRequest) error
	CurrentReservation() *Reservation
	ListReservations(ctx context.Context) ([]Reservation, error)
	IsReserved(reservations []Reservation, shard ktypes.Shard) bool
	IsShardClosed(context.Context) (bool, error)
	CloseShard(context.Context) error
	ClearIterator()
}
type Client struct {
	// internal fields
	config       *Config
	reservation  *Reservation
	logger       *slog.Logger
	nextIterator *string
}

func NewClient(config *Config) *Client {
	return &Client{
		config: config,
		logger: config.Logger.With("seed", config.Seed, "worker", config.WorkerID, "group", config.GroupID),
	}
}
func (c *Client) Init(ctx context.Context) error {
	c.logger.Info("initializing metamorphosis client")

	if err := c.config.Validate(); err != nil {
		return err
	}

	if err := c.ReserveShard(ctx); err != nil {
		return err
	}

	return nil
}

/*
func (m *Client) retrieveRandomShardID(ctx context.Context) (string, error) {
	// Get existing reservations
	reservations, err := m.ListReservations(ctx)
	if err != nil {
		return "", err
	}
	m.logger.Info("list shards to retrieve random")
	// List Shards
	output, err := m.config.KinesisClient.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: aws.String(m.config.StreamARN),
	})
	if err != nil {
		m.logger.Error("error in listshards part", "error", err)
		return "", err
	}
	shards := output.Shards
	shardSize := len(shards)

	seed := m.config.Seed
	// Find first unreserved shard
	for i := range shards {
		index := seed + i
		if index > shardSize-1 {
			index = index - shardSize
		}
		shard := shards[index]
		if !m.IsReserved(reservations, shard) {
			return *shard.ShardId, nil
		}
	}

	return "", ErrAllShardsReserved
}
*/

func (m *Client) CurrentReservation() *Reservation {
	return m.reservation
}

func (m *Client) IsReserved(reservations []Reservation, shard ktypes.Shard) bool {
	for _, reservation := range reservations {
		if reservation.ShardID == *shard.ShardId || reservation.LatestSequence == ShardClosed {
			slog.Debug("shard is reserved", "shard", *shard.ShardId)
			return true
		}
	}
	m.logger.Warn("shard is not reserved", "shard", *shard.ShardId)
	return false
}
