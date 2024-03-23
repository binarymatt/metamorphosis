package metamorphosis

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type Metamorphosis struct {
	// internal fields
	config *Config
	//kc          KinesisAPI
	// dc          DynamoDBAPI
	reservation *Reservation
	seed        int
	log         *slog.Logger
}

func New(config *Config, seed int) *Metamorphosis {
	return &Metamorphosis{
		seed:   seed,
		config: config,
		log:    slog.With("seed", seed, "worker", config.WorkerID, "group", config.GroupID),
	}
}
func (m *Metamorphosis) Init(ctx context.Context) (func(), error) {
	m.log.Info("initializing metamorphosis client")
	if err := m.config.Bootstrap(ctx); err != nil {
		return nil, err
	}
	//m.kc = m.config.kinesisClient
	//m.dc = m.config.dynamoClient

	if err := m.config.Validate(); err != nil {
		return nil, err
	}

	if err := m.reserveShard(ctx); err != nil {
		return nil, err
	}

	if m.config.RenewTime > 0 {
		go func(ctx context.Context) {
			if err := m.renewReservation(ctx); err != nil {
				m.log.Error("error in goroutine renewal", "error", err)
			}
		}(ctx)
	}

	return func() {
		if err := m.releaseReservation(ctx); err != nil {
			m.log.Error("error releasing reservation", "error", err)
		}
	}, nil
}

func (m *Metamorphosis) retrieveRandomShardID(ctx context.Context) (string, error) {
	// Get existing reservations
	reservations, err := m.listReservations(ctx)
	if err != nil {
		return "", err
	}
	m.log.Info("list shards to retrieve random")
	// List Shards
	output, err := m.config.kinesisClient.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: aws.String(m.config.StreamARN),
	})
	if err != nil {
		m.log.Error("error in listshards part", "error", err)
		return "", err
	}
	shards := output.Shards
	shardSize := len(shards)
	// Find first unreserved shard
	for i := range shards {
		index := m.seed + i
		if index > shardSize-1 {
			index = index - shardSize
		}
		shard := shards[index]
		if !isReserved(reservations, shard) {
			return *shard.ShardId, nil
		}
	}

	return "", ErrAllShardsReserved
}
func isReserved(reservations []Reservation, shard ktypes.Shard) bool {
	for _, reservation := range reservations {
		if reservation.ShardID == *shard.ShardId {
			return true
		}
	}
	return false
}
