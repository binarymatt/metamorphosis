package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"golang.org/x/sync/errgroup"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

type ClientContextKey struct{}
type RecordProcessor = func(context.Context, *metamorphosisv1.Record) error

type Actor struct {
	id        string
	mc        API
	processor RecordProcessor
	logger    *slog.Logger
}

func (a *Actor) Work(ctx context.Context) error {
	reservation := a.mc.CurrentReservation()
	a.logger = a.logger.With("shard_id", reservation.ShardID, "worker_id", reservation.WorkerID, "group_id", reservation.GroupID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			record, err := a.mc.FetchRecord(ctx)
			if err != nil {
				a.logger.Error("error fetching record", "error", err)
				return err
			}
			if record == nil {
				time.Sleep(1 * time.Second)
				continue
			}
			ctxWithClient := context.WithValue(ctx, ClientContextKey{}, a.mc)
			if err := a.processor(ctxWithClient, record); err != nil {
				a.logger.Error("error processing record", "error", err, "processor", a.processor)
				return err
			}
			if err := a.mc.CommitRecord(ctx, record); err != nil {
				a.logger.Error("error commiting record", "error", err)
				return err
			}
		}
	}
}

type Manager struct {
	config *Config
	actors *errgroup.Group
	logger *slog.Logger
	// processor RecordProcessor
	ctx            context.Context
	internalClient API

	cachedShards      map[string]types.Shard
	workerIDs         []string
	currentActorCount int
	cacheLastChecked  time.Time
}

func New(ctx context.Context, config *Config) *Manager {
	eg, ctx := errgroup.WithContext(ctx)
	return &Manager{
		currentActorCount: 0,
		config:            config,
		actors:            eg,
		ctx:               ctx,
		logger:            config.logger.With("service", "manager"),
	}
}

func (m *Manager) Start(ctx context.Context) error {
	m.cachedShards = map[string]types.Shard{}
	if m.logger == nil {
		m.logger = slog.Default()
	}
	if err := m.config.Bootstrap(ctx); err != nil {
		m.logger.Error("could not bootstrap config", "error", err)
		return err
	}
	m.internalClient = NewClient(m.config, 0)
	m.actors, m.ctx = errgroup.WithContext(ctx)
	m.actors.Go(func() error {
		if err := m.Loop(m.ctx); err != nil {
			return err
		}
		return nil
	})
	return m.actors.Wait()
}
func (m *Manager) Loop(ctx context.Context) error {
	// check shard count
	ticker := time.NewTicker(5 * time.Second)
	m.logger.Info("starting loop")
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			shards, err := m.CheckForAvailableShards(ctx)
			if err != nil {
				m.logger.Error("error checking shards", "error", err)
				continue
			}
			if len(shards) < 1 {
				m.logger.Info("all shards are reserved, sleeping")
				continue
			}
			m.logger.Info("available shards", "count", len(shards))
			for _, shard := range shards {
				if m.currentActorCount < m.config.maxActorCount {
					// add more actors
					m.logger.Info("adding actor", "current_count", m.currentActorCount, "current_workers", m.workerIDs)
					index := m.currentActorCount
					m.actors.Go(func() error {
						workerID := fmt.Sprintf("%s.%d", m.config.workerPrefix, index)
						m.logger.Info("setting up actor inside error group", "index", index)
						logger := m.config.logger.With("service", "actor")
						cfg := NewConfig().
							WithLogger(logger).
							WithGroup(m.config.GroupID).
							WithWorkerID(workerID).
							WithStreamArn(m.config.StreamARN).
							WithKinesisClient(m.config.kinesisClient).
							WithDynamoClient(m.config.dynamoClient).
							WithShardID(*shard.ShardId).
							WithStreamArn(m.config.StreamARN).
							WithTableName(m.config.ReservationTable)
						client := NewClient(cfg, index)
						_, err := client.Init(ctx)
						if err != nil {
							if errors.Is(err, ErrShardReserved) {
								logger.Info("exiting out of routine, shard not available.", "error", err)
								return nil
							}
							return err
						}
						worker := &Actor{
							id:        workerID,
							mc:        client,
							logger:    logger,
							processor: m.config.recordProcessor,
						}
						defer func() {
							ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
							if err := client.ReleaseReservation(ctx); err != nil {
								logger.Error("error during reservation release", "error", err)
							}
							cancel()
							logger.Warn("removing actor count")
							m.currentActorCount--

						}()
						m.workerIDs = append(m.workerIDs, workerID)
						logger.Info("inside routine, starting work", "index", index)
						return worker.Work(m.ctx)
					})
					m.logger.Info("added worker with index", "index", index, "worker_count", m.currentActorCount)
					m.currentActorCount++
				}
			}
			// time.Sleep(1 * time.Second)
		}
	}
}

func (m *Manager) CheckForAvailableShards(ctx context.Context) ([]types.Shard, error) {
	m.logger.Info("checking for available shards", "current_actors", m.currentActorCount, "max_actors", m.config.maxActorCount)
	// List Shards
	if err := m.shardsState(ctx); err != nil {
		m.logger.Error("error updated shard state", "error", err)
		return nil, err
	}

	// get current reservations
	reservations, err := m.internalClient.ListReservations(ctx)
	if err != nil {
		return nil, err
	}
	availableShards := []types.Shard{}
	// Find first unreserved shard
	for _, shard := range m.cachedShards {
		if !IsReserved(reservations, shard) {
			availableShards = append(availableShards, shard)
		}
	}
	return availableShards, nil
}

func (m *Manager) shardsState(ctx context.Context) error {
	// do we need to check state again
	now := time.Now()
	if now.Before(m.cacheLastChecked.Add(m.config.shardCacheDuration)) {
		m.logger.Debug("cache hasn't expired")
		return nil
	}

	out, err := m.config.kinesisClient.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &m.config.StreamARN,
	})
	if err != nil {
		return err
	}
	for _, shard := range out.StreamDescription.Shards {
		m.cachedShards[*shard.ShardId] = shard
	}
	m.logger.Info("cached shard state", "count", len(m.cachedShards), "current_actors", m.currentActorCount)
	m.cacheLastChecked = time.Now()
	return nil
}
