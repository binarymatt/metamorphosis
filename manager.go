package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/coder/quartz"
	"golang.org/x/sync/errgroup"
)

var (
	ErrStreamNotAvailable = errors.New("stream status is either creating or deleting")
)

func New(config *Config) *Manager {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	m := &Manager{
		currentActorCount: 0,
		config:            config,
		logger:            logger.With("service", "manager"),
		cachedShards:      map[string]ShardState{},
		internalClient:    NewClient(config),
		clock:             quartz.NewReal(),
	}
	return m
}

type Manager struct {
	config         *Config
	actors         *errgroup.Group
	logger         *slog.Logger
	ctx            context.Context
	internalClient *Client

	workerIDs   []string
	workerMutex sync.Mutex

	currentActorCount int
	actorCountMutex   sync.Mutex
	provisionedActors int

	cacheLastChecked time.Time
	cachedShards     map[string]ShardState

	clock quartz.Clock
}

func (m *Manager) Start(ctx context.Context) error {
	if m.logger == nil {
		m.logger = slog.Default()
	}
	m.actors, m.ctx = errgroup.WithContext(ctx)
	m.actors.Go(func() error {
		if err := m.RefreshActorLoop(m.ctx); err != nil {
			return err
		}
		return nil
	})
	return m.actors.Wait()
}

func (m *Manager) RefreshActors(ctx context.Context) error {
	slog.Warn("refreshing actors")
	shards, err := m.GetAvailableShards(ctx)
	if err != nil {
		m.logger.Error("error getting available shards", "error", err)
		return err
	}
	if len(shards) < 1 {
		m.logger.Info("all shards are reserved")
		return nil
	}
	if m.currentActorCount >= m.config.MaxActorCount {
		m.logger.Info("no available actors")
		return nil
	}
	m.logger.Info("available shards", "count", len(shards))
	for _, shard := range shards {
		if m.currentActorCount < m.config.MaxActorCount {
			// add more actors
			m.logger.Info("adding actor", "current_count", m.currentActorCount, "prefix", m.config.WorkerPrefix, "shard", *shard.ShardId)
			m.actorCountMutex.Lock()
			index := m.provisionedActors
			m.actors.Go(func() error {
				workerID := fmt.Sprintf("%s.%d", m.config.WorkerPrefix, index)
				m.logger.Debug("setting up actor inside error group", "index", index, "worker_id", workerID)
				logger := m.config.Logger.With("service", "actor")
				cfg := NewConfig(
					WithLogger(logger),
					WithGroup(m.config.GroupID),
					WithWorkerID(workerID),
					WithStreamArn(m.config.StreamARN),
					WithKinesisClient(m.config.KinesisClient),
					WithDynamoClient(m.config.DynamoClient),
					WithShardID(*shard.ShardId),
					WithReservationTableName(m.config.ReservationTableName),
					WithRenewTime(m.config.RenewTime),
					WithReservationTimeout(m.config.ReservationTimeout),
					WithSeed(m.currentActorCount),
				)
				client := NewClient(cfg)

				// reserves shard to client
				err := client.Init(ctx)
				if err != nil {
					if errors.Is(err, ErrShardReserved) {
						logger.Error("exiting out of routine, shard not available.", "error", err)
						return nil
					}
					return err
				}
				actor := &Actor{
					id:                   workerID,
					mc:                   client,
					logger:               logger,
					processor:            m.config.RecordProcessor,
					SleepAfterProcessing: m.config.SleepAfterProcessing,
					batchSize:            m.config.BatchSize,
					shard:                shard,
				}
				if cfg.RenewTime > 0 {
					go func(ctx context.Context) {
						if err := client.RenewReservation(ctx); err != nil {
							m.logger.Error("error in goroutine renewal", "error", err)
						}
					}(ctx)
				}

				defer func() {
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					if err := client.ReleaseReservation(ctx); err != nil {
						logger.Error("error during reservation release", "error", err)
					}
					cancel()
					logger.Warn("removing actor count")
					m.DecrementActorCount()

				}()
				m.workerMutex.Lock()
				m.workerIDs = append(m.workerIDs, workerID)
				m.workerMutex.Unlock()
				logger.Debug("inside routine, starting work", "index", m.currentActorCount)

				return actor.Work(m.ctx)
			})
			m.logger.Info("added worker with index", "index", index, "worker_count", m.currentActorCount)
			m.provisionedActors++
			m.IncrementActorCount()
			m.actorCountMutex.Unlock()
		}
	}
	return nil
}

func (m *Manager) RefreshActorLoop(ctx context.Context) error {
	if err := m.RefreshActors(ctx); err != nil {
		slog.Error("error refreshing actors", "error", err)
		return err
	}
	ticker := time.NewTicker(m.config.ManagerLoopWaitTime)
	m.logger.Info("starting loop")
	for {
		select {
		case <-ctx.Done():
			m.logger.Warn("context is done")
			return nil
		case <-ticker.C:
			if err := m.RefreshActors(ctx); err != nil {
				slog.Error("error refreshing actors", "error", err)
			}
		}
	}
}

func (m *Manager) IncrementActorCount() {
	m.currentActorCount++
}

func (m *Manager) DecrementActorCount() {
	m.actorCountMutex.Lock()
	m.currentActorCount--
	m.actorCountMutex.Unlock()
}

func (m *Manager) GetAvailableShards(ctx context.Context) ([]types.Shard, error) {
	m.logger.Info("checking for available shards", "current_actors", m.currentActorCount, "max_actors", m.config.MaxActorCount)
	now := m.clock.Now()
	if err := m.shardsState(ctx); err != nil {
		m.logger.Error("error with stream/shards state", "error", err)
		return nil, err
	}
	availableShards := []types.Shard{}
	keys := make([]string, 0, len(m.cachedShards))
	for k := range m.cachedShards {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		state := m.cachedShards[k]

		if state.Reservation != nil && state.Reservation.LatestSequence == ShardClosed {
			continue
		}
		if state.Reservation != nil && state.Reservation.Expires().Before(now) {
			availableShards = append(availableShards, state.Shard)
		}
		if state.Reservation == nil {
			availableShards = append(availableShards, state.Shard)
		}
	}
	return availableShards, nil
}

type ShardState struct {
	Shard       types.Shard
	Reservation *Reservation
}

func (m *Manager) shardsState(ctx context.Context) error {
	// do we need to check state again
	now := m.clock.Now()
	if now.Before(m.cacheLastChecked.Add(m.config.ShardCacheDuration)) {
		m.logger.Debug("cache hasn't expired")
		return nil
	}

	out, err := m.config.KinesisClient.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamARN: &m.config.StreamARN,
	})
	if err != nil {
		return err
	}

	status := out.StreamDescriptionSummary.StreamStatus
	if status == types.StreamStatusCreating || status == types.StreamStatusDeleting {
		return ErrStreamNotAvailable
	}
	shards, err := m.getShards(ctx)
	if err != nil {
		return err
	}

	lookup, err := m.internalClient.ReservationLookUp(ctx)
	if err != nil {
		m.logger.Error("could not build reservation lookup table, is empty")
	}
	for _, shard := range shards {
		state := ShardState{Shard: shard}
		res, ok := lookup[*shard.ShardId]
		if ok {
			state.Reservation = res
		}
		m.cachedShards[*shard.ShardId] = state
		//m.logger.Warn("shard status", "id", *shard.ShardId, "parent", *shard.ParentShardId, "has_more_shard", *out.StreamDescription.HasMoreShards)
	}
	m.logger.Info("cached shard state", "count", len(m.cachedShards), "current_actors", m.currentActorCount)
	m.cacheLastChecked = m.clock.Now()
	return nil
}

func (m *Manager) getShards(ctx context.Context) ([]types.Shard, error) {
	out, err := m.config.KinesisClient.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: &m.config.StreamARN,
	})
	if err != nil {
		return nil, err
	}
	return out.Shards, nil
}
