package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"golang.org/x/sync/errgroup"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

type ClientContextKey struct{}
type LoggerContextKey struct{}
type RecordProcessor = func(context.Context, *metamorphosisv1.Record) error

type Actor struct {
	id                   string
	mc                   API
	processor            RecordProcessor
	logger               *slog.Logger
	SleepAfterProcessing time.Duration
	batchSize            int32
}

func (a *Actor) Work(ctx context.Context) error {
	reservation := a.mc.CurrentReservation()
	if reservation == nil {
		return ErrMissingReservation
	}
	a.logger = a.logger.With("shard_id", reservation.ShardID, "worker_id", reservation.WorkerID, "group_id", reservation.GroupID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			a.logger.Debug("fetching record")
			records, err := a.mc.FetchRecords(ctx, a.batchSize)
			if err != nil {
				a.logger.Error("error fetching record(s)", "error", err, "batch_size", a.batchSize)
				return err
			}
			if records == nil {
				a.logger.Debug("record is nil")
				time.Sleep(1 * time.Second)
				continue
			}
			for _, record := range records {
				if record == nil {
					a.logger.Debug("record is nil")
					continue
				}
				a.logger.Debug("processing record", "record", record)

				ctxWithClient := context.WithValue(ctx, ClientContextKey{}, a.mc)
				ctxWithLogger := context.WithValue(ctxWithClient, LoggerContextKey{}, a.logger)
				if err := a.processor(ctxWithLogger, record); err != nil {
					a.logger.Error("error processing record", "error", err, "processor", a.processor)
					return err
				}
				a.logger.Debug("commiting record")
				if err := a.mc.CommitRecord(ctx, record); err != nil {
					a.logger.Error("error commiting record", "error", err)
					return err
				}
			}
			a.logger.Debug("checking sleep", "sleep_time", a.SleepAfterProcessing)
			if a.SleepAfterProcessing != 0 {
				time.Sleep(a.SleepAfterProcessing)
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

	workerIDs   []string
	workerMutex sync.Mutex

	currentActorCount int
	actorCountMutex   sync.Mutex

	cacheLastChecked time.Time
	cachedShards     map[string]types.Shard
}

func New(ctx context.Context, config *Config) *Manager {
	eg, ctx := errgroup.WithContext(ctx)
	return &Manager{
		currentActorCount: 0,
		config:            config,
		actors:            eg,
		ctx:               ctx,
		logger:            config.Logger.With("service", "manager"),
		cachedShards:      map[string]types.Shard{},
	}
}

func (m *Manager) Start(ctx context.Context) error {
	if m.logger == nil {
		m.logger = slog.Default()
	}
	m.internalClient = NewClient(m.config)
	m.actors, m.ctx = errgroup.WithContext(ctx)
	m.actors.Go(func() error {
		if err := m.RefreshActorLoop(m.ctx); err != nil {
			return err
		}
		return nil
	})
	return m.actors.Wait()
}
func (m *Manager) RefreshActorLoop(ctx context.Context) error {
	// check shard count
	ticker := time.NewTicker(m.config.ManagerLoopWaitTime)
	m.logger.Info("starting loop")
	for {
		select {
		case <-ctx.Done():
			m.logger.Warn("context is done")
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
				if m.currentActorCount < m.config.MaxActorCount {
					// add more actors
					m.logger.Info("adding actor", "current_count", m.currentActorCount, "current_workers", m.workerIDs)
					m.actorCountMutex.Lock()
					index := m.currentActorCount
					m.actorCountMutex.Unlock()
					m.actors.Go(func() error {
						workerID := fmt.Sprintf("%s.%d", m.config.WorkerPrefix, index)
						m.logger.Info("setting up actor inside error group", "index", index)
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
							WithSeed(index),
						)
						client := NewClient(cfg)
						err := client.Init(ctx)
						if err != nil {
							if errors.Is(err, ErrShardReserved) {
								logger.Info("exiting out of routine, shard not available.", "error", err)
								return nil
							}
							return err
						}
						worker := &Actor{
							id:                   workerID,
							mc:                   client,
							logger:               logger,
							processor:            m.config.RecordProcessor,
							SleepAfterProcessing: m.config.SleepAfterProcessing,
							batchSize:            m.config.BatchSize,
						}
						if cfg.RenewTime > 0 {
							go func(ctx context.Context) {
								if err := client.RenewReservation(ctx); err != nil {
									m.logger.Error("error in goroutine renewal", "error", err)
								}
							}(ctx)
						}

						defer func() {
							// ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
							// if err := client.ReleaseReservation(ctx); err != nil {
							// 	logger.Error("error during reservation release", "error", err)
							// }
							//cancel()
							logger.Warn("removing actor count")
							m.DecrementActorCount()

						}()
						m.workerMutex.Lock()
						m.workerIDs = append(m.workerIDs, workerID)
						m.workerMutex.Unlock()
						logger.Debug("inside routine, starting work", "index", m.currentActorCount)
						return worker.Work(m.ctx)
					})
					m.logger.Info("added worker with index", "index", index, "worker_count", m.currentActorCount)
					m.IncrementActorCount()
				}
			}
		}
	}
}

func (m *Manager) IncrementActorCount() {
	m.actorCountMutex.Lock()
	m.currentActorCount++
	m.actorCountMutex.Unlock()
}

func (m *Manager) DecrementActorCount() {
	m.actorCountMutex.Lock()
	m.currentActorCount--
	m.actorCountMutex.Unlock()
}

func (m *Manager) AddActorID(id string)    {}
func (m *Manager) RemoveActorID(id string) {}

func (m *Manager) CheckForAvailableShards(ctx context.Context) ([]types.Shard, error) {
	m.logger.Info("checking for available shards", "current_actors", m.currentActorCount, "max_actors", m.config.MaxActorCount)
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
		if !m.internalClient.IsReserved(reservations, shard) {
			availableShards = append(availableShards, shard)
		}
	}
	return availableShards, nil
}

func (m *Manager) shardsState(ctx context.Context) error {
	// do we need to check state again
	now := Now()
	if now.Before(m.cacheLastChecked.Add(m.config.ShardCacheDuration)) {
		m.logger.Debug("cache hasn't expired")
		return nil
	}

	out, err := m.config.KinesisClient.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &m.config.StreamARN,
	})
	if err != nil {
		return err
	}
	for _, shard := range out.StreamDescription.Shards {
		m.cachedShards[*shard.ShardId] = shard
	}
	m.logger.Info("cached shard state", "count", len(m.cachedShards), "current_actors", m.currentActorCount)
	m.cacheLastChecked = Now()
	return nil
}
