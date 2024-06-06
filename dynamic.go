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

var (
	ErrStreamNotAvailable = errors.New("stream status is either creating or deleting")
)

type ClientContextKey struct{}
type LoggerContextKey struct{}
type RecordProcessor = func(context.Context, *metamorphosisv1.Record) error

type Actor struct {
	id                   string
	mc                   *Client
	processor            RecordProcessor
	logger               *slog.Logger
	SleepAfterProcessing time.Duration
	batchSize            int32
}

func LoggerWithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, LoggerContextKey{}, logger)
}
func LoggerFromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(LoggerContextKey{}).(*slog.Logger)
	if !ok {
		return slog.Default()
	}
	return logger
}
func ClientWithContext(ctx context.Context, client *Client) context.Context {
	return context.WithValue(ctx, ClientContextKey{}, client)
}
func ClientFromContext(ctx context.Context) *Client {
	api, ok := ctx.Value(ClientContextKey{}).(*Client)
	if !ok {
		return nil
	}
	return api
}
func (a *Actor) Work(ctx context.Context) error {
	reservation := a.mc.CurrentReservation()
	if reservation == nil {
		return ErrMissingReservation
	}
	//TODO wait on parent
	a.logger = a.logger.With("shard_id", reservation.ShardID, "worker_id", reservation.WorkerID, "group_id", reservation.GroupID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			a.logger.Debug("checking reservation")
			// see if shard is still active
			if closed, err := a.mc.IsShardClosed(ctx); err != nil || closed {
				a.logger.Debug("shard is closed shutdown worker")
				if err != nil {
					continue
				}
				if err := a.mc.CloseShard(ctx); err != nil {
					a.logger.Error("could not close shard", "error", err)
					return err
				}
				// Exit out to let other actors spin up if new shars are ready
				return nil

			}

			a.logger.Debug("fetching record")
			records, err := a.mc.FetchRecords(ctx, a.batchSize)
			if err != nil {
				a.logger.Error("error fetching record(s)", "error", err, "batch_size", a.batchSize)
				return err
			}
			if records == nil || len(records) < 1 {
				a.logger.Warn("records is empty. sleeping for 30 seconds")
				time.Sleep(30 * time.Second)
				continue
			}
			for _, record := range records {
				if record == nil {
					a.logger.Debug("record is nil")
					continue
				}
				a.logger.Debug("processing record", "record", record)

				cx := LoggerWithContext(ctx, a.logger)
				cx = ClientWithContext(cx, a.mc)
				// ctxWithLogger := context.WithValue(ctxWithClient, LoggerContextKey{}, a.logger)
				if err := a.processor(cx, record); err != nil {
					a.mc.ClearIterator()
					a.logger.Error("error processing record", "error", err, "processor", a.processor)
					return err
				}
				a.logger.Debug("commiting record")
				if err := a.mc.CommitRecord(ctx, record); err != nil {
					a.logger.Error("error commiting record", "error", err)
					a.mc.ClearIterator()
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
	config         *Config
	actors         *errgroup.Group
	logger         *slog.Logger
	ctx            context.Context
	internalClient *Client

	workerIDs   []string
	workerMutex sync.Mutex

	currentActorCount int
	actorCountMutex   sync.Mutex

	cacheLastChecked time.Time
	cachedShards     map[string]types.Shard
}

func New(config *Config) *Manager {
	return &Manager{
		currentActorCount: 0,
		config:            config,
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
func (m *Manager) RefreshActors(ctx context.Context) error {
	shards, err := m.CheckForAvailableShards(ctx)
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
			m.logger.Info("adding actor", "current_count", m.currentActorCount, "current_workers", m.workerIDs, "prefix", m.config.WorkerPrefix)
			m.actorCountMutex.Lock()
			index := m.currentActorCount
			m.actors.Go(func() error {
				workerID := fmt.Sprintf("%s.%d", m.config.WorkerPrefix, index)
				m.logger.Info("setting up actor inside error group", "index", index, "worker_id", workerID)
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

				// reserves shard to client
				err := client.Init(ctx)
				if err != nil {
					if errors.Is(err, ErrShardReserved) {
						logger.Info("exiting out of routine, shard not available.", "error", err)
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

				return actor.Work(m.ctx)
			})
			m.logger.Info("added worker with index", "index", index, "worker_count", m.currentActorCount)
			m.IncrementActorCount()
			m.actorCountMutex.Unlock()
		}
	}
	return nil
}
func (m *Manager) RefreshActorLoop(ctx context.Context) error {
	slog.Warn("refreshing actors")
	if err := m.RefreshActors(ctx); err != nil {
		slog.Error("error refreshing actors", "error", err)
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

func (m *Manager) AddActorID(id string)    {}
func (m *Manager) RemoveActorID(id string) {}

// TODO - check to see if shard is closed.
func (m *Manager) CheckForAvailableShards(ctx context.Context) ([]types.Shard, error) {
	m.logger.Info("checking for available shards", "current_actors", m.currentActorCount, "max_actors", m.config.MaxActorCount)
	// List Shards
	if err := m.shardsState(ctx); err != nil {
		m.logger.Error("error with stream/shards state", "error", err)
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

	out, err := m.config.KinesisClient.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamARN: &m.config.StreamARN,
	})
	if err != nil {
		return err
	}

	status := out.StreamDescriptionSummary.StreamStatus
	//m.logger.Warn("stream status", "shard_count", len(out.StreamDescription.Shards), "status", out.StreamDescription.StreamStatus)
	if status == types.StreamStatusCreating || status == types.StreamStatusDeleting {
		return ErrStreamNotAvailable
	}
	shards, err := m.getShards(ctx)
	if err != nil {
		return err
	}
	for _, shard := range shards {
		m.cachedShards[*shard.ShardId] = shard
		//m.logger.Warn("shard status", "id", *shard.ShardId, "parent", *shard.ParentShardId, "has_more_shard", *out.StreamDescription.HasMoreShards)
	}
	m.logger.Info("cached shard state", "count", len(m.cachedShards), "current_actors", m.currentActorCount)
	m.cacheLastChecked = Now()
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

func (m *Manager) Shutdown(ctx context.Context) error {
	return nil
}
