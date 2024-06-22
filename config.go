package metamorphosis

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var (
	ErrInvalidConfiguration = errors.New("invalid metamorphosis config")
)

// Config contains the lower level settings
type Config struct {
	// required fields
	GroupID       string
	WorkerID      string
	StreamARN     string
	KinesisClient KinesisAPI
	DynamoClient  DynamoDBAPI

	// optional fields
	ReservationTableName string
	ShardID              string
	ReservationTimeout   time.Duration
	RenewTime            time.Duration
	ManagerLoopWaitTime  time.Duration
	RecordProcessor      RecordProcessor
	Logger               *slog.Logger
	ShardCacheDuration   time.Duration
	MaxActorCount        int
	WorkerPrefix         string
	SleepAfterProcessing time.Duration
	Seed                 int
	BatchSize            int32
}

type Option func(*Config)

func NewConfig(opts ...Option) *Config {
	cfg := &Config{
		Logger:               slog.Default(),
		MaxActorCount:        1,
		RenewTime:            30 * time.Second,
		ReservationTimeout:   1 * time.Minute,
		ReservationTableName: "metamorphosis_reservations",
		ManagerLoopWaitTime:  1 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
func WithGroup(id string) Option {
	return func(c *Config) {
		c.GroupID = id
	}
}
func WithWorkerID(id string) Option {
	return func(c *Config) {
		c.WorkerID = id
	}
}
func WithShardID(id string) Option {
	return func(c *Config) {
		c.ShardID = id
	}
}
func WithStreamArn(arn string) Option {
	return func(c *Config) {
		c.StreamARN = arn
	}
}
func WithReservationTableName(name string) Option {
	return func(c *Config) {
		c.ReservationTableName = name
	}
}
func WithReservationTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.ReservationTimeout = d
	}
}
func WithRenewTime(d time.Duration) Option {
	return func(c *Config) {
		c.RenewTime = d
	}
}

func WithKinesisClient(client KinesisAPI) Option {
	return func(c *Config) {
		c.KinesisClient = client
	}
}
func WithDynamoClient(client DynamoDBAPI) Option {
	return func(c *Config) {
		c.DynamoClient = client
	}
}
func WithRecordProcessor(p RecordProcessor) Option {
	return func(c *Config) {
		c.RecordProcessor = p
	}
}

func WithLogger(l *slog.Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

func WithShardCacheDuration(d time.Duration) Option {
	return func(c *Config) {
		c.ShardCacheDuration = d
	}
}

func WithMaxActorCount(actors int) Option {
	return func(c *Config) {
		c.MaxActorCount = actors
	}
}

func WithWorkerPrefix(id string) Option {
	return func(c *Config) {
		c.WorkerPrefix = id
	}
}

func WithSeed(seed int) Option {
	return func(c *Config) {
		c.Seed = seed
	}
}

func WithManagerLoopWaitTime(d time.Duration) Option {
	return func(c *Config) {
		c.ManagerLoopWaitTime = d
	}
}

func WithBatchSize(size int32) Option {
	return func(c *Config) {
		c.BatchSize = size
	}
}

func (c *Config) Validate() error {
	if c.GroupID == "" {
		return fmt.Errorf("groupID must be present: %w", ErrInvalidConfiguration)
	}
	if c.WorkerID == "" && c.WorkerPrefix == "" {
		return fmt.Errorf("workerID or prefix must be present: %w", ErrInvalidConfiguration)
	}
	if c.StreamARN == "" {
		return fmt.Errorf("kinesis stream arn must be present and valid: %w", ErrInvalidConfiguration)
	}
	if c.ReservationTableName == "" {
		return fmt.Errorf("reservation table name must be present: %w", ErrInvalidConfiguration)
	}
	return nil
}

func (c *Config) GroupKey() string {
	return fmt.Sprintf("%s-%s", c.StreamARN, c.GroupID)
}
