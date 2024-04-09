package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

var (
	ErrInvalidConfiguration = errors.New("invalid metamorphosis config")
)

type Config struct {
	// required fields
	GroupID          string
	WorkerID         string
	StreamARN        string
	ReservationTable string

	// optional fields
	// StartingSequence   string
	ShardID            string
	ReservationTimeout time.Duration
	RenewTime          time.Duration
	kinesisClient      KinesisAPI
	dynamoClient       DynamoDBAPI

	recordProcessor    RecordProcessor
	logger             *slog.Logger
	shardCacheDuration time.Duration
	maxActorCount      int
	workerPrefix       string
}

func NewConfig() *Config {
	return &Config{
		logger:        slog.Default(),
		maxActorCount: 1,
	}
}
func (c *Config) Copy() *Config {
	slog.Info("config before copy", "c", c)
	return &Config{
		GroupID:            c.GroupID,
		WorkerID:           c.WorkerID,
		StreamARN:          c.StreamARN,
		ReservationTable:   c.ReservationTable,
		ShardID:            c.ShardID,
		ReservationTimeout: c.ReservationTimeout,
		RenewTime:          c.RenewTime,
	}
}
func (c *Config) WithGroup(id string) *Config {
	c.GroupID = id
	return c
}
func (c *Config) WithWorkerID(id string) *Config {
	c.WorkerID = id
	return c
}
func (c *Config) WithShardID(id string) *Config {
	c.ShardID = id
	return c
}
func (c *Config) WithStreamArn(arn string) *Config {
	c.StreamARN = arn
	return c
}
func (c *Config) WithTableName(table string) *Config {
	c.ReservationTable = table
	return c
}
func (c *Config) WithReservationTimeout(d time.Duration) *Config {
	c.ReservationTimeout = d
	return c
}
func (c *Config) WithRenewTime(d time.Duration) *Config {
	c.RenewTime = d
	return c
}

func (c *Config) WithKinesisClient(client KinesisAPI) *Config {
	c.kinesisClient = client
	return c
}
func (c *Config) WithDynamoClient(client DynamoDBAPI) *Config {
	c.dynamoClient = client
	return c
}
func (c *Config) WithProcessor(p RecordProcessor) *Config {
	c.recordProcessor = p
	return c
}

func (c *Config) WithLogger(l *slog.Logger) *Config {
	c.logger = l
	return c
}

func (c *Config) WithShardCacheDuration(d time.Duration) *Config {
	c.shardCacheDuration = d
	return c
}

func (c *Config) WithMaxActorCount(actors int) *Config {
	c.maxActorCount = actors
	return c
}

func (c *Config) WithPrefix(id string) *Config {
	c.workerPrefix = id
	return c
}

func (c *Config) Validate() error {
	if c.GroupID == "" {
		return fmt.Errorf("groupID must be present: %w", ErrInvalidConfiguration)
	}
	if c.WorkerID == "" && c.workerPrefix == "" {
		return fmt.Errorf("workerID or prefix must be present: %w", ErrInvalidConfiguration)
	}
	if c.StreamARN == "" {
		return fmt.Errorf("kinesis stream arn must be present and valid: %w", ErrInvalidConfiguration)
	}
	if c.ReservationTable == "" {
		return fmt.Errorf("reservation table name must be present: %w", ErrInvalidConfiguration)
	}
	return nil
}
func (c *Config) Bootstrap(ctx context.Context) error {
	if c.ReservationTimeout == (0 * time.Second) {
		c.ReservationTimeout = 1 * time.Minute
	}

	if c.kinesisClient == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			slog.Error("could not load aws config")
			return err
		}
		c.kinesisClient = kinesis.NewFromConfig(cfg)
	}
	if c.dynamoClient == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			slog.Error("could not load aws config")
			return err
		}
		c.dynamoClient = dynamodb.NewFromConfig(cfg)
	}
	return nil
}
