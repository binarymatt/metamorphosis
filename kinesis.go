package metamorphosis

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/proto"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

const (
	ShardClosed = "SHARD_CLOSED"
)

var (
	ErrMissingReservation = errors.New("missing reservation")
	ErrStreamError        = errors.New("stream error")
)

type KinesisAPI interface {
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
	PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
	CreateStream(ctx context.Context, params *kinesis.CreateStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error)
	DeleteStream(ctx context.Context, params *kinesis.DeleteStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error)
	// DescribeStream(ctx context.Context, params *kinesis.DescribeStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error)
	DescribeStreamSummary(ctx context.Context, params *kinesis.DescribeStreamSummaryInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamSummaryOutput, error)
}

type PutRecordsRequest struct {
	Records    []*metamorphosisv1.Record
	StreamName *string
	StreamArn  *string
}

func (c *Client) IsIteratorClosed(ctx context.Context) (bool, error) {
	iterator, err := c.getShardIterator(ctx)
	if err != nil {
		return false, err
	}
	if iterator == nil {
		return true, nil
	}
	return false, nil
}

func (c *Client) getShardIterator(ctx context.Context) (*string, error) {
	kc := c.config.KinesisClient
	c.logger.Debug("getting shard iterator")
	if c.reservation == nil {
		return nil, ErrMissingReservation
	}
	now := c.clock.Now()
	expiredCache := false
	if now.After(c.iteratorCacheExpires) {
		c.logger.Warn("iterator cache time is up", "cacheExpires", c.iteratorCacheExpires, "now", now)
		expiredCache = true
	}
	input := &kinesis.GetShardIteratorInput{
		StreamARN: &c.config.StreamARN,
		ShardId:   &c.config.ShardID,
	}

	if c.reservation.LatestSequence != "" {
		input.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		input.StartingSequenceNumber = &c.reservation.LatestSequence
	} else {
		input.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	}
	c.logger.Debug("shard iterator input", "input", *input)
	if (c.nextIterator != nil && *c.nextIterator == "") || expiredCache {
		c.logger.Info("getting iterator from kinesis API endpoiont", "shard", c.config.ShardID)
		out, err := kc.GetShardIterator(ctx, input)
		if err != nil {
			c.logger.Error("error getting shard iterator", "error", err)
			return nil, err
		}
		c.logger.Debug("iterator result", "iterator", *out.ShardIterator, "last_sequence", c.reservation.LatestSequence, "shard", c.config.ShardID)
		c.nextIterator = out.ShardIterator
		c.iteratorCacheExpires = c.clock.Now().Add(2 * time.Minute)
	} else {
		c.logger.Warn("getting cached iterator", "shard", c.config.ShardID)
	}
	return c.nextIterator, nil
}

func (m *Client) ClearIterator() {
	m.nextIterator = nil
}
func (m *Client) PutRecords(ctx context.Context, req *PutRecordsRequest) error {
	m.logger.Debug("adding records to stream")
	kc := m.config.KinesisClient
	kinesisRecords := make([]types.PutRecordsRequestEntry, len(req.Records))
	for i, record := range req.Records {
		data, err := proto.Marshal(record)
		if err != nil {
			return err
		}
		entry := types.PutRecordsRequestEntry{
			PartitionKey: aws.String(record.Id),
			Data:         data,
		}
		kinesisRecords[i] = entry
	}
	params := &kinesis.PutRecordsInput{
		Records:    kinesisRecords,
		StreamARN:  req.StreamArn,
		StreamName: req.StreamName,
	}
	_, err := kc.PutRecords(ctx, params)
	return err
}

func (m *Client) FetchRecord(ctx context.Context) (*metamorphosisv1.Record, error) {
	m.logger.Debug("fetching single record")
	records, err := m.FetchRecords(ctx, 1)
	if err != nil {
		return nil, err
	}
	if len(records) > 0 {
		return records[0], nil
	}
	return nil, nil
}

func (m *Client) FetchRecords(ctx context.Context, maxRecords int32) ([]*metamorphosisv1.Record, error) {
	kc := m.config.KinesisClient
	if m.reservation == nil {
		m.logger.Error("local reservation not present", "shard", m.config.ShardID, "group", m.config.GroupKey())
		r, err := m.fetchClientReservation(ctx)
		if err != nil {
			return nil, err
		}
		m.reservation = r

	}
	m.logger.Debug("starting fetch records", "worker_id", m.reservation.WorkerID)
	iterator, err := m.getShardIterator(ctx)
	if err != nil {
		return nil, err
	}
	if iterator == nil {
		// TODO mark shard as closed
		m.logger.Warn("iterator is nil shard might be closed")
	} else {
		m.logger.Debug("iterator state", "iterator", *iterator)
	}
	input := &kinesis.GetRecordsInput{
		ShardIterator: iterator,
		StreamARN:     &m.config.StreamARN,
		Limit:         &maxRecords,
	}
	output, err := kc.GetRecords(ctx, input)
	if err != nil {
		m.logger.Error("error getting records from kinesis", "error", err)
		return nil, err
	}
	nextIterator := output.NextShardIterator
	records := make([]*metamorphosisv1.Record, len(output.Records))
	for i, kr := range output.Records {
		r, err := m.translateRecord(kr)
		if err != nil {
			m.logger.Error("could not get metamorphosis record from kinesis", "error", err)
			return nil, err
		}
		records[i] = r
	}
	level := slog.LevelInfo
	if len(records) != int(maxRecords) {
		level = slog.LevelWarn
	}
	m.logger.Log(ctx, level, "records fetched from stream", "stream", m.config.StreamARN, "shard", m.reservation.ShardID, "records", len(records))
	m.nextIterator = nextIterator
	return records, nil
}
func (m *Client) translateRecord(kinesisRecord types.Record) (*metamorphosisv1.Record, error) {
	var record metamorphosisv1.Record
	err := proto.Unmarshal(kinesisRecord.Data, &record)
	record.Sequence = *kinesisRecord.SequenceNumber
	record.Shard = m.config.ShardID
	return &record, err
}
