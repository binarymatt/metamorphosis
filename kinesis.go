package metamorphosis

import (
	"context"
	"errors"

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

func (m *Client) IsShardClosed(ctx context.Context) (bool, error) {
	iterator, err := m.getShardIterator(ctx)
	if err != nil {
		return false, err
	}
	if iterator == nil {
		return true, nil
	}
	return false, nil
}

func (m *Client) getShardIterator(ctx context.Context) (*string, error) {
	kc := m.config.KinesisClient
	m.logger.Debug("getting shard iterator")
	if m.reservation == nil {
		return nil, ErrMissingReservation
	}
	input := &kinesis.GetShardIteratorInput{
		StreamARN: &m.config.StreamARN,
		ShardId:   &m.config.ShardID,
	}
	if m.reservation.LatestSequence != "" {
		input.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		input.StartingSequenceNumber = &m.reservation.LatestSequence
	} else {
		input.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	}
	m.logger.Debug("shard iterator input", "input", *input)
	if m.nextIterator == nil {
		m.logger.Warn("getting iterator from api")
		out, err := kc.GetShardIterator(ctx, input)
		if err != nil {
			m.logger.Error("error getting shard iterator", "error", err)
			return nil, err
		}
		m.nextIterator = out.ShardIterator
	}
	return m.nextIterator, nil
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

func (m *Client) FetchRecords(ctx context.Context, max int32) ([]*metamorphosisv1.Record, error) {
	kc := m.config.KinesisClient
	if m.reservation == nil {
		m.logger.Error("reservation not present")
		r, err := m.fetchReservation(ctx)
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
	}
	input := &kinesis.GetRecordsInput{
		ShardIterator: iterator,
		StreamARN:     &m.config.StreamARN,
		Limit:         &max,
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
	m.logger.Info("records fetched from stream", "stream", m.config.StreamARN, "shard", m.reservation.ShardID, "records", len(records))
	if nextIterator != nil {
		m.nextIterator = nextIterator
	} else {
		// TODO Mark shard as closed.
		// m.CloseShard(ctx)
	}
	return records, nil
}
func (m *Client) translateRecord(kinesisRecord types.Record) (*metamorphosisv1.Record, error) {
	var record metamorphosisv1.Record
	err := proto.Unmarshal(kinesisRecord.Data, &record)
	record.Sequence = *kinesisRecord.SequenceNumber
	record.Shard = m.config.ShardID
	return &record, err
}
