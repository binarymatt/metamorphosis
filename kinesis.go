package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/proto"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
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
	DescribeStream(ctx context.Context, params *kinesis.DescribeStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error)
}

type PutRecordsRequest struct {
	Records []*metamorphosisv1.Record
	Stream  string
}

func (m *Metamorphosis) getShardIterator(ctx context.Context) (*string, error) {
	kc := m.config.kinesisClient
	m.log.Info("getting shard iterator")
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
	m.log.Info("shard iterator input", "input", *input)
	out, err := kc.GetShardIterator(ctx, input)
	if err != nil {
		fmt.Println("error getting shard iterator", err)
		return nil, err
	}
	return out.ShardIterator, nil
}

func (m *Metamorphosis) PutRecords(ctx context.Context, req *PutRecordsRequest) error {
	slog.Info("adding records to stream")
	kc := m.config.kinesisClient
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
		Records: kinesisRecords,
	}
	params.StreamName = &req.Stream
	_, err := kc.PutRecords(ctx, params)
	return err
}

func (m *Metamorphosis) FetchRecord(ctx context.Context) (*metamorphosisv1.Record, error) {
	records, err := m.FetchRecords(ctx, 1)
	if err != nil {
		return nil, err
	}
	if len(records) > 0 {
		return records[0], nil
	}
	return nil, nil
}

func (m *Metamorphosis) FetchRecords(ctx context.Context, max int32) ([]*metamorphosisv1.Record, error) {
	kc := m.config.kinesisClient
	m.log.Info("starting fetch records", "reservation", m.reservation)
	if m.reservation == nil {
		m.log.Error("reservation not present")
		r, err := m.fetchReservation(ctx)
		if err != nil {
			return nil, err
		}
		m.reservation = r

	}
	iterator, err := m.getShardIterator(ctx)
	if err != nil {
		return nil, err
	}
	input := &kinesis.GetRecordsInput{
		ShardIterator: iterator,
		StreamARN:     &m.config.StreamARN,
		Limit:         &max,
	}
	output, err := kc.GetRecords(ctx, input)
	if err != nil {
		return nil, err
	}
	records := make([]*metamorphosisv1.Record, len(output.Records))
	for i, kr := range output.Records {
		r, err := m.translateRecord(kr)
		if err != nil {
			m.log.Error("could not get metamorphosis record from kinesis", "error", err)
			return nil, err
		}
		records[i] = r
	}
	m.log.Info("records fetched from stream", "stream", m.config.StreamARN, "shard", m.reservation.ShardID)

	return records, nil
}
func (m *Metamorphosis) translateRecord(kinesisRecord types.Record) (*metamorphosisv1.Record, error) {
	var record metamorphosisv1.Record
	err := proto.Unmarshal(kinesisRecord.Data, &record)
	record.Sequence = *kinesisRecord.SequenceNumber
	return &record, err
}
