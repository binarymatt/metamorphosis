package ondemand

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"golang.org/x/sync/errgroup"

	"github.com/binarymatt/metamorphosis"
	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

type Worker struct {
	shardID   string
	mc        metamorphosis.API
	processor func(context.Context, *metamorphosisv1.Record) error
}

func (w *Worker) Work(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			record, err := w.mc.FetchRecord(ctx)
			if err != nil {
				return err
			}
			if err := w.processor(ctx, record); err != nil {
				return err
			}
			if err := w.mc.CommitRecord(ctx, record); err != nil {
				return err
			}
		}
	}
}

type Manager struct {
	commit       chan *metamorphosisv1.Record
	read         chan *metamorphosisv1.Record
	actors       *errgroup.Group
	actorCount   int
	maxActor     int
	ctx          context.Context
	kc           metamorphosis.KinesisAPI
	dc           metamorphosis.DynamoDBAPI
	config       metamorphosis.Config
	cachedShards map[string]types.Shard
}

func (m *Manager) Start(ctx context.Context) error {
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
	for {
		select {
		case <-ctx.Done():
			return nil
		default:

		}
	}
}

func (m *Manager) shardsState(ctx context.Context) error {
	out, err := m.kc.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &m.config.StreamARN,
	})
	if err != nil {
		return err
	}
	for _, shard := range out.StreamDescription.Shards {
		m.cachedShards[*shard.ShardId] = shard
	}
	return nil
}
