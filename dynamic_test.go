package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/shoenig/test/must"
	"golang.org/x/sync/errgroup"

	"github.com/binarymatt/metamorphosis/mocks"
)

func TestNew(t *testing.T) {
	config := testConfig()
	m := New(context.Background(), config)
	must.NotNil(t, m)
	must.Eq(t, m.currentActorCount, 0)
	must.Eq(t, m.config, config)
}

func TestActorWork_NoReservation(t *testing.T) {
	config := testConfig()
	cl := NewClient(config, 0)
	a := Actor{
		id: "test",
		mc: cl,
	}
	err := a.Work(context.Background())
	must.ErrorIs(t, err, ErrMissingReservation)
}
func TestManager_shardStateCached(t *testing.T) {
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	kc := mocks.NewKinesisAPI(t)
	config := testConfig().WithKinesisClient(kc).WithDynamoClient(dc).WithShardCacheDuration(1 * time.Second)
	m := New(ctx, config)
	m.cacheLastChecked = time.Now()
	err := m.shardsState(ctx)
	must.NoError(t, err)
}

func TestManager_shardStateRefresh(t *testing.T) {
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	kc := mocks.NewKinesisAPI(t)
	config := testConfig().WithKinesisClient(kc).WithDynamoClient(dc).WithShardCacheDuration(1 * time.Second)
	m := New(ctx, config)
	m.cacheLastChecked = time.Now().Add(-1 * time.Hour)

	kc.EXPECT().DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: aws.String("arn"),
	}).Return(&kinesis.DescribeStreamOutput{
		StreamDescription: &types.StreamDescription{
			Shards: []types.Shard{
				{ShardId: aws.String("shard1")},
			},
		},
	}, nil)
	must.Eq(t, map[string]types.Shard{}, m.cachedShards)
	err := m.shardsState(ctx)
	must.NoError(t, err)
	expectedShardState := map[string]types.Shard{
		"shard1": {
			ShardId: aws.String("shard1"),
		},
	}
	must.Eq(t, expectedShardState, m.cachedShards)
}
func TestManager_shardStateKinesisError(t *testing.T) {

	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	kc := mocks.NewKinesisAPI(t)
	config := testConfig().WithKinesisClient(kc).WithDynamoClient(dc).WithShardCacheDuration(1 * time.Second)
	m := New(ctx, config)
	m.cacheLastChecked = time.Now().Add(-1 * time.Hour)

	oops := errors.New("oops")
	kc.EXPECT().DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: aws.String("arn"),
	}).Return(&kinesis.DescribeStreamOutput{}, oops)
	must.Eq(t, map[string]types.Shard{}, m.cachedShards)
	err := m.shardsState(ctx)
	must.ErrorIs(t, err, oops)
}
func TestManager_LoopNoShards(t *testing.T) {
	n := time.Now()
	Now = func() time.Time {
		return n
	}
	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)
	dc := mocks.NewDynamoDBAPI(t)
	kc := mocks.NewKinesisAPI(t)
	config := testConfig().WithKinesisClient(kc).WithDynamoClient(dc)
	config.MangerLoopWaitTime = 100 * time.Millisecond
	m := New(context.Background(), config)
	m.internalClient = NewClient(config, 0)
	kc.EXPECT().DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: aws.String("arn"),
	}).
		Return(&kinesis.DescribeStreamOutput{
			StreamDescription: &types.StreamDescription{},
		}, nil)
	dc.EXPECT().Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String("table"),
		ExpressionAttributeNames: map[string]string{
			"#0": "expiresAt",
			"#1": "groupID",
		},
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":0": &dtypes.AttributeValueMemberN{
				Value: fmt.Sprintf("%d", n.Unix()),
			},
			":1": &dtypes.AttributeValueMemberS{
				Value: "group",
			},
		},
		KeyConditionExpression: aws.String("#1 = :1"),
		FilterExpression:       aws.String("#0 > :0"),
	}).
		Return(&dynamodb.QueryOutput{}, nil)
	eg.Go(func() error {
		return m.Loop(ctx)
	})
	eg.Go(func() error {
		time.Sleep(200 * time.Millisecond)
		cancel()
		return nil
	})
	err := eg.Wait()
	must.NoError(t, err)
	must.Eq(t, m.currentActorCount, 0)
}

func TestManager_LoopAvailableShard(t *testing.T) {
	n := time.Now()
	Now = func() time.Time {
		return n
	}
	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)
	dc := mocks.NewDynamoDBAPI(t)
	kc := mocks.NewKinesisAPI(t)
	config := testConfig().WithKinesisClient(kc).WithDynamoClient(dc)
	config.MangerLoopWaitTime = 100 * time.Millisecond
	m := New(context.Background(), config)
	m.internalClient = NewClient(config, 0)
	kc.EXPECT().DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: aws.String("arn"),
	}).
		Return(&kinesis.DescribeStreamOutput{
			StreamDescription: &types.StreamDescription{},
		}, nil)
	dc.EXPECT().Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String("table"),
		ExpressionAttributeNames: map[string]string{
			"#0": "expiresAt",
			"#1": "groupID",
		},
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":0": &dtypes.AttributeValueMemberN{
				Value: fmt.Sprintf("%d", n.Unix()),
			},
			":1": &dtypes.AttributeValueMemberS{
				Value: "group",
			},
		},
		KeyConditionExpression: aws.String("#1 = :1"),
		FilterExpression:       aws.String("#0 > :0"),
	}).
		Return(&dynamodb.QueryOutput{

			Items: []map[string]dtypes.AttributeValue{
				{
					"groupID": &dtypes.AttributeValueMemberS{
						Value: "group",
					},
					"shardID": &dtypes.AttributeValueMemberS{
						Value: "shard",
					},
					"WorkerID": &dtypes.AttributeValueMemberS{
						Value: "worker",
					},
					"expiresAt": &dtypes.AttributeValueMemberN{
						Value: "0",
					},
					"latestSequence": &dtypes.AttributeValueMemberS{
						Value: "last",
					},
				},
			},
		}, nil)
	eg.Go(func() error {
		return m.Loop(ctx)
	})
	eg.Go(func() error {
		time.Sleep(200 * time.Millisecond)
		must.Eq(t, m.currentActorCount, 1)
		cancel()
		return nil
	})
	err := eg.Wait()
	must.NoError(t, err)
	must.Eq(t, m.currentActorCount, 0)
}
