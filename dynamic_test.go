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
	"github.com/stretchr/testify/mock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
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
	cl := NewClient(config)
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
	m.internalClient = NewClient(config)
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
	t.SkipNow()
	n := time.Now()
	Now = func() time.Time {
		return n
	}
	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)
	dc := mocks.NewDynamoDBAPI(t)
	kc := mocks.NewKinesisAPI(t)
	config := testConfig().WithKinesisClient(kc).WithDynamoClient(dc).WithMaxActorCount(1).WithPrefix("worker")
	config.SleepAfterProcessing = 10 * time.Millisecond
	config.MangerLoopWaitTime = 100 * time.Millisecond
	config.recordProcessor = func(ctx context.Context, record *metamorphosisv1.Record) error {
		return nil
	}
	m := New(context.Background(), config)
	m.internalClient = NewClient(config)

	// mock get available shards
	kc.EXPECT().DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: aws.String("arn"),
	}).
		Return(&kinesis.DescribeStreamOutput{
			StreamDescription: &types.StreamDescription{
				Shards: []types.Shard{
					{ShardId: aws.String("shard1")},
				},
			},
		}, nil)

	// mock get reservations
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

	expires := n.Unix()

	// mock fetch reservation
	dc.EXPECT().GetItem(mock.AnythingOfType("*context.cancelCtx"), &dynamodb.GetItemInput{}).Return(&dynamodb.GetItemOutput{
		Item: map[string]dtypes.AttributeValue{
			"groupID":        &dtypes.AttributeValueMemberS{Value: "group"},
			"shardID":        &dtypes.AttributeValueMemberS{Value: "shardID"},
			"workerID":       &dtypes.AttributeValueMemberS{Value: "worker"},
			"expiresAt":      &dtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires)},
			"latestSequence": &dtypes.AttributeValueMemberS{Value: "sequence1"},
		},
	}, nil).Maybe()

	// mock reserve shard
	dc.EXPECT().UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("table"),
		Key: map[string]dtypes.AttributeValue{
			GroupIDKey: &dtypes.AttributeValueMemberS{Value: "group"},
			ShardIDKey: &dtypes.AttributeValueMemberS{Value: "shard1"},
		},
		ConditionExpression: aws.String("(attribute_not_exists (#0)) OR (#1 = :0) OR (#0 < :1)"),
		UpdateExpression:    aws.String("SET #1 = :2, #0 = :3\n"),
		ExpressionAttributeNames: map[string]string{
			"#0": "expiresAt",
			"#1": "workerID",
		},
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":0": &dtypes.AttributeValueMemberS{Value: "worker.0"},
			":1": &dtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires)},
			":2": &dtypes.AttributeValueMemberS{Value: "worker.0"},
			":3": &dtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires+1)},
		},
		ReturnValues: dtypes.ReturnValueAllNew,
	}).Return(&dynamodb.UpdateItemOutput{
		Attributes: map[string]dtypes.AttributeValue{
			"groupID":        &dtypes.AttributeValueMemberS{Value: "group"},
			"shardID":        &dtypes.AttributeValueMemberS{Value: "shardID"},
			"workerID":       &dtypes.AttributeValueMemberS{Value: "worker"},
			"expiresAt":      &dtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires+1)},
			"latestSequence": &dtypes.AttributeValueMemberS{Value: "sequence"},
		},
	}, nil)

	// mock commit record
	dc.EXPECT().UpdateItem(mock.AnythingOfType("*context.cancelCtx"), &dynamodb.UpdateItemInput{

		TableName: &config.ReservationTable,
		Key: map[string]dtypes.AttributeValue{
			GroupIDKey: &dtypes.AttributeValueMemberS{Value: config.GroupID},
			ShardIDKey: &dtypes.AttributeValueMemberS{Value: "shard1"},
		},
		ConditionExpression: aws.String("#0 = :0"),
		UpdateExpression:    aws.String("SET #1 = :1, #2 = :2\n"),
		ExpressionAttributeNames: map[string]string{
			"#0": "workerID",
			"#1": "expiresAt",
			"#2": "latestSequence",
		},
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":0": &dtypes.AttributeValueMemberS{Value: "worker.0"},
			":2": &dtypes.AttributeValueMemberS{Value: "1"},
			":1": &dtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires+1)},
		},
		ReturnValues: dtypes.ReturnValueAllNew,
	}).Return(&dynamodb.UpdateItemOutput{}, nil).Maybe()

	kc.EXPECT().GetShardIterator(mock.AnythingOfType("*context.cancelCtx"), &kinesis.GetShardIteratorInput{
		ShardId:                aws.String("shard1"),
		ShardIteratorType:      types.ShardIteratorTypeAfterSequenceNumber,
		StartingSequenceNumber: aws.String("sequence"),
		StreamARN:              aws.String("arn"),
	}).
		Return(&kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("1"),
		}, nil)

	metaRecord := metamorphosisv1.Record{
		Id: "partitionKey",
		Headers: map[string]string{
			"version": "2",
		},
		Body: []byte(`{"test":"json"}`),
	}
	data, err := proto.Marshal(&metaRecord)
	must.NoError(t, err)
	kc.EXPECT().GetRecords(mock.AnythingOfType("*context.cancelCtx"), &kinesis.GetRecordsInput{
		StreamARN:     aws.String("arn"),
		Limit:         aws.Int32(1),
		ShardIterator: aws.String("1"),
	}).
		Return(&kinesis.GetRecordsOutput{
			Records: []types.Record{
				{
					SequenceNumber: aws.String("1"),
					PartitionKey:   aws.String("partitionKey"),
					Data:           data,
				},
			},
		}, nil).Once()

	eg.Go(func() error {
		return m.Loop(ctx)
	})
	eg.Go(func() error {
		time.Sleep(109 * time.Millisecond)
		defer cancel()
		return nil
	})
	err = eg.Wait()
	must.NoError(t, err)
	must.Eq(t, m.currentActorCount, 1)
}
