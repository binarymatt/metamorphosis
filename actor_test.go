package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/shoenig/test/must"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
	"github.com/binarymatt/metamorphosis/mocks"
)

func setupTestClient(t *testing.T, opts ...Option) (*Client, *mocks.KinesisAPI, *mocks.DynamoDBAPI) {
	dc := mocks.NewDynamoDBAPI(t)
	kc := mocks.NewKinesisAPI(t)
	opts = append(opts, WithDynamoClient(dc), WithKinesisClient(kc))
	client := NewClient(testConfig(opts...))
	return client, kc, dc
}

func TestNewActor(t *testing.T) {
	cfg := testConfig()
	cfg.Logger = nil
	client := NewClient(cfg)
	shard := types.Shard{}
	actor := NewActor(shard, cfg, client)
	must.Eq(t, shard, actor.shard)
	must.NotNil(t, actor.logger)
	must.Eq(t, cfg.WorkerID, actor.id)
}
func TestWaitForParent_ShardClosed(t *testing.T) {
	Now = func() time.Time {
		return time.Now()
	}
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	client := NewClient(testConfig(WithDynamoClient(dc)))
	actor := &Actor{
		id: "test",
		mc: client,
		shard: types.Shard{
			ParentShardId: aws.String("parentShard"),
		},
		logger: slog.Default(),
	}

	dc.EXPECT().
		GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("table"),
			Key: map[string]dtypes.AttributeValue{
				"groupID": &dtypes.AttributeValueMemberS{Value: "arn-group"},
				"shardID": &dtypes.AttributeValueMemberS{Value: "parentShard"},
			},
		}).
		Return(&dynamodb.GetItemOutput{
			Item: map[string]dtypes.AttributeValue{
				"groupID":        &dtypes.AttributeValueMemberS{Value: "arn-group"},
				"shardID":        &dtypes.AttributeValueMemberS{Value: "parentShard"},
				"latestSequence": &dtypes.AttributeValueMemberS{Value: ShardClosed},
			},
		}, nil)

	err := actor.WaitForParent(ctx)
	must.NoError(t, err)
}

func TestWaitForParent_NoParentShard(t *testing.T) {
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	client := NewClient(testConfig(WithDynamoClient(dc)))
	actor := &Actor{
		id:     "test",
		mc:     client,
		shard:  types.Shard{},
		logger: slog.Default(),
	}
	err := actor.WaitForParent(ctx)
	must.NoError(t, err)
}

func TestWaitForParent_UnknownError(t *testing.T) {
	dc := mocks.NewDynamoDBAPI(t)
	client := NewClient(testConfig(WithDynamoClient(dc)))
	actor := &Actor{
		id: "test",
		mc: client,
		shard: types.Shard{
			ParentShardId: aws.String("parentShard"),
		},
		logger: slog.Default(),
	}
	ctx := context.Background()
	Now = func() time.Time {
		return time.Now()
	}
	ErrOops := errors.New("oops")

	dc.EXPECT().
		GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("table"),
			Key: map[string]dtypes.AttributeValue{
				"groupID": &dtypes.AttributeValueMemberS{Value: "arn-group"},
				"shardID": &dtypes.AttributeValueMemberS{Value: "parentShard"},
			},
		}).
		Return(&dynamodb.GetItemOutput{}, ErrOops)
	err := actor.WaitForParent(ctx)
	must.ErrorIs(t, err, ErrOops)

}
func TestWork_NoParentReservation(t *testing.T) {
	client, _, dc := setupTestClient(t)
	ctx := context.Background()
	Now = func() time.Time {
		return time.Now().Add(-29999 * time.Millisecond)
	}
	a := &Actor{
		id: "test",
		mc: client,
		shard: types.Shard{
			ParentShardId: aws.String("parentShard"),
		},
		logger: slog.Default(),
	}
	a.mc.reservation = &Reservation{}

	dc.EXPECT().
		GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("table"),
			Key: map[string]dtypes.AttributeValue{
				"groupID": &dtypes.AttributeValueMemberS{Value: "arn-group"},
				"shardID": &dtypes.AttributeValueMemberS{Value: "parentShard"},
			},
		}).
		Return(&dynamodb.GetItemOutput{}, nil)
	err := a.Work(ctx)
	must.NoError(t, err)
}

func TestWork_ErrorsCases(t *testing.T) {
	//dc := mocks.NewDynamoDBAPI(t)
	//client := NewClient(testConfig(WithDynamoClient(dc)))
	//client.reservation = &Reservation{}
	client, _, dc := setupTestClient(t)
	ctx := context.Background()
	Now = func() time.Time {
		return time.Now()
	}
	defaultSetup := func() *Actor {
		a := &Actor{
			id: "test",
			mc: client,
			shard: types.Shard{
				ParentShardId: aws.String("parentShard"),
			},
			logger: slog.Default(),
		}
		a.mc.reservation = &Reservation{}
		return a
	}

	errOops := errors.New("oops")
	cases := []struct {
		name         string
		expectations func()
		err          error
		setup        func() *Actor
	}{
		{
			name:  "unknown error",
			setup: defaultSetup,
			expectations: func() {
				dc.EXPECT().
					GetItem(ctx, &dynamodb.GetItemInput{
						TableName: aws.String("table"),
						Key: map[string]dtypes.AttributeValue{
							"groupID": &dtypes.AttributeValueMemberS{Value: "arn-group"},
							"shardID": &dtypes.AttributeValueMemberS{Value: "parentShard"},
						},
					}).
					Return(&dynamodb.GetItemOutput{}, errOops)
			},
			err: errOops,
		},
		{
			name: "missing reservation",
			setup: func() *Actor {
				a := defaultSetup()
				a.mc.reservation = nil
				return a
			},
			err: ErrMissingReservation,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectations != nil {
				tc.expectations()
			}
			a := tc.setup()
			err := a.Work(ctx)
			must.ErrorIs(t, err, tc.err)
		})
	}

}
func TestWork_ContextDone(t *testing.T) {
	client, _, _ := setupTestClient(t)
	client.reservation = &Reservation{}
	a := &Actor{
		id:     "test",
		mc:     client,
		shard:  types.Shard{},
		logger: slog.Default(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := a.Work(ctx)
	must.ErrorIs(t, err, context.Canceled)
}
func TestWork_ShardClosed(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	Now = func() time.Time {
		return now
	}

	client, _, dc := setupTestClient(t)
	client.reservation = &Reservation{}
	a := &Actor{
		id:     "test",
		mc:     client,
		shard:  types.Shard{},
		logger: slog.Default(),
	}
	// set iterator closed
	client.nextIterator = nil
	client.iteratorCacheExpires = now.Add(1 * time.Minute)

	// expect close shard call to dynamo
	dc.EXPECT().UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("table"),
		Key: map[string]dtypes.AttributeValue{
			GroupIDKey: &dtypes.AttributeValueMemberS{Value: "arn-group"},
			ShardIDKey: &dtypes.AttributeValueMemberS{Value: "shardID"},
		},
		UpdateExpression: aws.String("SET #0 = :0\n"),
		ExpressionAttributeNames: map[string]string{
			"#0": "latestSequence",
		},
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":0": &dtypes.AttributeValueMemberS{Value: ShardClosed},
		},
		ReturnValues: dtypes.ReturnValueNone,
	}).Return(nil, nil).Once()
	err := a.Work(ctx)
	must.NoError(t, err)
}

func TestProcessRecords_EmptyRecords(t *testing.T) {
	records := []*metamorphosisv1.Record{nil}
	ctx := context.Background()
	client, _, _ := setupTestClient(t)
	a := &Actor{
		id:     "test",
		mc:     client,
		shard:  types.Shard{},
		logger: slog.Default(),
	}
	err := a.processRecords(ctx, records)
	must.NoError(t, err)
}

func TestProcessRecords_HappyPath(t *testing.T) {
	n := time.Now()
	Now = func() time.Time {
		return n
	}
	unixNow := n.Unix()
	records := []*metamorphosisv1.Record{
		{Id: "test", Sequence: "testSequence"},
	}
	ctx := context.Background()
	processor := func(ctx context.Context, record *metamorphosisv1.Record) error {
		// TODO assert that logger exists
		// TODO assert that client exists
		// TODO assert that record is one passed in
		return nil
	}
	client, _, dc := setupTestClient(t)
	a := &Actor{
		id:        "test",
		mc:        client,
		shard:     types.Shard{},
		logger:    slog.Default(),
		processor: processor,
	}
	dc.EXPECT().UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("table"),
		Key: map[string]dtypes.AttributeValue{
			GroupIDKey: &dtypes.AttributeValueMemberS{Value: "arn-group"},
			ShardIDKey: &dtypes.AttributeValueMemberS{Value: "shardID"},
		},
		ConditionExpression: aws.String("#0 = :0"),
		ExpressionAttributeNames: map[string]string{
			"#0": WorkerIDKey,
			"#1": ExpiresAtKey,
			"#2": LatestSequenceKey,
		},
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":0": &dtypes.AttributeValueMemberS{Value: "worker"},
			":1": &dtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", unixNow+1)},
			":2": &dtypes.AttributeValueMemberS{Value: "testSequence"},
		},
		ReturnValues:     dtypes.ReturnValueAllNew,
		UpdateExpression: aws.String("SET #1 = :1, #2 = :2\n"),
	}).Return(&dynamodb.UpdateItemOutput{
		Attributes: map[string]dtypes.AttributeValue{},
	}, nil).Once()
	err := a.processRecords(ctx, records)
	must.NoError(t, err)
}

func TestProcessRecords_processor_fails(t *testing.T) {
	n := time.Now()
	Now = func() time.Time {
		return n
	}
	records := []*metamorphosisv1.Record{
		{Id: "test", Sequence: "testSequence"},
	}
	ctx := context.Background()
	errOops := errors.New("oops")
	processor := func(ctx context.Context, record *metamorphosisv1.Record) error {
		// TODO assert that logger exists
		// TODO assert that client exists
		// TODO assert that record is one passed in
		return errOops
	}
	client, _, _ := setupTestClient(t)
	client.nextIterator = aws.String("test")
	a := &Actor{
		id:        "test",
		mc:        client,
		shard:     types.Shard{},
		logger:    slog.Default(),
		processor: processor,
	}
	err := a.processRecords(ctx, records)
	must.ErrorIs(t, err, errOops)
	must.Nil(t, a.mc.nextIterator)
}

func TestProcessRecords_commit_fails(t *testing.T) {
	n := time.Now()
	Now = func() time.Time {
		return n
	}
	unixNow := n.Unix()
	records := []*metamorphosisv1.Record{
		{Id: "test", Sequence: "testSequence"},
	}
	ctx := context.Background()
	processor := func(ctx context.Context, record *metamorphosisv1.Record) error {
		return nil
	}
	errOops := errors.New("oops")
	client, _, dc := setupTestClient(t)
	client.nextIterator = aws.String("test")
	a := &Actor{
		id:        "test",
		mc:        client,
		shard:     types.Shard{},
		logger:    slog.Default(),
		processor: processor,
	}
	dc.EXPECT().UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("table"),
		Key: map[string]dtypes.AttributeValue{
			GroupIDKey: &dtypes.AttributeValueMemberS{Value: "arn-group"},
			ShardIDKey: &dtypes.AttributeValueMemberS{Value: "shardID"},
		},
		ConditionExpression: aws.String("#0 = :0"),
		ExpressionAttributeNames: map[string]string{
			"#0": WorkerIDKey,
			"#1": ExpiresAtKey,
			"#2": LatestSequenceKey,
		},
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":0": &dtypes.AttributeValueMemberS{Value: "worker"},
			":1": &dtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", unixNow+1)},
			":2": &dtypes.AttributeValueMemberS{Value: "testSequence"},
		},
		ReturnValues:     dtypes.ReturnValueAllNew,
		UpdateExpression: aws.String("SET #1 = :1, #2 = :2\n"),
	}).Return(&dynamodb.UpdateItemOutput{}, errOops).Once()
	err := a.processRecords(ctx, records)
	must.ErrorIs(t, err, errOops)
	must.Nil(t, a.mc.nextIterator)
}
