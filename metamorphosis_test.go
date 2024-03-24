package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/shoenig/test/must"
	"github.com/stretchr/testify/mock"

	"github.com/binarymatt/metamorphosis/mocks"
)

func testConfig() *Config {
	return &Config{
		GroupID:            "group",
		WorkerID:           "worker",
		StreamARN:          "arn",
		ReservationTable:   "table",
		ShardID:            "shardID",
		ReservationTimeout: 1 * time.Second,
	}

}
func TestInit_InvalidConfig(t *testing.T) {
	dc := mocks.NewDynamoDBAPI(t)
	config := &Config{}
	config = config.WithDynamoClient(dc)
	c := New(config, 0)
	closer, err := c.Init(context.Background())
	must.ErrorIs(t, err, ErrInvalidConfiguration)
	must.Nil(t, closer)
}

func TestInit_ReserveShard(t *testing.T) {
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	dc.On("UpdateItem", ctx, mock.Anything).Return(&dynamodb.UpdateItemOutput{
		Attributes: map[string]types.AttributeValue{
			"groupID": &types.AttributeValueMemberS{
				Value: "testGroup",
			},
		},
	}, nil)
	config := testConfig().WithDynamoClient(dc)
	c := New(config, 0)
	must.Nil(t, c.reservation)
	closer, err := c.Init(ctx)
	must.NoError(t, err)
	must.NotNil(t, closer)
	must.NotNil(t, c.reservation)
	must.Eq(t, &Reservation{
		GroupID: "testGroup",
	}, c.reservation)
}

func TestReserveShard(t *testing.T) {
	now := time.Now()
	Now = func() time.Time {
		return now
	}
	ctx := context.Background()
	config := testConfig()
	expires := now.Unix()
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String("table"),
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: "group"},
			ShardIDKey: &types.AttributeValueMemberS{Value: "shardID"},
		},
		ConditionExpression: aws.String("(attribute_not_exists (#0)) OR (#1 = :0) OR (#0 < :1)"),
		UpdateExpression:    aws.String("SET #1 = :2, #0 = :3\n"),
		ExpressionAttributeNames: map[string]string{
			"#0": "expiresAt",
			"#1": "workerID",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":0": &types.AttributeValueMemberS{Value: "worker"},
			":1": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires)},
			":2": &types.AttributeValueMemberS{Value: "worker"},
			":3": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires+1)},
		},
		ReturnValues: types.ReturnValueAllNew,
	}
	dynamoErr := errors.New("oops")
	cases := []struct {
		name          string
		err           error
		expectedError error
		out           *dynamodb.UpdateItemOutput
		reservation   *Reservation
	}{
		{
			name: "happy path",
			out: &dynamodb.UpdateItemOutput{
				Attributes: map[string]types.AttributeValue{
					"groupID":        &types.AttributeValueMemberS{Value: "group"},
					"shardID":        &types.AttributeValueMemberS{Value: "shardID"},
					"workerID":       &types.AttributeValueMemberS{Value: "worker"},
					"expiresAt":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires+1)},
					"latestSequence": &types.AttributeValueMemberS{Value: "sequence"},
				},
			},
			reservation: &Reservation{
				GroupID:        "group",
				ShardID:        "shardID",
				WorkerID:       "worker",
				ExpiresAt:      expires + 1,
				LatestSequence: "sequence",
			},
		},
		{
			name:          "already reserved",
			err:           &types.ConditionalCheckFailedException{},
			expectedError: ErrShardReserved,
		},
		{
			name:          "dynamo error",
			err:           dynamoErr,
			expectedError: dynamoErr,
		},
	}
	for _, tc := range cases {
		must.True(t, t.Run(tc.name, func(t *testing.T) {

			dc := mocks.NewDynamoDBAPI(t)
			dc.EXPECT().UpdateItem(ctx, input).Return(tc.out, tc.err).Once()
			m := New(config.WithDynamoClient(dc), 0)
			err := m.reserveShard(ctx)
			if tc.expectedError == nil {
				must.NoError(t, err)
			} else {
				must.ErrorIs(t, err, tc.expectedError)
			}
			must.Eq(t, tc.reservation, m.reservation)
		}))
	}
}

func TestReleaseReservation(t *testing.T) {
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	config := testConfig().WithDynamoClient(dc)
	m := New(config, 0)
	input := &dynamodb.UpdateItemInput{
		TableName: &config.ReservationTable,
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: config.GroupID},
			ShardIDKey: &types.AttributeValueMemberS{Value: config.ShardID},
		},
		ConditionExpression: aws.String("#0 = :0"),
		UpdateExpression:    aws.String("SET #0 = :1, #1 = :2\n"),
		ExpressionAttributeNames: map[string]string{
			"#0": "workerID",
			"#1": "expiresAt",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":0": &types.AttributeValueMemberS{Value: "worker"},
			":1": &types.AttributeValueMemberS{Value: "worker"},
			":2": &types.AttributeValueMemberN{Value: "0"},
		},
	}
	dc.EXPECT().UpdateItem(ctx, input).Return(nil, nil).Once()
	err := m.releaseReservation(ctx)
	must.NoError(t, err)
}
func TestReleaseReservation_Error(t *testing.T) {
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	config := testConfig().WithDynamoClient(dc)
	m := New(config, 0)
	input := &dynamodb.UpdateItemInput{
		TableName: &config.ReservationTable,
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: config.GroupID},
			ShardIDKey: &types.AttributeValueMemberS{Value: config.ShardID},
		},
		ConditionExpression: aws.String("#0 = :0"),
		UpdateExpression:    aws.String("SET #0 = :1, #1 = :2\n"),
		ExpressionAttributeNames: map[string]string{
			"#0": "workerID",
			"#1": "expiresAt",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":0": &types.AttributeValueMemberS{Value: "worker"},
			":1": &types.AttributeValueMemberS{Value: "worker"},
			":2": &types.AttributeValueMemberN{Value: "0"},
		},
	}
	oops := errors.New("didn't work")
	dc.EXPECT().UpdateItem(ctx, input).Return(nil, oops).Once()
	err := m.releaseReservation(ctx)
	must.ErrorIs(t, err, oops)
}

func TestIsReserved(t *testing.T) {
	reservations := []Reservation{}
	shard := ktypes.Shard{
		ShardId: aws.String("1"),
	}
	must.False(t, isReserved(reservations, shard))
	reservations = append(reservations, Reservation{
		ShardID: "1",
	})
	must.True(t, isReserved(reservations, shard))
}

func TestRetrieveRandomShardID(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		setup  func(*mocks.DynamoDBAPI, *mocks.KinesisAPI)
		err    error
		shard  string
		offset int
	}{
		{
			name: "no reservations",
			setup: func(dc *mocks.DynamoDBAPI, kc *mocks.KinesisAPI) {

				dc.EXPECT().Query(ctx, mock.AnythingOfType("*dynamodb.QueryInput")).Return(&dynamodb.QueryOutput{}, nil).Once()
				kc.EXPECT().ListShards(ctx, mock.AnythingOfType("*kinesis.ListShardsInput")).
					Return(&kinesis.ListShardsOutput{
						Shards: []ktypes.Shard{
							{ShardId: aws.String("1")},
						},
					}, nil)
			},
			shard: "1",
		},
		{
			name: "all reservered",
			setup: func(dc *mocks.DynamoDBAPI, kc *mocks.KinesisAPI) {

				dc.EXPECT().Query(ctx, mock.AnythingOfType("*dynamodb.QueryInput")).
					Return(&dynamodb.QueryOutput{
						Items: []map[string]types.AttributeValue{
							{
								"groupID": &types.AttributeValueMemberS{
									Value: "group",
								},
								"shardID": &types.AttributeValueMemberS{
									Value: "1",
								},
								"WorkerID": &types.AttributeValueMemberS{
									Value: "worker",
								},
								"expiresAt": &types.AttributeValueMemberN{
									Value: "0",
								},
								"latestSequence": &types.AttributeValueMemberS{
									Value: "last",
								},
							},
						},
					}, nil).Once()
				kc.EXPECT().ListShards(ctx, mock.AnythingOfType("*kinesis.ListShardsInput")).
					Return(&kinesis.ListShardsOutput{
						Shards: []ktypes.Shard{
							{ShardId: aws.String("1")},
						},
					}, nil)
			},
			shard: "",
			err:   ErrAllShardsReserved,
		},
		{
			name: "offset",
			setup: func(dc *mocks.DynamoDBAPI, kc *mocks.KinesisAPI) {

				dc.EXPECT().Query(ctx, mock.AnythingOfType("*dynamodb.QueryInput")).
					Return(&dynamodb.QueryOutput{
						Items: []map[string]types.AttributeValue{},
					}, nil).Once()
				kc.EXPECT().ListShards(ctx, mock.AnythingOfType("*kinesis.ListShardsInput")).
					Return(&kinesis.ListShardsOutput{
						Shards: []ktypes.Shard{
							{ShardId: aws.String("1")},
						},
					}, nil)
			},
			shard:  "1",
			offset: 1,
		},
		{
			name: "list reservations error",
			setup: func(dc *mocks.DynamoDBAPI, kc *mocks.KinesisAPI) {

				dc.EXPECT().Query(ctx, mock.AnythingOfType("*dynamodb.QueryInput")).
					Return(&dynamodb.QueryOutput{}, errors.New("oops")).Once()
			},
			err: errors.New("oops"),
		},
		{
			name: "list shards error",
			setup: func(dc *mocks.DynamoDBAPI, kc *mocks.KinesisAPI) {

				dc.EXPECT().Query(ctx, mock.AnythingOfType("*dynamodb.QueryInput")).
					Return(&dynamodb.QueryOutput{
						Items: []map[string]types.AttributeValue{},
					}, nil).Once()
				kc.EXPECT().ListShards(ctx, mock.AnythingOfType("*kinesis.ListShardsInput")).
					Return(&kinesis.ListShardsOutput{}, errors.New("oops list shards error"))
			},
			err: errors.New("oops list shards error"),
		},
	}
	for _, tc := range cases {
		must.True(t, t.Run(tc.name, func(t *testing.T) {
			dc := mocks.NewDynamoDBAPI(t)
			kc := mocks.NewKinesisAPI(t)
			config := testConfig().WithDynamoClient(dc).WithKinesisClient(kc)
			tc.setup(dc, kc)
			m := New(config, tc.offset)
			s, err := m.retrieveRandomShardID(ctx)
			must.Eq(t, tc.err, err)
			must.Eq(t, tc.shard, s)
		}))
	}

}