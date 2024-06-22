package metamorphosis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/shoenig/test/must"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
	"github.com/binarymatt/metamorphosis/mocks"
)

func testConfig(opts ...Option) *Config {
	cfg := &Config{
		GroupID:              "group",
		WorkerID:             "worker",
		StreamARN:            "arn",
		ReservationTableName: "table",
		ShardID:              "shardID",
		ReservationTimeout:   1 * time.Second,
		Logger:               slog.Default(),
		ManagerLoopWaitTime:  1 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg

}
func testConfigWithMocks(t *testing.T) (*Config, *mocks.KinesisAPI, *mocks.DynamoDBAPI) {
	dc := mocks.NewDynamoDBAPI(t)
	kc := mocks.NewKinesisAPI(t)
	cfg := testConfig(WithDynamoClient(dc), WithKinesisClient(kc))
	return cfg, kc, dc
}
func TestInit_InvalidConfig(t *testing.T) {
	dc := mocks.NewDynamoDBAPI(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	config := &Config{
		Logger: logger,
	}
	config.DynamoClient = dc
	c := NewClient(config)
	err := c.Init(context.Background())
	must.ErrorIs(t, err, ErrInvalidConfiguration)
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
	config := testConfig(WithDynamoClient(dc))
	c := NewClient(config)
	must.Nil(t, c.reservation)
	err := c.Init(ctx)
	must.NoError(t, err)
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
			GroupIDKey: &types.AttributeValueMemberS{Value: "arn-group"},
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
					"groupID":        &types.AttributeValueMemberS{Value: "arn-group"},
					"shardID":        &types.AttributeValueMemberS{Value: "shardID"},
					"workerID":       &types.AttributeValueMemberS{Value: "worker"},
					"expiresAt":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires+1)},
					"latestSequence": &types.AttributeValueMemberS{Value: "sequence"},
				},
			},
			reservation: &Reservation{
				GroupID:        "arn-group",
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
			config.DynamoClient = dc
			m := NewClient(config)
			err := m.ReserveShard(ctx)
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
	config := testConfig()
	config.DynamoClient = dc
	m := NewClient(config)
	input := &dynamodb.UpdateItemInput{
		TableName: &config.ReservationTableName,
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: "arn-group"},
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
	err := m.ReleaseReservation(ctx)
	must.NoError(t, err)
}
func TestReleaseReservation_Error(t *testing.T) {
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	config := testConfig()
	config.DynamoClient = dc
	m := NewClient(config)
	input := &dynamodb.UpdateItemInput{
		TableName: &config.ReservationTableName,
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: "arn-group"},
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
	err := m.ReleaseReservation(ctx)
	must.ErrorIs(t, err, oops)
}

func TestIsReserved(t *testing.T) {
	c := Client{
		logger: slog.Default(),
	}
	reservations := []Reservation{}
	shard := ktypes.Shard{
		ShardId: aws.String("1"),
	}
	must.False(t, c.IsReserved(reservations, shard))
	reservations = append(reservations, Reservation{
		ShardID: "1",
	})
	must.True(t, c.IsReserved(reservations, shard))
}

func TestProtoInProto(t *testing.T) {
	first := &metamorphosisv1.Record{
		Id: "first",
	}
	data, err := proto.Marshal(first)
	must.NoError(t, err)
	second := &metamorphosisv1.Record{
		Id:   "second",
		Body: data,
	}
	s, err := proto.Marshal(second)
	must.NoError(t, err)
	s2 := metamorphosisv1.Record{}
	err = proto.Unmarshal(s, &s2)
	must.NoError(t, err)
	must.Eq(t, "second", s2.Id)

	f1 := metamorphosisv1.Record{}
	err = proto.Unmarshal(s2.Body, &f1)
	must.NoError(t, err)

	must.Eq(t, "first", f1.Id)

}
