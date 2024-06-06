package metamorphosis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/shoenig/test/must"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
	"github.com/binarymatt/metamorphosis/mocks"
)

func TestListReservations(t *testing.T) {
	n := time.Now()
	Now = func() time.Time {
		return n
	}
	dc := mocks.NewDynamoDBAPI(t)
	m := NewClient(NewConfig(
		WithReservationTableName("metamorphosis_reservations"),
		WithDynamoClient(dc),
		WithStreamArn("arn"),
		WithGroup("testGroup")))

	must.True(t, t.Run("happy path", func(t *testing.T) {
		dc.EXPECT().Query(context.Background(), &dynamodb.QueryInput{
			TableName: aws.String("metamorphosis_reservations"),
			ExpressionAttributeNames: map[string]string{
				"#0": "expiresAt",
				"#1": "groupID",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":0": &types.AttributeValueMemberN{
					Value: fmt.Sprintf("%d", n.Unix()),
				},
				":1": &types.AttributeValueMemberS{
					Value: "arn-testGroup",
				},
			},
			KeyConditionExpression: aws.String("#1 = :1"),
			FilterExpression:       aws.String("#0 > :0"),
		}).
			Return(&dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						"groupID": &types.AttributeValueMemberS{
							Value: "group",
						},
						"shardID": &types.AttributeValueMemberS{
							Value: "shard",
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
		reservations, err := m.ListReservations(context.Background())
		must.NoError(t, err)
		expectedReservations := []Reservation{
			{
				GroupID:        "group",
				ShardID:        "shard",
				WorkerID:       "worker",
				ExpiresAt:      0,
				LatestSequence: "last",
			},
		}
		must.Eq(t, expectedReservations, reservations)
	}))
}

func TestReservationExpires(t *testing.T) {
	n := time.Now()
	expected := time.Unix(n.Unix(), 0)
	r := Reservation{
		ExpiresAt: n.Unix(),
	}
	must.Eq(t, expected, r.Expires())
}

func TestCommitRecord(t *testing.T) {
	now := time.Now()
	Now = func() time.Time {
		return now
	}
	ctx := context.Background()
	dc := mocks.NewDynamoDBAPI(t)
	config := testConfig(WithDynamoClient(dc))
	expires := now.Add(config.ReservationTimeout)
	m := NewClient(config)
	input := &dynamodb.UpdateItemInput{
		TableName: &config.ReservationTableName,
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: "arn-group"},
			ShardIDKey: &types.AttributeValueMemberS{Value: config.ShardID},
		},
		ConditionExpression: aws.String("#0 = :0"),
		UpdateExpression:    aws.String("SET #1 = :1, #2 = :2\n"),
		ExpressionAttributeNames: map[string]string{
			"#0": "workerID",
			"#1": "expiresAt",
			"#2": "latestSequence",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":0": &types.AttributeValueMemberS{Value: "worker"},
			":2": &types.AttributeValueMemberS{Value: "sequence1"},
			":1": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires.Unix())},
		},
		ReturnValues: types.ReturnValueAllNew,
	}
	output := &dynamodb.UpdateItemOutput{
		Attributes: map[string]types.AttributeValue{
			"groupID":        &types.AttributeValueMemberS{Value: "group"},
			"shardID":        &types.AttributeValueMemberS{Value: "shardID"},
			"workerID":       &types.AttributeValueMemberS{Value: "worker"},
			"expiresAt":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", expires.Unix())},
			"latestSequence": &types.AttributeValueMemberS{Value: "sequence1"},
		},
	}
	dc.EXPECT().UpdateItem(ctx, input).Return(output, nil).Once()
	err := m.CommitRecord(ctx, &metamorphosisv1.Record{Sequence: "sequence1"})
	must.NoError(t, err)
	must.Eq(t, &Reservation{
		GroupID:        "group",
		ShardID:        "shardID",
		WorkerID:       "worker",
		ExpiresAt:      expires.Unix(),
		LatestSequence: "sequence1",
	}, m.reservation)
}
