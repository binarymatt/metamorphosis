package metamorphosis

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/shoenig/test/must"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
	"github.com/binarymatt/metamorphosis/mocks"
)

func TestGetShardIterator(t *testing.T) {

	must.True(t, t.Run("missing reservation", func(t *testing.T) {
		kc := mocks.NewKinesisAPI(t)
		config := testConfig().WithKinesisClient(kc)
		m := New(config, 0)

		iterator, err := m.getShardIterator(context.Background())
		must.Nil(t, iterator)
		must.ErrorIs(t, err, ErrMissingReservation)
	}))
	must.True(t, t.Run("after sequence", func(t *testing.T) {
		kc := mocks.NewKinesisAPI(t)
		config := testConfig().WithKinesisClient(kc)
		m := New(config, 0)
		m.reservation = &Reservation{
			LatestSequence: "last",
		}
		kc.EXPECT().GetShardIterator(
			context.Background(),
			&kinesis.GetShardIteratorInput{
				StreamARN:              aws.String("arn"),
				ShardId:                aws.String("shardID"),
				ShardIteratorType:      types.ShardIteratorTypeAfterSequenceNumber,
				StartingSequenceNumber: aws.String("last"),
			},
		).Return(&kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("test"),
		}, nil)
		iterator, err := m.getShardIterator(context.Background())
		must.NoError(t, err)
		must.Eq(t, "test", *iterator)
	}))
	must.True(t, t.Run("trim horizon", func(t *testing.T) {
		kc := mocks.NewKinesisAPI(t)
		config := testConfig().WithKinesisClient(kc)
		m := New(config, 0)
		m.reservation = &Reservation{
			LatestSequence: "",
		}
		kc.EXPECT().GetShardIterator(
			context.Background(),
			&kinesis.GetShardIteratorInput{
				StreamARN:         aws.String("arn"),
				ShardId:           aws.String("shardID"),
				ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			},
		).Return(&kinesis.GetShardIteratorOutput{
			ShardIterator: aws.String("test"),
		}, nil)
		iterator, err := m.getShardIterator(context.Background())
		must.NoError(t, err)
		must.Eq(t, "test", *iterator)
	}))
	must.True(t, t.Run("kinesis error", func(t *testing.T) {
		kc := mocks.NewKinesisAPI(t)
		config := testConfig().WithKinesisClient(kc)
		m := New(config, 0)
		m.reservation = &Reservation{
			LatestSequence: "",
		}
		KinesisErr := errors.New("oops")
		kc.EXPECT().GetShardIterator(
			context.Background(),
			&kinesis.GetShardIteratorInput{
				StreamARN:         aws.String("arn"),
				ShardId:           aws.String("shardID"),
				ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			},
		).Return(&kinesis.GetShardIteratorOutput{}, KinesisErr).Once()
		iterator, err := m.getShardIterator(context.Background())
		must.Nil(t, iterator)
		must.ErrorIs(t, err, KinesisErr)
	}))
}

func TestPutRecords(t *testing.T) {
	kc := mocks.NewKinesisAPI(t)
	config := testConfig().WithKinesisClient(kc)
	m := New(config, 0)
	record := &metamorphosisv1.Record{
		Id:   "partitionKey",
		Body: []byte(`{"test":"json"}`),
	}
	raw, err := proto.Marshal(record)
	must.NoError(t, err)

	kc.EXPECT().PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamName: aws.String("test"),
		Records: []types.PutRecordsRequestEntry{
			{
				PartitionKey: aws.String("partitionKey"),
				Data:         raw,
			},
		},
	}).
		Return(&kinesis.PutRecordsOutput{}, nil).Once()
	err = m.PutRecords(context.Background(), &PutRecordsRequest{StreamName: aws.String("test"), Records: []*metamorphosisv1.Record{record}})
	must.NoError(t, err)
}

func TestFetchRecords(t *testing.T) {
	ctx := context.Background()

	kc := mocks.NewKinesisAPI(t)
	dc := mocks.NewDynamoDBAPI(t)
	config := testConfig().WithKinesisClient(kc).WithDynamoClient(dc)
	m := New(config, 0)

	dc.EXPECT().GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("table"),
		Key: map[string]dtypes.AttributeValue{
			GroupIDKey: &dtypes.AttributeValueMemberS{Value: "group"},
			ShardIDKey: &dtypes.AttributeValueMemberS{Value: "shardID"},
		},
	}).
		Return(&dynamodb.GetItemOutput{
			Item: map[string]dtypes.AttributeValue{
				"groupID": &dtypes.AttributeValueMemberS{
					Value: "testGroup",
				},
				"shardID": &dtypes.AttributeValueMemberS{
					Value: "testShard",
				},
				"workerID": &dtypes.AttributeValueMemberS{
					Value: "testWorker",
				},
				"expiresAt": &dtypes.AttributeValueMemberN{
					Value: "0",
				},
			},
		}, nil)

	kc.EXPECT().GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamARN:         aws.String("arn"),
		ShardId:           aws.String("shardID"),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	}).Return(&kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("iterator"),
	}, nil).Once()

	metaRecord := metamorphosisv1.Record{
		Id: "partitionKey",
		Headers: map[string]string{
			"version": "2",
		},
		Body: []byte(`{"test":"json"}`),
	}
	data, err := proto.Marshal(&metaRecord)
	must.NoError(t, err)
	kc.EXPECT().GetRecords(ctx, &kinesis.GetRecordsInput{
		StreamARN:     aws.String("arn"),
		Limit:         aws.Int32(1),
		ShardIterator: aws.String("iterator"),
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
	metaRecord.Sequence = "1"
	expectedRecords := []*metamorphosisv1.Record{&metaRecord}

	must.Nil(t, m.reservation)
	records, err := m.FetchRecords(ctx, 1)
	must.NotNil(t, m.reservation)
	must.NoError(t, err)
	must.Eq(t, expectedRecords, records, must.Cmp(protocmp.Transform()))
}
