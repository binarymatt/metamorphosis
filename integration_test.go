package metamorphosis

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/lmittmann/tint"
	"github.com/shoenig/test/must"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

const (
	tableName  = "metamorphosis_reservations_test"
	shard      = "shardId-000000000000"
	group      = "group"
	streamName = "test-stream"
	streamARN  = "arn:aws:kinesis:us-east-1:000000000000:stream/test-stream"
)

type IntegrationTestSuite struct {
	suite.Suite
	dc  DynamoDBAPI
	kc  KinesisAPI
	t   time.Time
	ctx context.Context
}

func (i *IntegrationTestSuite) SetupSuite() {
	logger := slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:     slog.LevelInfo,
		AddSource: true,
	}))
	slog.SetDefault(logger)
	i.dc = buildDynamoClient()
	i.kc = buildKinesisClient()
	i.createStream()

	for j := range 10 {
		i.addKinesisRecord(fmt.Sprintf("record%d", j))
	}
}

func (i *IntegrationTestSuite) TearDownSuite() {
	i.deleteStream()
}

func (i *IntegrationTestSuite) SetupTest() {
	i.ctx = context.Background()
	i.createTable()
	n := time.Now()
	Now = func() time.Time { return n }
	i.t = n

}

func (i *IntegrationTestSuite) TearDownTest() {
	i.deleteTable()
}

func TestIntegrationTests(t *testing.T) {
	runIntegration := os.Getenv("METAMORPHOSIS_INTEGRATION_TESTS")
	if runIntegration == "" {
		t.Skip("skipping test in short mode.")
	}
	suite.Run(t, new(IntegrationTestSuite))
}

func (i *IntegrationTestSuite) setupReservation(worker, sequence string, expiration int64) {
	reservation := &Reservation{
		GroupID:        group,
		WorkerID:       worker,
		ShardID:        shard,
		ExpiresAt:      expiration,
		LatestSequence: sequence,
	}
	item, err := attributevalue.MarshalMap(reservation)
	i.Require().NoError(err)

	_, err = i.dc.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	i.Require().NoError(err)

}

func (i *IntegrationTestSuite) getCurrentReservation(group, shard string) Reservation {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: group},
			ShardIDKey: &types.AttributeValueMemberS{Value: shard},
		},
	}
	out, err := i.dc.GetItem(context.Background(), input)
	i.Require().NoError(err)
	var reservation Reservation
	err = attributevalue.UnmarshalMap(out.Item, &reservation)
	i.Require().NoError(err)
	return reservation
}

func (i *IntegrationTestSuite) TestReserveShard_ExpiredReservation() {
	client := defaultClient("worker1")
	i.setupReservation("worker2", "latest", i.t.Add(-2*time.Minute).Unix())

	err := client.ReserveShard(context.Background())
	must.NoError(i.T(), err)

	expected := Reservation{
		GroupID:        group,
		WorkerID:       "worker1",
		ShardID:        shard,
		LatestSequence: "latest",
		ExpiresAt:      i.t.Add(client.config.ReservationTimeout).Unix(),
	}
	actual := i.getCurrentReservation(client.config.GroupID, client.config.ShardID)
	must.Eq(i.T(), expected, actual)
}
func (i *IntegrationTestSuite) TestReservedShard_NoReservation() {
	ctx := context.Background()
	client := defaultClient("worker1")
	err := client.ReserveShard(ctx)
	must.NoError(i.T(), err)

	expected := Reservation{
		GroupID:        group,
		ShardID:        shard,
		WorkerID:       "worker1",
		LatestSequence: "",
		ExpiresAt:      i.t.Add(client.config.ReservationTimeout).Unix(),
	}
	actual := i.getCurrentReservation(group, shard)
	must.Eq(i.T(), expected, actual)
}

func (i *IntegrationTestSuite) TestReserveshard_ExistingWorker() {
	ctx := context.Background()
	client := defaultClient("worker1")
	i.setupReservation("worker1", "", i.t.Add(-2*time.Minute).Unix())
	err := client.ReserveShard(ctx)
	must.NoError(i.T(), err)

	expected := Reservation{
		GroupID:        group,
		ShardID:        shard,
		WorkerID:       "worker1",
		LatestSequence: "",
		ExpiresAt:      i.t.Add(client.config.ReservationTimeout).Unix(),
	}
	actual := i.getCurrentReservation(group, shard)
	must.Eq(i.T(), expected, actual)
}
func (i *IntegrationTestSuite) TestReserveShard_ExistingReservationOtherWorker() {
	client := defaultClient("worker1")
	i.setupReservation("worker2", "", i.t.Add(1*time.Minute).Unix())
	err := client.ReserveShard(context.Background())
	must.ErrorIs(i.T(), err, ErrShardReserved)
}

func (i *IntegrationTestSuite) TestCommitRecord_Success() {

	ctx := context.Background()
	client := defaultClient("worker1")
	i.setupReservation("worker1", "firstRecord", i.t.Add(30*time.Second).Unix())
	err := client.CommitRecord(ctx, &metamorphosisv1.Record{
		Id:       "testRecord",
		Sequence: "lastRecord",
	})
	must.NoError(i.T(), err)

	expected := Reservation{
		GroupID:        group,
		ShardID:        shard,
		WorkerID:       "worker1",
		LatestSequence: "lastRecord",
		ExpiresAt:      i.t.Add(client.config.ReservationTimeout).Unix(),
	}
	actual := i.getCurrentReservation(group, shard)
	must.Eq(i.T(), expected, actual)
	must.Eq(i.T(), &actual, client.reservation)
}
func (i *IntegrationTestSuite) TestCommitRecord_Noreservation() {
	ctx := context.Background()
	client := defaultClient("worker1")
	err := client.CommitRecord(ctx, &metamorphosisv1.Record{
		Id:       "testRecord",
		Sequence: "lastRecord",
	})
	i.Require().ErrorIs(err, ErrShardReserved)
	i.Nil(client.reservation)

}
func (i *IntegrationTestSuite) TestFetchRecords_NoReservation() {
	ctx := context.Background()
	client := defaultClient("worker1")
	records, err := client.FetchRecords(ctx, 1)
	i.ErrorIs(err, ErrNotFound)
	i.Nil(records)
}
func (i *IntegrationTestSuite) TestFetchRecords_OneRecord() {
	mc := defaultClient("worker1")
	err := mc.Init(i.ctx)
	must.NoError(i.T(), err)

	records, err := mc.FetchRecords(i.ctx, 1)
	must.NoError(i.T(), err)
	must.Len(i.T(), 1, records)

	must.Eq(i.T(), "record0", records[0].Id)
	must.Eq(i.T(), map[string]string{"id": "record0", "name": "test"}, records[0].Headers)
	must.Eq(i.T(), []byte(`{"record_type":"json"}`), records[0].Body)
	must.NotEq(i.T(), "", records[0].Sequence)
}
func (i *IntegrationTestSuite) TestFetchRecords_MultipleRecords() {
	mc := defaultClient("worker1")
	err := mc.Init(i.ctx)
	must.NoError(i.T(), err)

	records, err := mc.FetchRecords(i.ctx, 5)
	must.NoError(i.T(), err)
	must.Len(i.T(), 5, records)

	must.Eq(i.T(), "record0", records[0].Id)
	must.Eq(i.T(), "record1", records[1].Id)
	must.Eq(i.T(), "record2", records[2].Id)
	must.Eq(i.T(), "record3", records[3].Id)
	must.Eq(i.T(), "record4", records[4].Id)
}

func (i *IntegrationTestSuite) TestFetchCommitLoop() {
	ctx := context.Background()

	client := defaultClient("loopWorker")
	err := client.Init(ctx)
	must.NoError(i.T(), err)

	// fetch and commit
	actual, err := client.FetchRecord(ctx)
	must.NoError(i.T(), err)
	must.NotNil(i.T(), actual)

	must.Eq(i.T(), "record0", actual.Id)

	err = client.CommitRecord(ctx, actual)
	must.NoError(i.T(), err)

	res := i.getCurrentReservation(group, shard)
	must.Eq(i.T(), actual.Sequence, res.LatestSequence)
	must.Eq(i.T(), actual.Sequence, client.reservation.LatestSequence)

	record, err := client.FetchRecord(ctx)
	must.NoError(i.T(), err)
	must.NotNil(i.T(), record)

	must.Eq(i.T(), "record1", record.Id)
}

func (i *IntegrationTestSuite) addKinesisRecord(id string) {
	k := &metamorphosisv1.Record{
		Id:      id,
		Headers: map[string]string{"name": "test", "id": id},
		Body:    []byte(`{"record_type":"json"}`),
	}
	data, err := proto.Marshal(k)
	must.NoError(i.T(), err)
	_, err = i.kc.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamARN: aws.String(streamARN),
		Records: []ktypes.PutRecordsRequestEntry{
			{PartitionKey: aws.String(id), Data: data},
		},
	})
	must.NoError(i.T(), err)
}
func defaultClient(workerID string) *Client {
	config := NewConfig(
		WithGroup(group),
		WithWorkerID(workerID),
		WithStreamArn(streamARN),
		WithReservationTableName(tableName),
		WithReservationTimeout(1*time.Minute),
		WithShardID(shard),
		WithDynamoClient(buildDynamoClient()),
		WithKinesisClient(buildKinesisClient()))

	return NewClient(config)
}

func buildDynamoClient() DynamoDBAPI {

	awsEndpoint := "http://localhost:4566"
	awsRegion := "us-east-1"

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           awsEndpoint,
			SigningRegion: awsRegion,
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "SECRET_KEY", "TOKEN")),
	)
	if err != nil {
		log.Fatalf("Cannot load the AWS configs: %s", err)
	}
	return dynamodb.NewFromConfig(awsCfg)
}
func buildKinesisClient() KinesisAPI {
	awsEndpoint := "http://localhost:4566"
	awsRegion := "us-east-1"

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           awsEndpoint,
			SigningRegion: awsRegion,
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "SECRET_KEY", "TOKEN")),
	)
	if err != nil {
		log.Fatalf("Cannot load the AWS configs: %s", err)
	}
	return kinesis.NewFromConfig(awsCfg)
}
func (i *IntegrationTestSuite) createTable() {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("groupID"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("shardID"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("groupID"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("shardID"),
				KeyType:       types.KeyTypeRange,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   aws.String(tableName),
	}
	_, err := i.dc.CreateTable(context.Background(), input)
	must.NoError(i.T(), err)

}

func (i *IntegrationTestSuite) deleteTable() {
	_, err := i.dc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	must.NoError(i.T(), err)
}

func (i *IntegrationTestSuite) createStream() {
	ctx := context.Background()
	input := &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
		StreamModeDetails: &ktypes.StreamModeDetails{
			StreamMode: ktypes.StreamModeProvisioned,
		},
	}
	_, err := i.kc.CreateStream(ctx, input)
	must.NoError(i.T(), err)
	slog.Info("created stream", "error", err)
	for j := 0; j < 20; j++ {
		out, err := i.kc.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamARN: aws.String(streamARN)})
		must.NoError(i.T(), err)
		slog.Info("getting stream status", "status", out.StreamDescriptionSummary.StreamStatus, "arn", *out.StreamDescriptionSummary.StreamARN)

		if out.StreamDescriptionSummary.StreamStatus == ktypes.StreamStatusActive {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (i *IntegrationTestSuite) deleteStream() {
	_, err := i.kc.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	must.NoError(i.T(), err)
}
