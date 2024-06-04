package metamorphosis

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

const (
	GroupIDKey        = "groupID"
	ShardIDKey        = "shardID"
	WorkerIDKey       = "workerID"
	ExpiresAtKey      = "expiresAt"
	LatestSequenceKey = "latestSequence"
)

var (
	ErrNotFound          = errors.New("reservation missing")
	ErrShardReserved     = errors.New("shard is already reserved")
	ErrAllShardsReserved = errors.New("all shards are reserved")
	Now                  = time.Now
)

type Reservation struct {
	// primary key
	GroupID string `dynamodbav:"groupID"`
	// secondary key
	ShardID string `dynamodbav:"shardID"`

	WorkerID       string `dynamodbav:"workerID"`
	ExpiresAt      int64  `dynamodbav:"expiresAt"`
	LatestSequence string `dynamodbav:"latestSequence"`
}

func (r *Reservation) Expires() time.Time {
	return time.Unix(r.ExpiresAt, 0)
}

type DynamoDBAPI interface {
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DeleteTable(ctx context.Context, params *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
}

func (m *Client) ListReservations(ctx context.Context) ([]Reservation, error) {
	m.logger.Info("listing reservations")
	client := m.config.DynamoClient
	keyCondition := expression.Key(GroupIDKey).Equal(expression.Value(m.config.GroupID))

	now := Now()
	filter := expression.Name(ExpiresAtKey).GreaterThan(expression.Value(now.Unix()))

	expr, err := expression.NewBuilder().
		WithKeyCondition(keyCondition).
		WithFilter(filter).
		Build()

	if err != nil {
		return nil, err
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(m.config.ReservationTableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
	}
	out, err := client.Query(ctx, input)
	if err != nil {
		m.logger.Error("error getting existing reservations")
		return nil, err
	}
	var reservations []Reservation
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &reservations); err != nil {
		return nil, err
	}
	return reservations, err
}
func (c *Client) ReserveShard(ctx context.Context) error {
	client := c.config.DynamoClient
	logger := c.logger.With("shard", c.config.ShardID, "timeout", c.config.ReservationTimeout, "worker", c.config.WorkerID, "group", c.config.GroupID)
	logger.Info("starting shard reservation")
	// 1. Is there an existing reservation for this group/worker, if so use that.
	// conditional PutItem
	now := Now()

	condition := expression.Or(
		expression.AttributeNotExists(expression.Name(ExpiresAtKey)),
		expression.Name(WorkerIDKey).Equal(expression.Value(c.config.WorkerID)),
		expression.Name(ExpiresAtKey).LessThan(expression.Value(now.Unix())),
	)

	expires := Now().Add(c.config.ReservationTimeout)
	update := expression.Set(expression.Name(WorkerIDKey), expression.Value(c.config.WorkerID)).
		Set(expression.Name(ExpiresAtKey), expression.Value(expires.Unix()))

	expr, err := expression.NewBuilder().
		WithCondition(condition).
		WithUpdate(update).
		Build()
	if err != nil {
		return err
	}
	input := &dynamodb.UpdateItemInput{
		TableName: &c.config.ReservationTableName,
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: c.config.GroupID},
			ShardIDKey: &types.AttributeValueMemberS{Value: c.config.ShardID},
		},
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}
	out, err := client.UpdateItem(ctx, input)
	var conditionFailed *types.ConditionalCheckFailedException
	if errors.As(err, &conditionFailed) {
		c.logger.Error("conditional check failed during shard reservation", "error", err)
		c.reservation = nil
		return ErrShardReserved

	}
	if err != nil {
		c.logger.Error("error reserving shard", "error", err)
		return err
	}
	var reservation Reservation
	if err := attributevalue.UnmarshalMap(out.Attributes, &reservation); err != nil {
		return err
	}
	c.reservation = &reservation
	c.logger = c.logger.With("shard_id", reservation.ShardID)
	c.logger.Info("reservation made", "expires", expires, "sequence", c.reservation.LatestSequence)
	return nil
}

func (m *Client) ReleaseReservation(ctx context.Context) error {
	client := m.config.DynamoClient
	condition := expression.Name(WorkerIDKey).Equal(expression.Value(m.config.WorkerID))

	update := expression.Set(expression.Name(WorkerIDKey), expression.Value(m.config.WorkerID)).
		Set(expression.Name(ExpiresAtKey), expression.Value(0))

	expr, err := expression.NewBuilder().
		WithCondition(condition).
		WithUpdate(update).
		Build()

	if err != nil {
		return err
	}

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &m.config.ReservationTableName,
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: m.config.GroupID},
			ShardIDKey: &types.AttributeValueMemberS{Value: m.config.ShardID},
		},
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	m.logger.Warn("reservation released", "error", err)
	return err
}

func (m *Client) RenewReservation(ctx context.Context) error {
	ticker := time.NewTicker(m.config.RenewTime)
	for {
		select {
		case <-ctx.Done():
			m.logger.Debug("reservation loop finished, context done")
			return nil
		case <-ticker.C:
			m.logger.Debug("trying reservation renewal")
			if err := m.ReserveShard(ctx); err != nil {
				return err
			}
		}
	}
}

func (m *Client) CommitRecord(ctx context.Context, record *metamorphosisv1.Record) error {
	sequence := record.Sequence
	client := m.config.DynamoClient

	condition := expression.Name(WorkerIDKey).Equal(expression.Value(m.config.WorkerID))

	expires := Now().Add(m.config.ReservationTimeout)
	update := expression.Set(expression.Name(ExpiresAtKey), expression.Value(expires.Unix())).
		Set(expression.Name(LatestSequenceKey), expression.Value(sequence))

	expr, err := expression.NewBuilder().
		WithCondition(condition).
		WithUpdate(update).
		Build()
	if err != nil {
		return err
	}

	input := &dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: m.config.GroupID},
			ShardIDKey: &types.AttributeValueMemberS{Value: m.config.ShardID},
		},
		TableName:                 &m.config.ReservationTableName,
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	m.logger.Debug("commiting position", "sequence", sequence, "reservation_expires", expires, "group", m.config.GroupID, "shard", m.config.ShardID, "worker", m.config.WorkerID)
	out, err := client.UpdateItem(ctx, input)
	if err != nil {
		var conditionFailed *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailed) {
			m.logger.Error("conditional check failed during commit ", "error", err)
			m.reservation = nil
			return ErrShardReserved

		}
		m.logger.Error("could not commit position", "error", err)
		return err
	}
	var reservation Reservation
	if err := attributevalue.UnmarshalMap(out.Attributes, &reservation); err != nil {
		return err
	}
	m.logger.Debug("setting reservation", "reservation", reservation)
	m.reservation = &reservation
	return nil
}

func (m *Client) fetchReservation(ctx context.Context) (*Reservation, error) {
	client := m.config.DynamoClient

	m.logger.Info("fetching reservation", "group", m.config.GroupID, "shard", m.config.ShardID, "table", m.config.ReservationTableName)
	input := &dynamodb.GetItemInput{
		TableName: &m.config.ReservationTableName,
		Key: map[string]types.AttributeValue{
			GroupIDKey: &types.AttributeValueMemberS{Value: m.config.GroupID},
			ShardIDKey: &types.AttributeValueMemberS{Value: m.config.ShardID},
		},
	}
	out, err := client.GetItem(ctx, input)
	if err != nil {
		m.logger.Error("error getting item", "error", err)
		return nil, err
	}
	m.logger.Info("retrieved reservation", "item", out.Item)
	if out.Item == nil {
		m.logger.Error("no reservation retrieved")
		return nil, ErrNotFound
	}
	var reservation Reservation
	if err := attributevalue.UnmarshalMap(out.Item, &reservation); err != nil {
		return nil, err
	}
	return &reservation, err
}
