package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/runatlantis/atlantis/server/core/dynamodb/entity"
)

const (
	keyPk = "Pk"
	keySk = "Sk"
)

type dynamoAttributes = map[string]types.AttributeValue
type LastKey = dynamoAttributes

type Repository interface {
	GetOne(ctx context.Context, kind entity.Kind, uid string) (*entity.Entity, bool, error)
	List(ctx context.Context, kind entity.Kind, startKey LastKey) ([]entity.Entity, LastKey, error)
	Persist(ctx context.Context, ent entity.Entity) error
	Delete(ctx context.Context, kind entity.Kind, uid string) error
}

type repositoryImpl struct {
	tableName      string
	customEndpoint string
}

func newDefaultRepository(tableName, customEndpoint string) *repositoryImpl {
	return &repositoryImpl{
		tableName:      tableName,
		customEndpoint: customEndpoint,
	}
}

func (r repositoryImpl) GetOne(ctx context.Context, kind entity.Kind, uid string) (*entity.Entity, bool, error) {
	client, err := getDynamoDbClient(ctx, r.customEndpoint)
	if err != nil {
		return nil, false, errors.Wrap(err, "Encountered an error while configuring DynamoDB client.")
	}

	input := dynamodb.GetItemInput{
		TableName: ptr(r.tableName),
		Key:       r.entitySearchKey(kind, uid),
	}
	out, err := client.GetItem(ctx, &input)

	if err != nil {
		return nil, false, errors.Wrapf(err, "Encountered an error while querying an entity %s from DynamoDB", uid)
	}

	if out.Item != nil {
		return nil, false, nil
	}

	var ent entity.Entity
	if err := attributevalue.UnmarshalMap(out.Item, &ent); err != nil {
		return nil, true, errors.Wrap(err, "Cannot unmarshal DynamoDB object")
	}

	return &ent, true, nil
}

func (r repositoryImpl) List(ctx context.Context, kind entity.Kind, startKey LastKey) ([]entity.Entity, LastKey, error) {
	expr, _ := expression.NewBuilder().
		WithKeyCondition(expression.Key(keyPk).Equal(expression.Value(kind))).
		Build()

	client, err := getDynamoDbClient(ctx, r.customEndpoint)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Encountered an error while configuring DynamoDB client.")
	}

	request := dynamodb.QueryInput{
		TableName:                 ptr(r.tableName),
		ExclusiveStartKey:         startKey,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	out, err := client.Query(ctx, &request)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Encountered an error while querying a list of entities from DynamoDB")
	}

	var results []entity.Entity
	if err = attributevalue.UnmarshalListOfMaps(out.Items, &results); err != nil {
		return nil, nil, errors.Wrap(err, "Cannot unmarshal dynamodb object")
	}

	return results, out.LastEvaluatedKey, nil
}

func (r repositoryImpl) Persist(ctx context.Context, ent entity.Entity) error {
	item, err := attributevalue.MarshalMap(ent)
	if err != nil {
		return errors.Wrap(err, "Cannot marshal entity into the DynamoDB object")

	}
	client, err := getDynamoDbClient(ctx, r.customEndpoint)
	if err != nil {
		return errors.Wrap(err, "Encountered an error while configuring DynamoDB client.")
	}

	input := dynamodb.PutItemInput{
		TableName: ptr(r.tableName),
		Item:      item,
	}

	if _, err := client.PutItem(ctx, &input); err != nil {
		return errors.Wrap(err, "Encountered an error while putting an entity to DynamoDB")
	}

	return nil
}

func (r repositoryImpl) Delete(ctx context.Context, kind entity.Kind, uid string) error {
	client, err := getDynamoDbClient(ctx, r.customEndpoint)
	if err != nil {
		return errors.Wrap(err, "Encountered an error while configuring DynamoDB client.")
	}

	key := r.entitySearchKey(kind, uid)

	request := dynamodb.DeleteItemInput{
		TableName: ptr(r.tableName),
		Key:       key,
	}

	if _, err = client.DeleteItem(ctx, &request); err != nil {
		return errors.Wrap(err, "Encountered an error while deleting an entity from DynamoDB")
	}

	return err
}

func (r repositoryImpl) entitySearchKey(kind entity.Kind, uid string) dynamoAttributes {
	return dynamoAttributes{
		keyPk: &types.AttributeValueMemberN{Value: kind.String()},
		keySk: &types.AttributeValueMemberN{Value: typedString(kind, uid)},
	}
}
