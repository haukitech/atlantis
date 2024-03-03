package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/runatlantis/atlantis/server/core/dynamodb/entity"
)

const (
	keyPk = "Pk"
	keySk = "Sk"
)

type Repository interface {
	GetOne(kind entity.Kind, uid string) (*entity.Entity, error)
	List(kind entity.Kind, startKey string) ([]entity.Entity, error)
	Persist(ent entity.Entity) error
	Delete(ent entity.Entity) error
}

type repositoryImpl struct {
	client    dynamodb.Client
	tableName string
}

func (r repositoryImpl) GetOne(kind entity.Kind, uid string) (*entity.Entity, bool, error) {
	typedUid := TypedString(kind, uid)

	expr, _ := expression.NewBuilder().
		WithKeyCondition(expression.Key(keyPk).Equal(expression.Value(kind))).
		WithKeyCondition(expression.Key(keySk).Equal(expression.Value(typedUid))).
		Build()

	out, err := r.client.GetItem(
		context.TODO(),
		&dynamodb.GetItemInput{
			TableName: aws.String(r.tableName),
			Key:       expr.Values(),
		},
	)

	if err != nil {
		return nil, false, errors.Wrapf(err, "Encountered an error while fetching an entity with UID %s from DynamoDB", uid)
	}

	if out.Item != nil {
		return nil, false, nil
	}

	var ent entity.Entity
	if err := attributevalue.UnmarshalMap(out.Item, &ent); err != nil {
		return nil, true, errors.Wrap(err, "Cannot unmarshal dynamodb object")
	}

	return &ent, true, nil

}

func (r repositoryImpl) List(kind entity.Kind, startKey string) ([]entity.Entity, error) {
	return nil, nil
}

func (r repositoryImpl) Persist(ent entity.Entity) error {
	//TODO implement me
	panic("implement me")
}

func (r repositoryImpl) Delete(ent entity.Entity) error {
	//TODO implement me
	panic("implement me")
}
