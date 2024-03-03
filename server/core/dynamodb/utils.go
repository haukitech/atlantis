package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/runatlantis/atlantis/server/core/dynamodb/entity"
)

func typedString(kind entity.Kind, value string) string {
	return fmt.Sprintf("%d_%s", kind, value)
}

func getDynamoDbClient(ctx context.Context, awsEndpoint string) (*dynamodb.Client, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if awsEndpoint != "" {
			return aws.Endpoint{
				PartitionID: "aws",
				URL:         awsEndpoint,
			}, nil
		}

		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, errors.Wrap(err, "Could not load AWS configuration.")
	}

	return dynamodb.NewFromConfig(cfg), nil
}
