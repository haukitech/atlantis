package dynamodb

import (
	"fmt"
	"github.com/runatlantis/atlantis/server/core/dynamodb/entity"
)

func TypedString(kind entity.Kind, value string) string {
	return fmt.Sprintf("%d_%s", kind, value)
}
