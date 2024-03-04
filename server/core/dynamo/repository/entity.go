package repository

import (
	"encoding/json"
	"fmt"
)

const (
	EProjectLock Kind = iota
	ECommandLock
)

type Kind int8

func (k Kind) String() string {
	return fmt.Sprintf("%d", k)
}

type Entity struct {
	Pk     Kind
	Sk     string
	Object string
}

func UnmarshalObject[T any](entity Entity, object *T) {
	_ = json.Unmarshal([]byte(entity.Object), object)
}

func NewEntityFromObject(kind Kind, uid string, object any) Entity {
	marshaled, _ := json.Marshal(object)

	return Entity{
		Pk:     kind,
		Sk:     typedString(kind, uid),
		Object: string(marshaled),
	}
}
