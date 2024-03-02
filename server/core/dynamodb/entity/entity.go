package entity

import (
	"encoding/json"
	"github.com/runatlantis/atlantis/server/events/models"
)

type Kind int8

const (
	ELock Kind = iota
)

type Entity struct {
	Pk     Kind
	Sk     string
	Object string
}

func ToModel[T any](entity Entity) T {
	var obj T
	_ = json.Unmarshal([]byte(entity.Object), &obj)

	return obj
}

func NewFromProjectLock(lock models.ProjectLock) Entity {
	object, _ := json.Marshal(lock)

	return Entity{
		Pk:     ELock,
		Object: string(object),
	}
}
