package entity

import (
	"encoding/json"
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

func ToObject[T any](entity Entity) T {
	var object T
	_ = json.Unmarshal([]byte(entity.Object), &object)

	return object
}

func NewFromObject[T any](kind Kind, uid string, object T) Entity {
	marshaled, _ := json.Marshal(object)

	return Entity{
		Pk:     kind,
		Sk:     uid,
		Object: string(marshaled),
	}
}
