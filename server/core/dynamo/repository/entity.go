// Copyright 2024 Hauki Tech Sp. z o.o.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Modified hereafter by contributors to runatlantis/atlantis.

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
