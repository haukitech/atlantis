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

package dynamodb

import (
	"context"
	"github.com/pkg/errors"
	"github.com/runatlantis/atlantis/server/core/dynamodb/entity"
	"github.com/runatlantis/atlantis/server/core/dynamodb/repository"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"time"
)

type DynamoDb struct {
	repository repository.Repository
}

func New(tableName, customEndpoint string) *DynamoDb {
	return &DynamoDb{
		repository: repository.New(tableName, customEndpoint),
	}
}

func (d DynamoDb) listAllLocks() ([]models.ProjectLock, error) {
	ctx := context.Background()

	allLocks := make([]models.ProjectLock, 0)
	var lastKey repository.LastKey

	for {
		results, lastKey, err := d.repository.List(ctx, entity.EProjectLock, lastKey)
		if err != nil {
			return nil, errors.Wrap(err, "Could not load all project locks from DynamoDb.")
		}

		for _, res := range results {
			allLocks = append(allLocks, entity.ToObject[models.ProjectLock](res))
		}

		if lastKey == nil {
			break
		}
	}

	return allLocks, nil
}

func (d DynamoDb) TryLock(lock models.ProjectLock) (bool, models.ProjectLock, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) Unlock(project models.Project, workspace string) (*models.ProjectLock, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) List() ([]models.ProjectLock, error) {
	return d.listAllLocks()
}

func (d DynamoDb) GetLock(project models.Project, workspace string) (*models.ProjectLock, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) UnlockByPull(repoFullName string, pullNum int) ([]models.ProjectLock, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) UpdateProjectStatus(pull models.PullRequest, workspace string, repoRelDir string, newStatus models.ProjectPlanStatus) error {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) GetPullStatus(pull models.PullRequest) (*models.PullStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) DeletePullStatus(pull models.PullRequest) error {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) UpdatePullWithResults(pull models.PullRequest, newResults []command.ProjectResult) (models.PullStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) LockCommand(cmdName command.Name, lockTime time.Time) (*command.Lock, error) {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) UnlockCommand(cmdName command.Name) error {
	//TODO implement me
	panic("implement me")
}

func (d DynamoDb) CheckCommandLock(cmdName command.Name) (*command.Lock, error) {
	//TODO implement me
	return nil, nil
}
