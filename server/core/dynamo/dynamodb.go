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

package dynamo

import (
	"context"
	"github.com/pkg/errors"
	"github.com/runatlantis/atlantis/server/core/dynamo/repository"
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
	ctx := context.Background()

	lock := d.newCommandLock(cmdName, lockTime)
	ent := repository.NewEntityFromObject(
		repository.ECommandLock,
		cmdName.String(),
		lock,
	)

	if err := d.repository.Persist(ctx, ent); err != nil {
		return nil, errors.Wrapf(err, "Could not lock command %s", cmdName)
	}

	return &lock, nil
}

func (d DynamoDb) UnlockCommand(cmdName command.Name) error {
	ctx := context.Background()

	err := d.repository.Delete(ctx, repository.ECommandLock, cmdName.String())
	if err != nil {
		return errors.Wrapf(err, "Could not unlock command %s", cmdName)
	}

	return nil
}

func (d DynamoDb) CheckCommandLock(cmdName command.Name) (*command.Lock, error) {
	ctx := context.Background()

	ent, found, err := d.repository.GetOne(ctx, repository.ECommandLock, cmdName.String())
	if err != nil {
		return nil, errors.Wrapf(err, "Could not check lock status for command %s", cmdName)
	}

	if !found {
		return nil, nil
	}

	var lock command.Lock
	repository.UnmarshalObject(*ent, &lock)

	return &lock, nil
}

func (d DynamoDb) newCommandLock(cmdName command.Name, lockTime time.Time) command.Lock {
	return command.Lock{
		CommandName: cmdName,
		LockMetadata: command.LockMetadata{
			UnixTime: lockTime.Unix(),
		},
	}
}

func (d DynamoDb) listAllLocks() ([]models.ProjectLock, error) {
	ctx := context.Background()

	allLocks := make([]models.ProjectLock, 0)
	var lastKey repository.LastKey

	for {
		results, lastKey, err := d.repository.List(ctx, repository.EProjectLock, lastKey)
		if err != nil {
			return nil, errors.Wrap(err, "Could not load all project locks from DynamoDb.")
		}

		for _, res := range results {
			var lock models.ProjectLock
			repository.UnmarshalObject(res, &lock)

			allLocks = append(allLocks, lock)
		}

		if lastKey == nil {
			break
		}
	}

	return allLocks, nil
}
