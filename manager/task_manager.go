package manager

import (
	"context"
	"github.com/caser789/dtask/model"
)

type ITaskManager interface {
	GetAllTasks(ctx context.Context) ([]*model.TaskConfig, error)
	Watch(ctx context.Context) (<-chan []*model.TaskConfig, error)
}

type taskManager struct {
	taskConfigDAO model.ITaskConfigDAO
}

func (t *taskManager) GetAllTasks(ctx context.Context) ([]*model.TaskConfig, error) {
	return t.taskConfigDAO.List(ctx)
}

func (t *taskManager) Watch(ctx context.Context) (<-chan []*model.TaskConfig, error) {
	return t.taskConfigDAO.Watch(ctx)
}
