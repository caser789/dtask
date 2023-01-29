package manager

import (
	"context"
	"github.com/caser789/dtask/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ITaskManager interface {
	GetAllTasks(ctx context.Context) ([]*model.TaskConfig, error)
	Watch(ctx context.Context) (<-chan []*model.TaskConfig, error)
	WatchTaskConfig(ctx context.Context, taskName string) (<-chan *model.TaskConfig, error)
	Put(ctx context.Context, value *model.TaskConfig) error
}

func NewTaskManager(client *clientv3.Client) *TaskManager {
	return &TaskManager{
		taskConfigDAO: model.NewTaskConfigDAO(client),
	}
}

type TaskManager struct {
	taskConfigDAO model.ITaskConfigDAO
}

func (t *TaskManager) GetAllTasks(ctx context.Context) ([]*model.TaskConfig, error) {
	return t.taskConfigDAO.List(ctx)
}

func (t *TaskManager) Watch(ctx context.Context) (<-chan []*model.TaskConfig, error) {
	return t.taskConfigDAO.Watch(ctx)
}

func (t *TaskManager) WatchTaskConfig(ctx context.Context, taskName string) (<-chan *model.TaskConfig, error) {
	return t.taskConfigDAO.WatchTaskConfig(ctx, taskName)
}

func (t *TaskManager) Put(ctx context.Context, value *model.TaskConfig) error {
	return t.taskConfigDAO.Put(ctx, value)
}
