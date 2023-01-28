package manager

import (
	"context"
	"github.com/caser789/dtask/model"
)

type IShardManager interface {
	Reset(ctx context.Context) error
	DeleteTask(ctx context.Context, taskName string) error
	GetRunningShards(ctx context.Context, taskName string) ([]string, error)
	SaveTask(ctx context.Context, taskName string, shardsByNode map[string][]string) error
}

type shardManager struct {
	addr string

	shardDAO       model.IShardDAO
	runningTaskDAO model.IRunningTaskDAO
}

func (s *shardManager) Reset(ctx context.Context) error {
	err := s.shardDAO.DeleteAll(ctx)
	if err != nil {
		return err
	}

	return s.runningTaskDAO.DeleteAll(ctx)
}

func (s *shardManager) DeleteTask(ctx context.Context, taskName string) error {
	return s.shardDAO.DeleteTask(ctx, taskName)
}

func (s *shardManager) GetRunningShards(ctx context.Context, taskName string) ([]string, error) {
	nodeWithShardsList, err := s.runningTaskDAO.ListByTask(ctx, taskName)
	if err != nil {
		return nil, err
	}
	var shards []string
	for _, n := range nodeWithShardsList {
		for _, x := range n.Shards {
			shards = append(shards, x)
		}
	}
	return shards, nil
}

func (s *shardManager) SaveTask(ctx context.Context, taskName string, shardsByNode map[string][]string) error {
	for node, shards := range shardsByNode {
		err := s.shardDAO.Put(ctx, taskName, node, shards)
		if err != nil {
			return err
		}
	}

	return nil
}
