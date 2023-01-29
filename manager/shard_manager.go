package manager

import (
	"context"
	"github.com/caser789/dtask/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type IShardManager interface {
	Reset(ctx context.Context) error
	DeleteTask(ctx context.Context, taskName string) error
	GetRunningShards(ctx context.Context, taskName string) ([]string, error)
	SaveTask(ctx context.Context, taskName string, shardsByNode map[string][]string) error
	GetShardsOnNode(ctx context.Context, taskName, node string) ([]string, error)
	WatchShardChange(ctx context.Context, taskName, node string) (<-chan model.Shards, error)
	WatchForNewTask(ctx context.Context) (<-chan model.TaskNodeShardStore, error)
	DeleteRunningShard(ctx context.Context, taskName, node, shard string) error
	DeleteRunningNode(ctx context.Context, taskName, node string) error
	SetShardRunning(ctx context.Context, taskName, node, shard string) error
}

func NewShardManager(addr string, client *clientv3.Client) *ShardManager {
	return &ShardManager{
		addr:           addr,
		shardDAO:       model.NewShardDAO(client),
		runningTaskDAO: model.NewRunningTaskDAO(client),
	}
}

type ShardManager struct {
	addr string

	shardDAO       model.IShardDAO
	runningTaskDAO model.IRunningTaskDAO
}

func (s *ShardManager) Reset(ctx context.Context) error {
	err := s.shardDAO.DeleteAll(ctx)
	if err != nil {
		return err
	}

	return s.runningTaskDAO.DeleteAll(ctx)
}

func (s *ShardManager) DeleteTask(ctx context.Context, taskName string) error {
	return s.shardDAO.DeleteTask(ctx, taskName)
}

func (s *ShardManager) GetRunningShards(ctx context.Context, taskName string) ([]string, error) {
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

func (s *ShardManager) SaveTask(ctx context.Context, taskName string, shardsByNode map[string][]string) error {
	for node, shards := range shardsByNode {
		err := s.shardDAO.Put(ctx, taskName, node, shards)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *ShardManager) GetShardsOnNode(ctx context.Context, taskName, node string) ([]string, error) {
	return s.shardDAO.Get(ctx, taskName, node)
}

func (s *ShardManager) WatchShardChange(ctx context.Context, taskName, node string) (<-chan model.Shards, error) {
	return s.shardDAO.WatchShards(ctx, taskName, node)
}

func (s *ShardManager) WatchForNewTask(ctx context.Context) (<-chan model.TaskNodeShardStore, error) {
	return s.shardDAO.WatchForNewTask(ctx)
}

func (s *ShardManager) DeleteRunningShard(ctx context.Context, taskName, node, shard string) error {
	return s.runningTaskDAO.Delete(ctx, taskName, node, shard)
}

func (s *ShardManager) DeleteRunningNode(ctx context.Context, taskName, node string) error {
	return s.runningTaskDAO.DeleteTaskNode(ctx, taskName, node)
}

func (s *ShardManager) SetShardRunning(ctx context.Context, taskName, node, shard string) error {
	return s.runningTaskDAO.Put(ctx, taskName, node, shard)
}
