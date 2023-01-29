package manager

import (
	"context"
	"github.com/caser789/dtask/model"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type INodeManager interface {
	KeepRegister(ctx context.Context) error
	HasLeader(ctx context.Context) (bool, error)
	IsLeader(ctx context.Context) (bool, error)
	GetNodes(ctx context.Context) ([]string, error)
	SaveLeaderNodes(ctx context.Context, nodes []string) error
	Watch(ctx context.Context) (<-chan []string, error)
}

func NewNodeManager(addr string, client *clientv3.Client) *NodeManager {
	return &NodeManager{
		addr:      addr,
		leaderDAO: model.NewLeaderDAO(client),
		nodeDao:   model.NewNodeDAO(client),
	}
}

type NodeManager struct {
	addr string

	leaderDAO     model.ILeaderDAO
	nodeDao       model.INodeDAO
	leaderDataDAO model.ILeaderDataDAO
}

func (m *NodeManager) KeepRegister(ctx context.Context) error {
	_, err := m.nodeDao.Register(ctx, m.addr)
	if err != nil {
		return err
	}

	m.leaderDAO.Campaign(ctx, m.addr)
	return nil
}

func (m *NodeManager) HasLeader(ctx context.Context) (bool, error) {
	leader, err := m.leaderDAO.Get(ctx)
	if err != nil {
		return false, err
	}

	return leader != "", nil
}

func (m *NodeManager) IsLeader(ctx context.Context) (bool, error) {
	leader, err := m.leaderDAO.Get(ctx)
	if err != nil {
		return false, err
	}

	return leader == m.addr, nil
}

func (m *NodeManager) GetNodes(ctx context.Context) ([]string, error) {
	return m.nodeDao.List(ctx)
}

func (m *NodeManager) SaveLeaderNodes(ctx context.Context, nodes []string) error {
	return m.leaderDataDAO.Put(ctx, nodes)
}

func (m *NodeManager) Watch(ctx context.Context) (<-chan []string, error) {
	return m.nodeDao.Watch(ctx)
}
