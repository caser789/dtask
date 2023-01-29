package processor

import (
	"context"
	"github.com/caser789/dtask/manager"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewRunner(addr string, client *clientv3.Client) *runner {
	return &runner{
		addr:         addr,
		nodeManager:  manager.NewNodeManager(addr, client),
		shardManager: manager.NewShardManager(addr, client),
		leader:       NewLeaderProcessor(addr, client),
		worker:       NewWorkerProcessor(addr, client),
		close:        make(chan interface{}),
	}
}

type runner struct {
	addr         string
	nodeManager  manager.INodeManager
	shardManager manager.IShardManager

	leader Runner
	worker Runner
	close  chan interface{}
}

func (r *runner) Run(ctx context.Context) error {
	err := r.nodeManager.KeepRegister(ctx)
	if err != nil {
		return err
	}

	hasLeader, err := r.nodeManager.HasLeader(ctx)
	if err != nil {
		return err
	}

	// handle no leader
	if !hasLeader {
		err = r.shardManager.Reset(ctx)
		if err != nil {
			return err
		}
	}

	go r.leader.Run(ctx)
	go r.worker.Run(ctx)

	for {
		select {
		case <-r.close:
			return nil
		}
	}
}
