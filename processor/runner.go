package processor

import (
	"context"
	"github.com/caser789/dtask/manager"
)

func NewRunner(addr string) {

}

type runner struct {
	nodeManager  manager.INodeManager
	shardManager manager.IShardManager
	// leader processor
	// worker processor
	// addr
}

func (r *runner) init(ctx context.Context) error {
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

	// save task configs
	// leader processor.start
	// worker processor.start
}
