package processor

import (
	"context"
	"fmt"
	"github.com/caser789/dtask/manager"
	"github.com/caser789/dtask/model"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type leaderProcessor struct {
	nodeManager  manager.INodeManager
	taskManager  manager.ITaskManager
	shardManager manager.IShardManager

	addr   string
	closer chan interface{}

	running bool
	mutex   sync.Mutex
}

func (p *leaderProcessor) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.run(ctx)
		case <-p.closer:
			return
		}
	}
}

func (p *leaderProcessor) Stop(ctx context.Context) {
	close(p.closer)
}

func (p *leaderProcessor) stopSubTask(ctx context.Context) {
}

func (p *leaderProcessor) startSubTask(ctx context.Context) {
	nodes, err := p.nodeManager.GetNodes(ctx)
	if err != nil {
		logrus.Error("start sub task get nodes error")
		return
	}

	tasks, err := p.taskManager.GetAllTasks(ctx)
	if err != nil {
		logrus.Error("start sub task get all tasks error")
		return
	}

	err = p.shardTasks(ctx, nodes, tasks)
	if err != nil {
		logrus.Error("start sub task shard tasks error")
		return
	}

	go p.watchTaskChange(ctx)
	go p.watchNodeChange(ctx)
	// TODO check task config change 主动
	// TODO check node change 主动
}

func (p *leaderProcessor) shardTasks(ctx context.Context, nodes []string, taskConfigs []*model.TaskConfig) error {
	for _, task := range taskConfigs {
		err := p.shardTask(ctx, task, nodes)
		if err != nil {
			logrus.Error("start sub task shard task error")
			return err
		}
	}
	return nil
}

func (p *leaderProcessor) shardTask(ctx context.Context, config *model.TaskConfig, nodes []string) error {
	// stop running task
	// 1. 删掉 shards
	// 2. 等 running shards 消失
	err := p.shardManager.DeleteTask(ctx, config.Name)
	if err != nil {
		return err
	}
	for {
		// handleleader subtask stopped
		shards, err := p.shardManager.GetRunningShards(ctx, config.Name)
		if err != nil {
			continue
		}

		if len(shards) == 0 {
			break
		}

		time.Sleep(time.Second * 1)
	}

	// shard
	shardsByNode := shard(nodes, int(config.ShardNumber))

	// save
	return p.shardManager.SaveTask(ctx, config.Name, shardsByNode)
}

func shard(nodes []string, shardNum int) map[string][]string {
	// key = node, value = ["1", "2"]
	var res = make(map[string][]string)
	shardNo := 0
	for i := 0; i < len(nodes); {
		if shardNo == shardNum {
			break
		}
		res[nodes[i]] = append(res[nodes[i]], fmt.Sprintf("%s", shardNo))
		shardNo++
		i++
		if i == len(nodes) {
			i = 0
		}
	}
	return res
}

func (p *leaderProcessor) watchNodeChange(ctx context.Context) {
	rch, err := p.nodeManager.Watch(ctx)
	if err != nil {
		return
	}

	for {
		select {
		case nodes := <-rch:
			tasks, err := p.taskManager.GetAllTasks(ctx)
			if err != nil {
				continue
			}

			err = p.shardTasks(ctx, nodes, tasks)
			if err != nil {
				logrus.Error("watch node change shard tasks error")
			}
		}
	}
}

func (p *leaderProcessor) watchTaskChange(ctx context.Context) {
	rch, err := p.taskManager.Watch(ctx)
	if err != nil {
		return
	}

	for {
		select {
		case taskConfigs := <-rch:
			nodes, err := p.nodeManager.GetNodes(ctx)
			if err != nil {
				continue
			}

			err = p.shardTasks(ctx, nodes, taskConfigs)
			if err != nil {
				logrus.Error("watch node change shard tasks error")
			}
		}
	}

}

func (p *leaderProcessor) run(ctx context.Context) {
	isLeader, err := p.nodeManager.IsLeader(ctx)
	if err != nil {
		return
	}

	if p.running && isLeader {
		return
	}

	if !p.running && !isLeader {
		return
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.running && !isLeader {
		p.stopSubTask(ctx)
		p.running = false
		return
	}

	p.startSubTask(ctx)
	p.running = true
}
