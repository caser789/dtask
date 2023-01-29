package processor

import (
	"context"
	"fmt"
	"github.com/caser789/dtask/invoker"
	"github.com/caser789/dtask/manager"
	"github.com/caser789/dtask/model"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
)

func NewWorkerProcessor(node string, client *clientv3.Client) *workerProcessor {
	return &workerProcessor{
		node:         node,
		taskManager:  manager.NewTaskManager(client),
		shardManager: manager.NewShardManager(node, client),
		invokers:     make(map[string]map[string]invoker.IInvoker),
		mutex:        sync.Mutex{},
	}
}

type workerProcessor struct {
	taskManager  manager.ITaskManager
	shardManager manager.IShardManager

	node     string
	invokers map[string]map[string]invoker.IInvoker //[taskName]:[shardNo]:invoker
	mutex    sync.Mutex
}

func (p *workerProcessor) Run(ctx context.Context) error {
	logrus.WithField("node", p.node).Info("start to run")
	tasks, err := p.taskManager.GetAllTasks(ctx)
	if err != nil {
		return err
	}

	for _, t := range tasks {
		shards, err := p.shardManager.GetShardsOnNode(ctx, t.Name, p.node)
		if err != nil {
			return err
		}

		err = p.loadTask(ctx, t, shards)
		if err != nil {
			return err
		}

		go p.watchShardChange(ctx, t.Name)
		go p.watchTaskChange(ctx, t.Name)
	}
	go p.watchNewTask(ctx)

	return nil
}

func (p *workerProcessor) loadTask(ctx context.Context, t *model.TaskConfig, shards []string) error {
	logrus.WithField("node", p.node).Info("load task")
	invokerByShard := p.getTaskInvokers(t.Name)

	if len(shards) == 0 {
		// stop invokers
		for _, i := range invokerByShard {
			p.waitInvokerStop(i.StopRunning())
			p.deleteInvoker(t.Name, fmt.Sprintf("%d", i.GetShard()))
		}
		return p.shardManager.DeleteRunningNode(ctx, t.Name, p.node)
	}

	shardStore := make(map[string]bool)
	for _, s := range shards {
		shardStore[s] = true
	}
	for _, i := range invokerByShard {
		s := fmt.Sprintf("%d", i.GetShard())
		if shardStore[s] {
			delete(shardStore, s)
			i.ReloadTaskConfig(t)
		} else {
			p.waitInvokerStop(i.StopRunning())
			p.deleteInvoker(t.Name, fmt.Sprintf("%d", i.GetShard()))
			p.shardManager.DeleteRunningShard(ctx, t.Name, p.node, s)
		}
	}
	for s, _ := range shardStore {
		var i invoker.IInvoker

		switch t.TaskType {
		case model.InternalTaskType_Task:
			i = invoker.NewRtTaskInvoker(ctx, t, p.node, s, nil)
			logrus.WithField("node", p.node).WithField("shard", s).Info("new invoker")
		}
		if i != nil {
			err := p.shardManager.SetShardRunning(ctx, t.Name, p.node, s)
			if err != nil {
				continue
			}

			p.addInvoker(t.Name, s, i)
			i.Start()
		}
	}

	return nil
}

func (p *workerProcessor) waitInvokerStop(stopCompleted <-chan bool) {
	for {
		select {
		case <-stopCompleted:
			// delete ru
			return
		}
	}
}

func (p *workerProcessor) deleteInvoker(tname string, shard string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.invokers[tname] == nil {
		p.invokers[tname] = make(map[string]invoker.IInvoker)
	}

	delete(p.invokers[tname], shard)
}

func (p *workerProcessor) addInvoker(tname string, shard string, i invoker.IInvoker) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.invokers[tname] == nil {
		p.invokers[tname] = make(map[string]invoker.IInvoker)
	}

	p.invokers[tname][shard] = i
}

func (p *workerProcessor) getTaskInvokers(task string) map[string]invoker.IInvoker {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var res = make(map[string]invoker.IInvoker)
	taskInvokers := p.invokers[task]
	if taskInvokers == nil {
		return res
	}

	for k, v := range taskInvokers {
		res[k] = v
	}
	return res
}

func (p *workerProcessor) watchShardChange(ctx context.Context, tname string) error {
	logrus.WithField("node", p.node).Info("watch for shard change")
	rch, err := p.shardManager.WatchShardChange(ctx, tname, p.node)
	if err != nil {
		return err
	}

	// if shard changed for this task
	for {
		select {
		case <-rch:
			tasks, err := p.taskManager.GetAllTasks(ctx)
			logrus.WithField("node", p.node).WithField("tasks", tasks).Info("watch for shard change")
			if err != nil {
				continue
			}
			for _, t := range tasks {
				shards, err := p.shardManager.GetShardsOnNode(ctx, t.Name, p.node)
				if err != nil {
					continue
				}

				err = p.loadTask(ctx, t, shards)
				if err != nil {
					continue
				}
			}
		}
	}
}

func (p *workerProcessor) watchNewTask(ctx context.Context) error {
	logrus.WithField("node", p.node).Info("watch for new task")
	rch, err := p.taskManager.Watch(ctx)
	if err != nil {
		return err
	}
	// for each new task

	for {
		select {
		case _ = <-rch:
			tasks, err := p.taskManager.GetAllTasks(ctx)
			logrus.WithField("node", p.node).WithField("tasks", tasks).Info("watch for new task")
			if err != nil {
				continue
			}
			for _, t := range tasks {
				shards, err := p.shardManager.GetShardsOnNode(ctx, t.Name, p.node)
				if err != nil {
					continue
				}

				err = p.loadTask(ctx, t, shards)
				if err != nil {
					continue
				}

				go p.watchTaskChange(ctx, t.Name)
			}
		}
	}
}

func (p *workerProcessor) watchTaskChange(ctx context.Context, task string) error {
	logrus.WithField("node", p.node).Info("watch task change")
	rch, err := p.taskManager.WatchTaskConfig(ctx, task)
	if err != nil {
		return err
	}

	// for each changed task
	for {
		select {
		case <-rch:
			tasks, err := p.taskManager.GetAllTasks(ctx)
			logrus.WithField("node", p.node).WithField("tasks", tasks).Info("watch for task change")
			if err != nil {
				continue
			}
			for _, t := range tasks {
				shards, err := p.shardManager.GetShardsOnNode(ctx, t.Name, p.node)
				if err != nil {
					continue
				}

				err = p.loadTask(ctx, t, shards)
				if err != nil {
					continue
				}
			}
		}
	}
}
