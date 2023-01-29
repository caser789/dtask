package main

import (
	"context"
	"github.com/caser789/dtask/manager"
	"github.com/caser789/dtask/model"
	"github.com/caser789/dtask/processor"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

func main() {
	ctx := context.Background()
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 3 * time.Second,
		Context:     ctx,
	})

	runner1 := processor.NewRunner("1.1.1.1:8080", cli)
	go runner1.Run(ctx)

	runner2 := processor.NewRunner("2.2.2.2:8080", cli)
	go runner2.Run(ctx)

	taskManager := manager.NewTaskManager(cli)
	taskManager.Put(ctx, &model.TaskConfig{Name: "task-1", TaskType: model.InternalTaskType_Task, ShardNumber: 5})

	for {
		time.Sleep(time.Second * 10)
	}
}
