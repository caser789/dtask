package model

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func GetClient(ctx context.Context) *clientv3.Client {
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 3 * time.Second,
		Context:     ctx,
	})

	return cli
}

func TestNewSystemConfigDAO(t *testing.T) {
	ctx := context.Background()
	cli := GetClient(ctx)

	input := &SystemConfig{LeaderChangeCheckInterval: 1, NodeChangeCheckInterval: 2}
	systemConfigDAO := NewSystemConfigDAO(cli)
	err := systemConfigDAO.Put(ctx, input)
	assert.NoError(t, err)

	output, err := systemConfigDAO.Get(ctx)
	assert.NoError(t, err)

	assert.Equal(t, input, output)
}

func _TestNewTaskConfigDAO(t *testing.T) {
	ctx := context.Background()
	cli := GetClient(ctx)

	taskName := "task-1"
	input := &TaskConfig{Name: taskName}
	taskConfigDAO := NewTaskConfigDAO(cli)
	err := taskConfigDAO.Put(ctx, input)
	assert.NoError(t, err)

	taskName2 := "task-2"
	input2 := &TaskConfig{Name: taskName2}
	err = taskConfigDAO.Put(ctx, input2)
	assert.NoError(t, err)

	output, err := taskConfigDAO.Get(ctx, taskName)
	assert.NoError(t, err)

	assert.Equal(t, input, output)

	configs, err := taskConfigDAO.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []*TaskConfig{input, input2}, configs)

	rch, err := taskConfigDAO.WatchTaskConfig(ctx, input)
	assert.NoError(t, err)

	rch2, err := taskConfigDAO.Watch(ctx)
	assert.NoError(t, err)

	go func() {
		for {
			select {
			case c := <-rch:
				fmt.Println("-----")
				fmt.Println(c.Encode())
				fmt.Println("-----")
			case c2 := <-rch2:
				fmt.Println("*****")
				fmt.Println(c2)
				fmt.Println("*****")
			default:
			}
		}
	}()

	i := 1
	for i < 10 {
		err = taskConfigDAO.Put(ctx, &TaskConfig{Name: taskName, ShardNumber: uint32(i)})
		assert.NoError(t, err)
		time.Sleep(time.Second)
		i++
	}
	time.Sleep(time.Second)
}
