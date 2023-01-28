package model

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestShardDao(t *testing.T) {
	ctx := context.Background()
	cli := GetClient(ctx)

	s1 := Shards{"1", "2"}
	shardDAO := NewShardDAO(cli)
	err := shardDAO.Put(ctx, "task-1", "1.1.1.1:8080", s1)
	assert.NoError(t, err)

	s2, err := shardDAO.Get(ctx, "task-1", "1.1.1.1:8080")
	assert.NoError(t, err)
	assert.Equal(t, s1, s2)

	err = shardDAO.Delete(ctx, "task-1", "1.1.1.1:8080")
	assert.NoError(t, err)
	s3, err := shardDAO.Get(ctx, "task-1", "1.1.1.1:8080")
	assert.NoError(t, err)
	assert.Equal(t, s3, Shards(nil))

	// test delete all
	s4 := Shards{"1", "2"}
	err = shardDAO.Put(ctx, "task-1", "1.1.1.1:8080", s4)
	assert.NoError(t, err)
	err = shardDAO.DeleteAll(ctx)
	assert.NoError(t, err)
	s5, err := shardDAO.Get(ctx, "task-1", "1.1.1.1:8080")
	assert.NoError(t, err)
	assert.Equal(t, s5, Shards(nil))

	// test delete task
	err = shardDAO.Put(ctx, "task-1", "1.1.1.1:8080", s4)
	assert.NoError(t, err)
	err = shardDAO.DeleteTask(ctx, "task-1")
	assert.NoError(t, err)
	s6, err := shardDAO.Get(ctx, "task-1", "1.1.1.1:8080")
	assert.NoError(t, err)
	assert.Equal(t, s6, Shards(nil))

	// test get by task
	err = shardDAO.Put(ctx, "task-1", "1.1.1.1:8080", Shards{"1", "2"})
	assert.NoError(t, err)
	err = shardDAO.Put(ctx, "task-1", "2.2.2.2:8080", Shards{"3", "4"})
	assert.NoError(t, err)
	nodeWithShardsList, err := shardDAO.GetByTask(ctx, "task-1")
	assert.NoError(t, err)
	assert.Equal(t, nodeWithShardsList, []*NodeWithShards{
		{Node: "1.1.1.1:8080", Shards: Shards{"1", "2"}},
		{Node: "2.2.2.2:8080", Shards: Shards{"3", "4"}},
	})
	err = shardDAO.DeleteAll(ctx)
	assert.NoError(t, err)

	// test get all
	err = shardDAO.Put(ctx, "task-1", "1.1.1.1:8080", Shards{"1", "2"})
	assert.NoError(t, err)
	err = shardDAO.Put(ctx, "task-1", "2.2.2.2:8080", Shards{"3", "4"})
	assert.NoError(t, err)
	err = shardDAO.Put(ctx, "task-2", "1.1.1.1:8080", Shards{"1", "2"})
	assert.NoError(t, err)
	err = shardDAO.Put(ctx, "task-2", "2.2.2.2:8080", Shards{"3", "4"})
	assert.NoError(t, err)
	resp, err := shardDAO.GetAll(ctx)
	assert.NoError(t, err)
	assert.Equal(t, resp, []*TaskWithNodesWithShards{
		{TaskName: "task-1", NodeWithShards: []*NodeWithShards{
			{Node: "1.1.1.1:8080", Shards: Shards{"1", "2"}},
			{Node: "2.2.2.2:8080", Shards: Shards{"3", "4"}},
		}},
		{TaskName: "task-2", NodeWithShards: []*NodeWithShards{
			{Node: "1.1.1.1:8080", Shards: Shards{"1", "2"}},
			{Node: "2.2.2.2:8080", Shards: Shards{"3", "4"}},
		}},
	})

	// test watch
	rch, err := shardDAO.WatchShards(ctx, "task-3", "1.1.1.1:8080")
	assert.NoError(t, err)

	rch2, err := shardDAO.WatchForNewTask(ctx)
	assert.NoError(t, err)

	go func() {
		for {
			select {
			case c := <-rch:
				fmt.Println("-----")
				fmt.Println(c.Encode())
				fmt.Println("-----")
			case c := <-rch2:
				fmt.Println("+++++")
				fmt.Println(c)
				fmt.Println("+++++")
			default:
			}
		}
	}()

	i := 1
	for i < 10 {
		err = shardDAO.Put(ctx, "task-3", "1.1.1.1:8080", Shards{fmt.Sprintf("%d", i)})
		assert.NoError(t, err)
		err = shardDAO.Put(ctx, fmt.Sprintf("task-%d", i), "1.1.1.1:8080", Shards{fmt.Sprintf("%d", i)})
		assert.NoError(t, err)
		time.Sleep(time.Second)
		i++
	}
	time.Sleep(time.Second)

}
