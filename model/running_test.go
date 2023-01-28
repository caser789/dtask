package model

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func _TestRunningTaskDAO(t *testing.T) {
	ctx := context.Background()
	cli := GetClient(ctx)

	runningTaskDAO := NewRunningTaskDAO(cli)

	err := runningTaskDAO.Put(ctx, "task-1", "1.1.1.1:8080", "1")
	assert.NoError(t, err)

	err = runningTaskDAO.Put(ctx, "task-1", "1.1.1.1:8080", "2")
	assert.NoError(t, err)
	err = runningTaskDAO.Put(ctx, "task-1", "2.2.2.2:8080", "3")
	assert.NoError(t, err)
	err = runningTaskDAO.Put(ctx, "task-1", "2.2.2.2:8080", "4")
	assert.NoError(t, err)
	err = runningTaskDAO.Put(ctx, "task-2", "1.1.1.1:8080", "4")
	assert.NoError(t, err)
	err = runningTaskDAO.Put(ctx, "task-2", "1.1.1.1:8080", "3")
	assert.NoError(t, err)
	err = runningTaskDAO.Put(ctx, "task-2", "2.2.2.2:8080", "2")
	assert.NoError(t, err)
	err = runningTaskDAO.Put(ctx, "task-2", "2.2.2.2:8080", "1")
	assert.NoError(t, err)

	shard, err := runningTaskDAO.Get(ctx, "task-1", "1.1.1.1:8080", "1")
	assert.NoError(t, err)
	assert.Equal(t, shard, "1")

	shards, err := runningTaskDAO.ListByTaskNode(ctx, "task-1", "1.1.1.1:8080")
	assert.NoError(t, err)
	assert.Equal(t, shards, Shards([]string{"1", "2"}))

	nodeWithShards, err := runningTaskDAO.ListByTask(ctx, "task-1")
	assert.NoError(t, err)
	assert.Equal(
		t, nodeWithShards,
		[]*NodeWithShards{
			{Node: "1.1.1.1:8080", Shards: []string{"1", "2"}},
			{Node: "2.2.2.2:8080", Shards: []string{"3", "4"}},
		},
	)

	tasks, err := runningTaskDAO.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, tasks, []*TaskWithNodesWithShards{
		{TaskName: "task-1", NodeWithShards: []*NodeWithShards{
			{Node: "1.1.1.1:8080", Shards: []string{"1", "2"}},
			{Node: "2.2.2.2:8080", Shards: []string{"3", "4"}},
		}},
		{TaskName: "task-2", NodeWithShards: []*NodeWithShards{
			{Node: "1.1.1.1:8080", Shards: []string{"3", "4"}},
			{Node: "2.2.2.2:8080", Shards: []string{"1", "2"}},
		}},
	})

	err = runningTaskDAO.Delete(ctx, "task-2", "1.1.1.1:8080", "3")
	assert.NoError(t, err)

	tasks, err = runningTaskDAO.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, tasks, []*TaskWithNodesWithShards{
		{TaskName: "task-1", NodeWithShards: []*NodeWithShards{
			{Node: "1.1.1.1:8080", Shards: []string{"1", "2"}},
			{Node: "2.2.2.2:8080", Shards: []string{"3", "4"}},
		}},
		{TaskName: "task-2", NodeWithShards: []*NodeWithShards{
			{Node: "1.1.1.1:8080", Shards: []string{"4"}},
			{Node: "2.2.2.2:8080", Shards: []string{"1", "2"}},
		}},
	})

	err = runningTaskDAO.DeleteTaskNode(ctx, "task-1", "1.1.1.1:8080")
	assert.NoError(t, err)

	tasks, err = runningTaskDAO.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, tasks, []*TaskWithNodesWithShards{
		{TaskName: "task-1", NodeWithShards: []*NodeWithShards{
			{Node: "2.2.2.2:8080", Shards: []string{"3", "4"}},
		}},
		{TaskName: "task-2", NodeWithShards: []*NodeWithShards{
			{Node: "1.1.1.1:8080", Shards: []string{"4"}},
			{Node: "2.2.2.2:8080", Shards: []string{"1", "2"}},
		}},
	})

	err = runningTaskDAO.DeleteTask(ctx, "task-2")
	assert.NoError(t, err)
	tasks, err = runningTaskDAO.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, tasks, []*TaskWithNodesWithShards{
		{TaskName: "task-1", NodeWithShards: []*NodeWithShards{
			{Node: "2.2.2.2:8080", Shards: []string{"3", "4"}},
		}},
	})

	err = runningTaskDAO.DeleteAll(ctx)
	assert.NoError(t, err)
	tasks, err = runningTaskDAO.List(ctx)
	assert.NoError(t, err)
	var x []*TaskWithNodesWithShards
	assert.Equal(t, tasks, x)
}
