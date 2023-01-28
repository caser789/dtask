package model

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLeaderDataDAO(t *testing.T) {
	ctx := context.Background()
	cli := GetClient(ctx)

	leaderDataDAO := NewLeaderDataDAO(cli)

	err := leaderDataDAO.Put(ctx, Nodes{"1.1.1.1:8080", "2.2.2.2:8080"})
	assert.NoError(t, err)
	nodes, err := leaderDataDAO.Get(ctx)
	assert.NoError(t, err)
	assert.Equal(t, nodes, Nodes{"1.1.1.1:8080", "2.2.2.2:8080"})

	err = leaderDataDAO.Put(ctx, Nodes{"1.1.1.1:8080", "2.2.2.2:8080", "3.3.3.3:8080"})
	assert.NoError(t, err)
	nodes, err = leaderDataDAO.Get(ctx)
	assert.NoError(t, err)
	assert.Equal(t, nodes, Nodes{"1.1.1.1:8080", "2.2.2.2:8080", "3.3.3.3:8080"})
}

func TestLeaderDAO(t *testing.T) {
	ctx := context.Background()
	cli := GetClient(ctx)

	leaderDAO := NewLeaderDAO(cli)
	leaderDAO.Campaign(ctx, "1.1.1.1:8080")
	leaderDAO.Campaign(ctx, "2.2.2.2:8080")

	i := 1
	for i < 20 {
		node, err := leaderDAO.Get(ctx)
		assert.NoError(t, err)
		fmt.Printf("leader is %s\n", node)
		time.Sleep(time.Second * 5)
		i++
	}
	time.Sleep(time.Second)
}

func TestNodeDAO(t *testing.T) {
	ctx := context.Background()
	cli := GetClient(ctx)

	nodeDAO := NewNodeDAO(cli)
	_, err := nodeDAO.Register(ctx, "1.1.1.1:8080")
	assert.NoError(t, err)
	_, err = nodeDAO.Register(ctx, "2.2.2.2:8080")
	assert.NoError(t, err)

	i := 1
	for i < 20 {
		nodes, err := nodeDAO.List(ctx)
		assert.NoError(t, err)
		fmt.Printf("nodes are %v\n", nodes)
		time.Sleep(time.Second * 5)
		i++
	}
	time.Sleep(time.Second)
}