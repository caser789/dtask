package model

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
)

func NewRunningTaskDAO(client *clientv3.Client) *runningTaskDAO {
	return &runningTaskDAO{
		client: client,
		prefix: "demo_app/running",
	}
}

type runningTaskDAO struct {
	client *clientv3.Client
	prefix string
}

func (c *runningTaskDAO) Put(ctx context.Context, taskName, node, shard string) error {
	key := fmt.Sprintf("%s/%s/%s/%s", c.prefix, taskName, node, shard)
	_, err := c.client.Put(ctx, key, shard)
	return err
}

func (c *runningTaskDAO) Get(ctx context.Context, taskName, node, shard string) (string, error) {
	key := fmt.Sprintf("%s/%s/%s/%s", c.prefix, taskName, node, shard)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("etcd returns nothing")
	}

	return string(resp.Kvs[0].Value), nil
}

func (c *runningTaskDAO) ListByTaskNode(ctx context.Context, taskName, node string) (Shards, error) {
	key := fmt.Sprintf("%s/%s/%s/", c.prefix, taskName, node)
	resp, err := c.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var shards Shards
	for _, ev := range resp.Kvs {
		shards = append(shards, string(ev.Value))
	}

	return shards, nil
}

type Shards []string

type NodeWithShards struct {
	Node   string
	Shards Shards
}

type TaskWithNodesWithShards struct {
	TaskName       string
	NodeWithShards []*NodeWithShards
}

func (c *runningTaskDAO) ListByTask(ctx context.Context, task string) ([]*NodeWithShards, error) {
	key := fmt.Sprintf("%s/%s/", c.prefix, task)
	resp, err := c.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	shardsByNode := make(map[string]Shards)
	// app / running / task / node / shard
	for _, ev := range resp.Kvs {
		parts := strings.Split(string(ev.Key), "/")
		node := parts[3]
		shard := parts[4]
		shardsByNode[node] = append(shardsByNode[node], shard)
	}

	var res []*NodeWithShards
	for k, v := range shardsByNode {
		res = append(res, &NodeWithShards{Node: k, Shards: v})
	}

	return res, nil
}

func (c *runningTaskDAO) List(ctx context.Context) ([]*TaskWithNodesWithShards, error) {
	resp, err := c.client.Get(ctx, c.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var store = make(map[string]map[string]map[string]bool)

	// app / running / task / node / shard
	for _, ev := range resp.Kvs {
		parts := strings.Split(string(ev.Key), "/")
		task := parts[2]
		node := parts[3]
		shard := parts[4]
		_, has := store[task]
		if !has {
			store[task] = make(map[string]map[string]bool)
		}
		_, has = store[task][node]
		if !has {
			store[task][node] = make(map[string]bool)
		}
		store[task][node][shard] = true
	}
	var res []*TaskWithNodesWithShards
	for t, n := range store {
		var nodesWithShards []*NodeWithShards
		for u, v := range n {
			var shards []string
			for x, _ := range v {
				shards = append(shards, x)
			}
			nodesWithShards = append(nodesWithShards, &NodeWithShards{Node: u, Shards: shards})
		}
		res = append(res, &TaskWithNodesWithShards{TaskName: t, NodeWithShards: nodesWithShards})
	}

	return res, nil
}
