package model

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
)

type IRunningTaskDAO interface {
	Put(ctx context.Context, taskName, node, shard string) error
	Get(ctx context.Context, taskName, node, shard string) (string, error)
	Delete(ctx context.Context, taskName, node, shard string) error
	DeleteTaskNode(ctx context.Context, taskName, node string) error
	DeleteTask(ctx context.Context, taskName string) error
	DeleteAll(ctx context.Context) error
	ListByTaskNode(ctx context.Context, taskName, node string) (Shards, error)
	ListByTask(ctx context.Context, task string) ([]*NodeWithShards, error)
	List(ctx context.Context) ([]*TaskWithNodesWithShards, error)
}

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

func (c *runningTaskDAO) Delete(ctx context.Context, taskName, node, shard string) error {
	key := fmt.Sprintf("%s/%s/%s/%s", c.prefix, taskName, node, shard)
	_, err := c.client.Delete(ctx, key)
	return err
}

func (c *runningTaskDAO) DeleteTaskNode(ctx context.Context, taskName, node string) error {
	key := fmt.Sprintf("%s/%s/%s", c.prefix, taskName, node)
	_, err := c.client.Delete(ctx, key, clientv3.WithPrefix())
	return err
}

func (c *runningTaskDAO) DeleteTask(ctx context.Context, taskName string) error {
	key := fmt.Sprintf("%s/%s", c.prefix, taskName)
	_, err := c.client.Delete(ctx, key, clientv3.WithPrefix())
	return err
}

func (c *runningTaskDAO) DeleteAll(ctx context.Context) error {
	_, err := c.client.Delete(ctx, c.prefix, clientv3.WithPrefix())
	return err
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

func (s *Shards) Encode() string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (s *Shards) Decode(value []byte) error {
	return json.Unmarshal(value, s)
}

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

	var store = make(map[string]map[string][]string)

	// app / running / task / node / shard
	for _, ev := range resp.Kvs {
		parts := strings.Split(string(ev.Key), "/")
		task := parts[2]
		node := parts[3]
		shard := parts[4]
		_, has := store[task]
		if !has {
			store[task] = make(map[string][]string)
		}
		_, has = store[task][node]
		if !has {
			store[task][node] = []string{}
		}
		store[task][node] = append(store[task][node], shard)
	}
	var res []*TaskWithNodesWithShards
	for t, n := range store {
		var nodesWithShards []*NodeWithShards
		for u, v := range n {
			nodesWithShards = append(nodesWithShards, &NodeWithShards{Node: u, Shards: v})
		}
		res = append(res, &TaskWithNodesWithShards{TaskName: t, NodeWithShards: nodesWithShards})
	}

	return res, nil
}
