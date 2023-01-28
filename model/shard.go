package model

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
)

func NewShardDAO(client *clientv3.Client) *shardDAO {
	return &shardDAO{
		client: client,
		prefix: "demo_app/shard",
	}
}

type shardDAO struct {
	client *clientv3.Client
	prefix string
}

func (c *shardDAO) Put(ctx context.Context, taskName, node string, shards Shards) error {
	key := fmt.Sprintf("%s/%s/%s", c.prefix, taskName, node)
	_, err := c.client.Put(ctx, key, shards.Encode())
	return err
}

func (c *shardDAO) Get(ctx context.Context, taskName, node string) (Shards, error) {
	key := fmt.Sprintf("%s/%s/%s", c.prefix, taskName, node)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	s := Shards{}
	err = s.Decode(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (c *shardDAO) GetByTask(ctx context.Context, taskName string) ([]*NodeWithShards, error) {
	key := fmt.Sprintf("%s/%s", c.prefix, taskName)
	resp, err := c.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var res []*NodeWithShards
	// app / shard / task / node
	for _, ev := range resp.Kvs {
		parts := strings.Split(string(ev.Key), "/")
		node := parts[3]
		s := Shards{}
		err = s.Decode(ev.Value)
		if err != nil {
			return nil, err
		}

		res = append(res, &NodeWithShards{Node: node, Shards: s})
	}

	return res, nil
}

func (c *shardDAO) GetAll(ctx context.Context) ([]*TaskWithNodesWithShards, error) {
	resp, err := c.client.Get(ctx, c.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var res []*TaskWithNodesWithShards
	// app / shard / task / node
	for _, ev := range resp.Kvs {
		parts := strings.Split(string(ev.Key), "/")
		task := parts[2]
		node := parts[3]
		s := Shards{}
		err = s.Decode(ev.Value)
		if err != nil {
			return nil, err
		}

		if len(res) == 0 || task != res[len(res)-1].TaskName {
			res = append(res, &TaskWithNodesWithShards{TaskName: task, NodeWithShards: []*NodeWithShards{}})
		}
		if len(res[len(res)-1].NodeWithShards) == 0 || res[len(res)-1].NodeWithShards[len(res[len(res)-1].NodeWithShards)-1].Node != node {
			res[len(res)-1].NodeWithShards = append(res[len(res)-1].NodeWithShards, &NodeWithShards{Node: node})
		}
		res[len(res)-1].NodeWithShards[len(res[len(res)-1].NodeWithShards)-1].Shards = s
	}

	return res, nil
}

func (c *shardDAO) Delete(ctx context.Context, taskName, node string) error {
	key := fmt.Sprintf("%s/%s/%s", c.prefix, taskName, node)
	_, err := c.client.Delete(ctx, key)
	return err
}

func (c *shardDAO) DeleteTask(ctx context.Context, taskName string) error {
	key := fmt.Sprintf("%s/%s", c.prefix, taskName)
	_, err := c.client.Delete(ctx, key, clientv3.WithPrefix())
	return err
}

func (c *shardDAO) DeleteAll(ctx context.Context) error {
	_, err := c.client.Delete(ctx, c.prefix, clientv3.WithPrefix())
	return err
}

func (c *shardDAO) WatchShards(ctx context.Context, task, node string) (<-chan Shards, error) {
	key := fmt.Sprintf("%s/%s/%s", c.prefix, task, node)
	rch := c.client.Watch(ctx, key)
	res := make(chan Shards)
	go func() {
		for {
			select {
			case resp, ok := <-rch:
				if !ok {
					rch = c.client.Watch(ctx, key)
					logrus.WithField("task_name", task).WithField("node", node).Error("rch closed")
					continue
				}
				for _, ev := range resp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						s := Shards{}
						err := s.Decode(ev.Kv.Value)
						if err != nil {
							logrus.WithField("task", task).WithField("node", node).Info("shard updated decode put error")
							continue
						}

						res <- s
						logrus.WithField("task", task).WithField("node", node).Info("shard updated")
					case mvccpb.DELETE:
						return
					}
				}
			}
		}
	}()

	return res, nil
}

type TaskNodeShardStore map[string]map[string]Shards

func (c *shardDAO) WatchForNewTask(ctx context.Context) (<-chan TaskNodeShardStore, error) {
	taskNodeShardsList, err := c.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	store := make(TaskNodeShardStore)
	for _, t := range taskNodeShardsList {
		_, has := store[t.TaskName]
		if !has {
			store[t.TaskName] = make(map[string]Shards)
		}
		for _, v := range t.NodeWithShards {
			store[t.TaskName][v.Node] = v.Shards
		}
	}

	rch := c.client.Watch(ctx, c.prefix, clientv3.WithPrefix())
	res := make(chan TaskNodeShardStore)
	go func() {
		for {
			select {
			case resp, ok := <-rch:
				if !ok {
					rch = c.client.Watch(ctx, c.prefix, clientv3.WithPrefix())
					logrus.WithField("prefix", c.prefix).Error("rch closed")
					continue
				}

				for _, ev := range resp.Events {
					parts := strings.Split(string(ev.Kv.Key), "/")
					task := parts[2]
					node := parts[3]
					s := Shards{}
					err = s.Decode(ev.Kv.Value)
					if err != nil {
						logrus.WithField("task", task).WithField("node", node).Error("decode put event error")
						continue
					}

					switch ev.Type {
					case mvccpb.PUT:
						_, has := store[task]
						if !has {
							store[task] = make(map[string]Shards)
						}
						store[task][node] = s
						if !has {
							// new task
							res <- store
						}
						logrus.WithField("task", task).WithField("node", node).WithField("shards", s).Info("new task put")
					case mvccpb.DELETE:
						logrus.WithField("task", task).WithField("node", node).WithField("shards", s).Info("new task delete")
						_, has := store[task]
						if !has {
							continue
						}
						_, has = store[task][node]
						if !has {
							continue
						}
						delete(store[task], node)
						if len(store) == 0 {
							delete(store, task)
						}
					}
				}
			}
		}
	}()

	return res, nil
}
