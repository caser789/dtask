package model

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewSystemConfigDAO(client *clientv3.Client) *systemConfigDAO {
	return &systemConfigDAO{
		client: client,
		key:    "demo_app/system_config",
	}
}

type systemConfigDAO struct {
	client *clientv3.Client
	key    string
}

func (c *systemConfigDAO) Put(ctx context.Context, value *SystemConfig) error {
	_, err := c.client.Put(ctx, c.key, value.Encode())
	return err
}

func (c *systemConfigDAO) Get(ctx context.Context) (*SystemConfig, error) {
	resp, err := c.client.Get(ctx, c.key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("etcd returns nothing")
	}

	var s = &SystemConfig{}
	err = s.Decode(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func NewTaskConfigDAO(client *clientv3.Client) *taskConfigDAO {
	return &taskConfigDAO{
		client: client,
		prefix: "demo_app/task_config",
	}
}

type taskConfigDAO struct {
	client *clientv3.Client
	prefix string
}

func (c *taskConfigDAO) getKey(taskName string) string {
	return fmt.Sprintf("%s/%s", c.prefix, taskName)
}

func (c *taskConfigDAO) Put(ctx context.Context, value *TaskConfig) error {
	_, err := c.client.Put(ctx, c.getKey(value.Name), value.Encode())
	return err
}

func (c *taskConfigDAO) Get(ctx context.Context, taskName string) (*TaskConfig, error) {
	resp, err := c.client.Get(ctx, c.getKey(taskName))
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("etcd returns nothing")
	}

	var s = &TaskConfig{}
	err = s.Decode(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (c *taskConfigDAO) List(ctx context.Context) ([]*TaskConfig, error) {
	resp, err := c.client.Get(ctx, c.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var configs []*TaskConfig
	for _, ev := range resp.Kvs {
		var s = &TaskConfig{}
		err = s.Decode(ev.Value)
		if err != nil {
			return nil, err
		}

		configs = append(configs, s)
	}
	return configs, nil
}

type TaskConfigStore map[string]*TaskConfig

func (c *taskConfigDAO) Watch(ctx context.Context) (<-chan TaskConfigStore, error) {
	taskConfigList, err := c.List(ctx)
	if err != nil {
		return nil, err
	}

	store := make(TaskConfigStore)
	for _, t := range taskConfigList {
		store[t.Name] = t
	}

	rch := c.client.Watch(ctx, c.prefix, clientv3.WithPrefix())
	res := make(chan TaskConfigStore)
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
					taskConfig := new(TaskConfig)
					err := taskConfig.Decode(ev.Kv.Value)
					if err != nil {
						logrus.WithField("value", ev.Kv.Value).Error("decode put event error")
						continue
					}

					switch ev.Type {
					case mvccpb.PUT:
						store[taskConfig.Name] = taskConfig
						res <- store
						logrus.WithField("value", taskConfig).Info("new task config put")
					case mvccpb.DELETE:
						delete(store, taskConfig.Name)
						res <- store
						logrus.WithField("value", taskConfig).Info("new task config delete")
					}
				}
			}
		}
	}()

	return res, nil
}

func (c *taskConfigDAO) WatchTaskConfig(ctx context.Context, config *TaskConfig) (<-chan *TaskConfig, error) {
	rch := c.client.Watch(ctx, c.getKey(config.Name))
	res := make(chan *TaskConfig)
	go func() {
		for {
			select {
			case resp, ok := <-rch:
				if !ok {
					rch = c.client.Watch(ctx, c.getKey(config.Name))
					logrus.WithField("task_name", config.Name).Error("rch closed")
					continue
				}
				for _, ev := range resp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						taskConfig := new(TaskConfig)
						err := taskConfig.Decode(ev.Kv.Value)
						if err != nil {
							logrus.WithField("value", ev.Kv.Value).Error("decode put event error")
							continue
						}
						res <- taskConfig
						logrus.WithField("value", taskConfig).Info("new task config put")
					case mvccpb.DELETE:
						return
					}
				}
			}
		}
	}()

	return res, nil
}

type SystemConfig struct {
	// Check leader changing every 10 seconds
	LeaderChangeCheckInterval int64 `json:"leader_change_check_interval"`
	// Check node changing every 30 seconds
	NodeChangeCheckInterval int64 `json:"node_change_check_interval"`
}

func (c *SystemConfig) Encode() string {
	b, _ := json.Marshal(c)
	return string(b)
}

func (c *SystemConfig) Decode(value []byte) error {
	return json.Unmarshal(value, c)
}

type TaskConfig struct {
	// unique name
	Name string `json:"name"`
	// only for timer task - Full crontab specs, e.g. "* * * * * ?"
	Spec string `json:"spec"`
	// total shards among all nodes
	ShardNumber uint32 `json:"shard_number"`
	// when fetch data,take limit size
	TakeLimit uint32 `json:"take_limit"`
	// if no data, pause milliseconds
	NoDataPause uint64 `json:"no_data_pause"`
	// custom parameter
	Params string `json:"params"`
	// turn on or off
	Alive bool `json:"alive"`
	// task type
	TaskType InternalTaskType `json:"task_type"`
}

func (c *TaskConfig) Encode() string {
	b, _ := json.Marshal(c)
	return string(b)
}

func (c *TaskConfig) Decode(value []byte) error {
	return json.Unmarshal(value, c)
}

type InternalTaskType uint32

const (
	InternalTaskType_Task                        InternalTaskType = 1 //task
	InternalTaskType_Schedule                    InternalTaskType = 2 //timer task
	InternalTaskType_SummaryMapTask              InternalTaskType = 3 //summary task - MapTask
	InternalTaskType_SummaryReduce1Task          InternalTaskType = 4 //summary task - Reduce1Task
	InternalTaskType_SummaryReduce2Task          InternalTaskType = 5 //summary task - Reduce2Task
	InternalTaskType_SummarySaveReduceResultTask InternalTaskType = 6 //summary task - SaveReduceResultTask
	InternalTaskType_SummaryCleanTask            InternalTaskType = 7 //summary task - CleanTask
)
