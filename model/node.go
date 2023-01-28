package model

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"strings"
	"time"
)

func NewLeaderDataDAO(client *clientv3.Client) *leaderDataDAO {
	return &leaderDataDAO{
		client: client,
		prefix: "demo_app/leader_data",
	}
}

type leaderDataDAO struct {
	client *clientv3.Client
	prefix string
}

type Nodes []string

func (s *Nodes) Encode() string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (s *Nodes) Decode(value []byte) error {
	return json.Unmarshal(value, s)
}

func (c *leaderDataDAO) Put(ctx context.Context, nodes Nodes) error {
	key := fmt.Sprintf("%s/%s", c.prefix, "nodes")
	_, err := c.client.Put(ctx, key, nodes.Encode())
	return err
}

func (c *leaderDataDAO) Get(ctx context.Context) (Nodes, error) {
	key := fmt.Sprintf("%s/%s", c.prefix, "nodes")
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	s := Nodes{}
	err = s.Decode(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func NewLeaderDAO(client *clientv3.Client) *leaderDAO {
	return &leaderDAO{
		client: client,
		key:    "demo_app/leader_node",
	}
}

type leaderDAO struct {
	client *clientv3.Client
	key    string
}

func (c *leaderDAO) Get(ctx context.Context) (string, error) {
	resp, err := c.client.Get(ctx, c.key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", nil
	}

	return string(resp.Kvs[0].Value), nil
}

const EtcdCampaignTimeout = 30 // time.Second
const EtcdQueryTimeout = 3 * time.Second

func (c *leaderDAO) Campaign(parentCtx context.Context, node string) {
	go func() {
		ctx, cancel := context.WithCancel(parentCtx)
		defer func() {
			cancel()
		}()
		for {
			select {
			case <-ctx.Done():
				logrus.WithField("follower", node).Error("follower context done")
				return
			default:
			}

			s, err := concurrency.NewSession(c.client, concurrency.WithTTL(EtcdCampaignTimeout))
			//call Campaign()，only one can success here and become leader，others will block in here
			if err != nil {
				logrus.WithField("error", err).Error("new session error")
				time.Sleep(time.Second * 2)
				continue
			}

			e := concurrency.NewElection(s, c.key)
			err = e.Campaign(ctx, node)
			if err != nil {
				logrus.WithField("error", err).Error("campaign error")
				_ = s.Close()
				time.Sleep(time.Second)
				continue
			}

			logrus.WithField("leader", node).Info("compaign success")

			shouldBreak := false
			for !shouldBreak {
				select {
				case <-s.Done(): // network issue, session recreation, retry election
					logrus.WithField("leader", node).Error("campaign session done")
					shouldBreak = true
					break
				case <-ctx.Done():
					logrus.WithField("leader", node).Error("campaign context done")
					ctxTmp, cancelTmp := context.WithTimeout(context.Background(), EtcdQueryTimeout)
					_ = e.Resign(ctxTmp)
					_ = s.Close()
					cancelTmp()
					return
				}
			}
		}
	}()
}

func NewNodeDAO(client *clientv3.Client) *nodeDAO {
	return &nodeDAO{
		client: client,
		prefix: "demo_app/node",
	}
}

type nodeDAO struct {
	client *clientv3.Client
	prefix string
}

func (n *nodeDAO) Register(ctx context.Context, addr string) (*etcdRegistry, error) {
	key := fmt.Sprintf("%s/%s", n.prefix, addr)
	return KeepRegister(ctx, n.client, key, addr)
}

func (n *nodeDAO) List(ctx context.Context) ([]string, error) {
	resp, err := n.client.Get(ctx, n.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var servers []string
	for _, ev := range resp.Kvs {
		sarray := strings.Split(string(ev.Key), "/")
		servers = append(servers, sarray[len(sarray)-1])
	}
	return servers, nil
}

func KeepRegister(ctx context.Context, cli *clientv3.Client, key, value string) (*etcdRegistry, error) {
	r := NewEtcdRegistry(cli)
	err := r.KeepRegister(ctx, key, value)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func NewEtcdRegistry(cli *clientv3.Client) *etcdRegistry {
	ctx, cancel := context.WithCancel(context.Background())
	return &etcdRegistry{
		client:     cli,
		cancelFunc: cancel,
		ctx:        ctx,
	}
}

type etcdRegistry struct {
	ctx           context.Context
	client        *clientv3.Client
	key           string
	value         string
	leaseId       clientv3.LeaseID
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	cancelFunc    context.CancelFunc
}

const EtcdKeyTTL = 10
const EtcdKeepAliveRetryInterval = 5 * time.Second

func (e *etcdRegistry) KeepRegister(ctx context.Context, key, value string) error {
	e.key = key
	e.value = value

	ctx, cancel := context.WithTimeout(ctx, EtcdQueryTimeout)
	defer cancel()

	grantResp, err := e.client.Grant(ctx, int64(EtcdKeyTTL))
	if err != nil {
		return err
	}

	_, err = e.client.Put(ctx, key, value, clientv3.WithLease(grantResp.ID))
	if err != nil {
		return err
	}

	e.leaseId = grantResp.ID
	c, err := e.client.KeepAlive(context.Background(), e.leaseId)
	if err != nil {
		return err
	}
	e.keepAliveChan = c

	go e.keepAlive()
	return nil
}

func (e *etcdRegistry) DeRegister(ctx context.Context) error {
	e.cancelFunc()
	ctx, cancel := context.WithTimeout(ctx, EtcdQueryTimeout)
	defer cancel()

	_, err := e.client.Revoke(ctx, e.leaseId)
	return err
}

func (e *etcdRegistry) keepAlive() {
	for {
		select {
		case resp := <-e.keepAliveChan:
			if resp == nil {
				logrus.WithField("resp", resp).Error("etcd keepalive error")
				go e.retry()
				return
			}
		case <-e.ctx.Done():
			return
		}
	}
}
func (e *etcdRegistry) retry() {
	ticker := time.Tick(EtcdKeepAliveRetryInterval)
	for {
		select {
		case <-ticker:
			err := e.KeepRegister(context.Background(), e.key, e.value)
			if err == nil {
				logrus.WithField("key", e.key).WithField("value", e.value).Info("retry keep register success")
				return
			}
			logrus.WithField("key", e.key).WithField("value", e.value).Error("retry keep register error", zap.Error(err))
		case <-e.ctx.Done():
			return
		}
	}
}
