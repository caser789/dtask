package dtask

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"strings"
	"time"
)

type IModel interface {
	RegisterNode(addr string) error
}

type memModel struct {
	store map[string]bool
}

func (m *memModel) RegisterNode(addr string) error {
	m.store[addr] = true
	return nil
}

func NewNodeModel(client *clientv3.Client) *nodeModel {
	return &nodeModel{client: client}
}

type nodeModel struct {
	client *clientv3.Client
}

func (n *nodeModel) getNodeKey(addr string) string {
	return fmt.Sprintf("app_name/node/%s", addr)
}

func (n *nodeModel) getLeaderKey() string {
	return "app_name/leader"
}

func (n *nodeModel) Register(ctx context.Context, addr string) (*etcdRegistry, error) {
	return KeepRegister(ctx, n.client, n.getNodeKey(addr), addr)
}

func (n *nodeModel) List(ctx context.Context) ([]string, error) {
	resp, err := n.client.Get(ctx, NodePath, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var servers []string
	for _, ev := range resp.Kvs {
		sarray := strings.Split(string(ev.Key), Separator)
		servers = append(servers, sarray[len(sarray)-1])
	}
	return servers, nil
}

func (n *nodeModel) Campaign(parentCtx context.Context, addr string) {
	go func() {
		ctx, cancel := context.WithCancel(parentCtx)
		defer func() {
			cancel()
		}()
		for {
			select {
			case <-ctx.Done():
				logrus.WithField("follower", addr).Error("follower context done")
				return
			default:
			}

			s, err := concurrency.NewSession(n.client, concurrency.WithTTL(EtcdCampaignTimeout))
			//call Campaign()，only one can success here and become leader，others will block in here
			if err != nil {
				logrus.WithField("error", err).Error("new session error")
				time.Sleep(time.Second * 2)
				continue
			}

			e := concurrency.NewElection(s, n.getLeaderKey())
			err = e.Campaign(ctx, addr)
			if err != nil {
				logrus.WithField("error", err).Error("campaign error")
				_ = s.Close()
				time.Sleep(time.Second)
				continue
			}

			logrus.WithField("leader", addr).Info("compaign success")

			shouldBreak := false
			for !shouldBreak {
				select {
				case <-s.Done(): // network issue, session recreation, retry election
					logrus.WithField("leader", addr).Error("campaign session done")
					shouldBreak = true
					break
				case <-ctx.Done():
					logrus.WithField("leader", addr).Error("campaign context done")
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

func (n *nodeModel) HasLeader(ctx context.Context) (bool, error) {
	resp, err := n.client.Get(ctx, n.getLeaderKey(), clientv3.WithFirstCreate()...)
	if err != nil {
		return false, err
	}

	return len(resp.Kvs) != 0, nil
}

func (n *nodeModel) IsLeader(ctx context.Context, addr string) (bool, error) {
	resp, err := n.client.Get(ctx, n.getLeaderKey(), clientv3.WithFirstCreate()...)
	if err != nil {
		return false, err
	}

	if len(resp.Kvs) == 0 {
		return false, nil
	}

	return string(resp.Kvs[0].Value) == addr, nil
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
