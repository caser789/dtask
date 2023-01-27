package dtask

import (
	"context"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestA(t *testing.T) {
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: EtcdQueryTimeout,
		Context:     ctx,
	})

	if err != nil {
		logrus.WithField("error", zap.Error(err)).Error("new etcd client error")
		return
	}

	m := NewNodeModel(cli)

	node1 := "1.1.1.1:8080"
	r1, err := m.Register(ctx, node1)
	if err != nil {
		logrus.Error("register error", zap.String("addr", node1), zap.Error(err))
		return
	}

	node2 := "2.2.2.2:8080"
	r2, err := m.Register(ctx, node2)
	if err != nil {
		logrus.Error("register error", zap.String("addr", node2), zap.Error(err))
		return
	}

	servers, err := m.List(ctx)
	if err != nil {
		logrus.WithField("err", zap.Error(err)).Error("list error")
		return
	}
	logrus.WithField("servers", zap.Strings("servers", servers)).Info("list ok")

	m.Campaign(ctx, node1)
	m.Campaign(ctx, node2)

	ticker := time.Tick(EtcdKeepAliveRetryInterval)
	var count int
	for {
		select {
		case <-ticker:
			hasLeader, err := m.HasLeader(ctx)
			if err != nil {
				logrus.WithField("err", zap.Error(err)).Error("has leader error")
				return
			}
			logrus.WithField("has leader", hasLeader).Info("has leader")

			isLeader1, err := m.IsLeader(ctx, node1)
			if err != nil {
				logrus.WithField("err", err).Error("is leader1 error")
				return
			}
			logrus.WithField("node", node1).WithField("is leader", isLeader1).Info("node 1")

			isLeader2, err := m.IsLeader(ctx, node2)
			if err != nil {
				logrus.WithField("err", zap.Error(err)).Error("is leader2 error")
				return
			}
			logrus.WithField("node", node2).WithField("is leader", isLeader2).Info("node 2")

			servers, err := m.List(ctx)
			if err != nil {
				logrus.WithField("err", zap.Error(err)).Error("list error")
				return
			}
			logrus.WithField("servers", zap.Strings("servers", servers)).Info("list ok")
			count += 1
			if count == 10 {
				err := r1.DeRegister(ctx)
				if err != nil {
					logrus.WithField("err", zap.Error(err)).Error("deregister error")
				}
			}
			if count == 20 {
				err := r2.DeRegister(ctx)
				if err != nil {
					logrus.WithField("err", zap.Error(err)).Error("deregister error")
				}
			}
		}
	}
}
