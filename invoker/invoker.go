package invoker

import (
	"context"
	"github.com/caser789/dtask/model"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"time"
)

type IInvoker interface {
	Execute()
	ReloadTaskConfig(taskConfig *model.TaskConfig)
	StopRunning() <-chan bool
	Start()
	GetShard() int32
}

// RtBaseInvoker
// implementations:
//    StopRunning(),Start(),GetShard()
// not implemented:
//    Execute(),ReloadTaskConfig()
//
type RtBaseInvoker struct {
	invoker           IInvoker
	shard             int32
	stopChan          chan bool
	stopCompletedChan chan bool
	stopOnce          sync.Once
	taskConfig        *model.TaskConfig
	LogPrefix         string
	ServerAddress     string
}

func newRtBaseInvoker(ctx context.Context, serverAddress string, taskConfig *model.TaskConfig, shard int32) RtBaseInvoker {
	return RtBaseInvoker{
		shard:             shard,
		taskConfig:        taskConfig,
		stopChan:          make(chan bool, 1),
		stopCompletedChan: make(chan bool, 1),
		ServerAddress:     serverAddress,
	}
}

func (r *RtBaseInvoker) StopRunning() <-chan bool {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
	return r.stopCompletedChan
}

func (r *RtBaseInvoker) Start() {
	go func() {
		for {
			select {
			case <-r.stopChan:
				close(r.stopCompletedChan)
				return
			default:
				r.invoker.Execute()
			}
		}
	}()
}

func (r *RtBaseInvoker) GetShard() int32 {
	return r.shard
}

type RtTaskInvoker struct {
	RtBaseInvoker
	rtTasks []RtTask
}

func NewRtTaskInvoker(ctx context.Context, task *model.TaskConfig, node string, shard string, rtTasks []RtTask) IInvoker {
	r := &RtTaskInvoker{rtTasks: []RtTask{&dummyRtTask{}}}
	a, _ := strconv.ParseInt(shard, 10, 32)
	logrus.WithField("shard", shard).WithField("int shard", a).Info("new rt task invoker")
	r.RtBaseInvoker = newRtBaseInvoker(ctx, node, task, int32(a))
	r.invoker = r
	return r
}

func (r *RtTaskInvoker) Execute() {
	//must catch panic error
	hasDataGlobal := false
	for _, rtTask := range r.rtTasks {
		elements := rtTask.Take()
		logrus.WithField("node", r.RtBaseInvoker.ServerAddress).WithField("shard", r.RtBaseInvoker.shard).WithField("elements", elements).Info("task tasks")
		if len(elements) == 0 {
			continue
		}

		hasDataGlobal = true
		for _, element := range elements {
			select {
			case <-r.stopChan:
				//invoker is closed
				return
			default:
				rtTask.Execute(element)
			}
		}
	}

	time.Sleep(time.Second * 60)
	if !hasDataGlobal {
		time.Sleep(time.Second * 600)
	}
}

func (r *RtTaskInvoker) ReloadTaskConfig(taskConfig *model.TaskConfig) {
	r.taskConfig = taskConfig
}

type RtTask interface {
	// Take get task records by taskConfig and shard
	Take() []*Task

	// Execute execute logic for each task record
	// Result: true: execute success
	Execute(*Task) bool
}

//Task identify a normal task
type Task struct {
	Id           uint64
	Type         uint32
	Status       uint32
	Started      bool
	ExecuteTime  int32
	ExecuteCount uint32
	BizId        string
	BizParam     string
	Part         int32
	CreateTime   int32
	UpdateTime   int32
}

type dummyRtTask struct{}

func (t *dummyRtTask) Take() []*Task {
	return []*Task{
		{Id: 1, BizId: "a"},
		{Id: 2, BizId: "b"},
		{Id: 3, BizId: "c"},
	}
}

func (t *dummyRtTask) Execute(task *Task) bool {
	logrus.WithField("id", task.Id).WithField("biz id", task.BizId).Info("execute dummy rt task")
	return true
}
