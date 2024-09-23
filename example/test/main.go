package main

import (
	"context"
	"fmt"
	"time"
)

// Task 任务
type Task struct {
	Id        int64
	Name      string
	Ext       context.Context
	Param     *RunReq
	fn        TaskFunc
	Cancel    context.CancelFunc
	StartTime int64
	EndTime   int64
	//日志
}

// TaskFunc 任务执行函数
type TaskFunc func(cxt context.Context, param *RunReq) string

// RunReq 触发任务请求参数
type RunReq struct {
	JobID                 int64  `json:"jobId"`                 // 任务ID
	ExecutorHandler       string `json:"executorHandler"`       // 任务标识
	ExecutorParams        string `json:"executorParams"`        // 任务参数
	ExecutorBlockStrategy string `json:"executorBlockStrategy"` // 任务阻塞策略
	ExecutorTimeout       int64  `json:"executorTimeout"`       // 任务超时时间，单位秒，大于零时生效
	LogID                 int64  `json:"logId"`                 // 本次调度日志ID
	LogDateTime           int64  `json:"logDateTime"`           // 本次调度日志时间
	GlueType              string `json:"glueType"`              // 任务模式，可选值参考 GlueTypeEnum
	GlueSource            string `json:"glueSource"`            // GLUE脚本代码
	GlueUpdatetime        int64  `json:"glueUpdatetime"`        // GLUE脚本更新时间，用于判定脚本是否变更以及是否需要刷新
	BroadcastIndex        int64  `json:"broadcastIndex"`        // 分片参数：当前分片
	BroadcastTotal        int64  `json:"broadcastTotal"`        // 分片参数：总分片
}

// Run 运行任务
func (t *Task) Run(str string) {
	fmt.Printf("id:=%d,name=%s,msg=%s,time=%s\n", t.Id, t.Name, str, time.Now().String())
	time.Sleep(time.Second)
	return
}

var task = &Task{}

func Run() {
	task.Id = 1
	task.Name = "task1"
	go task.Run("hello")
	task = &Task{}
	task.Id = 2
	task.Name = "task2"
	go task.Run("world")

}

func main() {
	Run()
	time.Sleep(time.Second * 10)
}
