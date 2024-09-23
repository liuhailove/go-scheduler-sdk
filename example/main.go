package main

import (
	"context"
	"fmt"
	gs "github.com/liuhailove/go-scheduler-sdk"
	"github.com/liuhailove/go-scheduler-sdk/example/task"
	"log"
)

func main() {
	exec := gs.NewExecutor(
		gs.ServerAddr("http://localhost:8082/gscheduler"),
		gs.AccessToken(""),                 //请求令牌(默认为空)
		gs.RegistryKey("my-golang-jobs-2"), //执行器名称
		gs.SetLogger(&logger{}),            //自定义日志
		gs.SetSync(true),
		gs.SetServerMode(gs.FIX_SERVER_MODE),
		gs.SetCollapse(true),
		gs.SetMaxConcurrencyNum(500),
		gs.WithTaskWrapper(func(taskFunc gs.TaskFunc) gs.TaskFunc {
			return func(ctx context.Context, param *gs.RunReq) ([]string, error) {
				ctx = context.WithValue(ctx, "firstKey", "firstKey")
				return taskFunc(ctx, param)
			}
		}),
		gs.WithTaskWrapper(func(taskFunc gs.TaskFunc) gs.TaskFunc {
			return func(ctx context.Context, param *gs.RunReq) ([]string, error) {
				fmt.Println(ctx.Value("firstKey"))
				ctx = context.WithValue(ctx, "secondKey", "second param ctx")
				return taskFunc(ctx, param)
			}
		}),
	)
	exec.Init()
	//设置日志查看handler
	exec.LogHandler(func(req *gs.LogReq) *gs.LogRes {
		return &gs.LogRes{Code: 200, Msg: "", Content: gs.LogResContent{
			FromLineNum: req.FromLineNum,
			ToLineNum:   2,
			LogContent:  "这个是自定义日志handler",
			IsEnd:       true,
		}}
	})
	// 注册任务handler
	exec.RegTask("task.test", task.Test)
	exec.RegTask("task.test2", task.Test2)
	exec.RegTask("task.test3", task.Test3)
	exec.RegTask("task.testP", task.TestP)
	exec.RegTask("task.panic", task.Panic)
	exec.RegTask("task.dbShardingMapReduceTaskFunc", task.DbShardingMapReduceTaskFunc)
	exec.RegTask("task.largeShardingMapReduceTaskFunc", task.LargeShardingMapReduceTaskFunc)
	exec.RegTask("task.tableShardingMapReduceTaskFunc", task.TableShardingMapReduceTaskFunc)
	exec.RegTask("task.rowShardingMapReduceTaskFunc", task.RowShardingMapReduceTaskFunc)
	exec.RegTask("task.rowSumShardingMapReduceTaskFunc", task.RowSumShardingMapReduceTaskFunc)
	exec.RegTask("task.rowReduceShardingMapReduceTaskFunc", task.RowReduceShardingMapReduceTaskFunc)
	exec.RegTask("TaskBillGenerate", task.TaskBillGenerate)
	exec.RegTask("DisburseSubTask", task.TaskBillGenerate)

	// 测试自定义串行
	exec.RegTask("task.CustomerShardingTaskFunc", task.CustomerShardingTaskFunc)
	exec.RegTask("task.CustomerPrintTaskFunc", task.CustomerPrintTaskFunc)

	// core test
	exec.RegTask("task.BillGenerateTask", task.BillGenerateTask)
	exec.RegTask("task.BillingFileTask", task.BillingFileTask)
	exec.RegTask("task.DpdnPlanTask", task.DpdnPlanTask)
	exec.RegTask("task.OverdueFileTask", task.OverdueFileTask)
	exec.RegTask("task.OverduePlanFileTask", task.OverduePlanFileTask)
	exec.RegTask("task.PlanRemainFileTask", task.PlanRemainFileTask)
	exec.RegTask("task.TaskBizDateChange", task.TaskBizDateChange)
	exec.RegTask("task.TaskFeeCalculate", task.TaskFeeCalculate)
	exec.Run()

	fmt.Println(exec.GetAddress())
}

// xxl.Logger接口实现
type logger struct{}

func (l *logger) Info(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf("自定义日志 [Info]- "+format, a...))
}

func (l *logger) Error(format string, a ...interface{}) {
	log.Println(fmt.Sprintf("自定义日志 [Error]- "+format, a...))
}

func (l *logger) Debug(format string, a ...interface{}) {
	log.Println(fmt.Sprintf("自定义日志 [Debug]- "+format, a...))
}
