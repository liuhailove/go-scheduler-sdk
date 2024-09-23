# go-scheduler-sdk

很多公司java与go开发共存，java中有go-scheduler做为任务调度引擎，为此也出现了go执行器(客户端)，使用起来比较简单：

# 支持

```	
1.执行器注册
2.耗时任务取消
3.任务注册，像写http.Handler一样方便
4.任务panic处理
5.阻塞策略处理
6.任务完成支持返回执行备注
7.任务超时取消 (单位：秒，0为不限制)
8.失败重试次数(在参数param中，目前由任务自行处理)
9.可自定义日志
10.自定义日志查看handler
11.支持外部路由（可与gin集成）
```

# Example

```
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
		gs.ServerAddr("http://localhost:8082/gs-job-admin"),
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

// gscheduler.Logger接口实现
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
```

# 示例项目

github.com/github.com/liuhailove/go-scheduler-sdk/example/

# 与gin框架集成

https://github.com/liuhailove/go-scheduler-sdk

# go-scheduler-admin配置

### 添加执行器

执行器管理->新增执行器,执行器列表如下：

```
AppName		名称		注册方式	OnLine 		机器地址 		操作
golang-jobs	golang执行器	自动注册 		查看 ( 1 ）   
```

查看->注册节点

```
http://127.0.0.1:9999
```

### 添加任务

任务管理->新增(注意，使用BEAN模式，JobHandler与RegTask名称一致)

```
1	测试panic	BEAN：task.panic	* 0 * * * ?	admin	STOP	
2	测试耗时任务	BEAN：task.test2	* * * * * ?	admin	STOP	
3	测试golang	BEAN：task.test		* * * * * ?	admin	STOP
```

