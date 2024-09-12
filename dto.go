package gs

import "fmt"

// 通用响应
type res struct {
	Code int64       `json:"code"` // 200 表示正常、其他失败
	Msg  interface{} `json:"msg"`  // 错误提示消息
}

/*****************  上行参数  *********************/

// Registry 注册参数
type Registry struct {
	RegistryGroup string `json:"registryGroup"`
	RegistryKey   string `json:"registryKey"`
	// LoadStat load1负载
	LoadStat float64 `json:"loadStat"`
	// CpuStat cpu 使用率
	CpuStat float64 `json:"cpuStat"`
	// MemoryStat 内存使用率
	MemoryStat    int64  `json:"memoryStat"`
	RegistryValue string `json:"registryValue"`

	// routerFlag 路由标签
	RouterFlag string `json:"routerFlag"`
}

// 执行器执行完任务后，回调任务结果时使用
type call []*callElement

type callElement struct {
	JobID         int64          `json:"jobId"`
	LogID         int64          `json:"logId"`
	ClientVersion string         `json:"clientVersion"`
	LogDateTim    int64          `json:"logDateTim"`
	ExecuteResult *ExecuteResult `json:"executeResult"`
	//以下是7.31版本 v2.3.0 Release所使用的字段
	HandleCode int      `json:"handleCode"` //200表示正常,500表示失败
	HandleMsg  []string `json:"handleMsg"`

	// 新版本增加
	HandleFinishDateTime int64 `json:"handleFinishDateTime"`
}

// reportDelay 上报延迟执行结构体
type reportDelay struct {
	JobID          int64  `json:"jobId"`
	LogID          int64  `json:"logId"`
	LogDateTim     int64  `json:"logDateTim"`
	CurrentDateTim int64  `json:"currentDateTim"`
	Address        string `json:"address"`
}

// ExecuteResult 任务执行结果 200 表示任务执行正常，500表示失败
type ExecuteResult struct {
	Code int64       `json:"code"`
	Msg  interface{} `json:"msg"`
}

/*****************  下行参数  *********************/

// 阻塞处理策略
const (
	serialExecution     = "SERIAL_EXECUTION"     //单机串行
	discardLater        = "DISCARD_LATER"        //丢弃后续调度
	coverEarly          = "COVER_EARLY"          //覆盖之前调度
	concurrentExecution = "CONCURRENT_EXECUTION" //单机并行
)

// RunReq 触发任务请求参数
type RunReq struct {
	JobID                                        int64  `json:"jobId"`                                        // 任务ID
	ExecutorHandler                              string `json:"executorHandler"`                              // 任务标识
	ExecutorParams                               string `json:"executorParams"`                               // 任务参数
	ExecutorBlockStrategy                        string `json:"executorBlockStrategy"`                        // 任务阻塞策略
	ExecutorTimeout                              int64  `json:"executorTimeout"`                              // 任务超时时间，单位秒，大于零时生效
	LogID                                        int64  `json:"logId"`                                        // 本次调度日志ID
	ParentLog                                    int64  `json:"parentLog"`                                    // 本次调度的父日志ID
	InstanceId                                   string `json:"instanceId"`                                   // 实例ID，一次运行串联的任务实例ID一致
	LogDateTime                                  int64  `json:"logDateTime"`                                  // 本次调度日志时间
	GlueType                                     string `json:"glueType"`                                     // 任务模式，可选值参考 com.xxl.job.core.glue.GlueTypeEnum
	GlueSource                                   string `json:"glueSource"`                                   // GLUE脚本代码
	GlueUpdatetime                               int64  `json:"glueUpdatetime"`                               // GLUE脚本更新时间，用于判定脚本是否变更以及是否需要刷新
	BroadcastIndex                               int64  `json:"broadcastIndex"`                               // 分片参数：当前分片
	BroadcastTotal                               int64  `json:"broadcastTotal"`                               // 分片参数：总分片
	BusinessStartExecutorToleranceThresholdInMin int32  `json:"businessStartExecutorToleranceThresholdInMin"` // 业务开始执行容忍阈值
}

func (r *RunReq) String() string {
	return fmt.Sprintf("{jobId=%d,logId=%d,parentLog=%d,instanceId=%s,executorHandler=%s,executorBlockStrategy=%s,executorTimeout=%d,logDateTime=%d,broadcastIndex=%d,broadcastTotal=%d,businessStartExecutorToleranceThresholdInMin=%d}",
		r.JobID,
		r.LogID,
		r.ParentLog,
		r.InstanceId,
		r.ExecutorHandler,
		r.ExecutorBlockStrategy,
		r.ExecutorTimeout,
		r.LogDateTime,
		r.BroadcastIndex,
		r.BroadcastTotal,
		r.BusinessStartExecutorToleranceThresholdInMin)
}

// 终止任务请求参数
type killReq struct {
	JobID int64 `json:"jobId"` // 任务ID
}

// 忙碌检测请求参数
type idleBeatReq struct {
	JobID int64 `json:"jobId"` // 任务ID
}

// LogReq 日志请求
type LogReq struct {
	LogDateTim  int64 `json:"logDateTim"`  // 本次调度日志时间
	LogID       int64 `json:"logId"`       // 本次调度日志ID
	FromLineNum int   `json:"fromLineNum"` // 日志开始行号，滚动加载日志
}

// LogRes 日志响应
type LogRes struct {
	Code    int64         `json:"code"`    // 200 表示正常、其他失败
	Msg     string        `json:"msg"`     // 错误提示消息
	Content LogResContent `json:"content"` // 日志响应内容
}

// LogResContent 日志响应内容
type LogResContent struct {
	FromLineNum int    `json:"fromLineNum"` // 本次请求，日志开始行数
	ToLineNum   int    `json:"toLineNum"`   // 本次请求，日志结束行号
	LogContent  string `json:"logContent"`  // 本次请求日志内容
	IsEnd       bool   `json:"isEnd"`       // 日志是否全部加载完
}

// JobLogReq 日志请求
type JobLogReq struct {
	LogStartTime int64 `json:"logStartTime"` // 本次调度日志开始时间
	LogEndTime   int64 `json:"LogEndTime"`   // 本次调度日志截至时间
	JobID        int64 `json:"jobId"`        // 本次调度任务ID
}

// OneLogReq 日志请求
type OneLogReq struct {
	LogStartTime int64 `json:"logStartTime"` // 本次调度日志开始时间
	LogEndTime   int64 `json:"LogEndTime"`   // 本次调度日志截至时间
	LogID        int64 `json:"logID"`        // 本次调度任务执行日志ID
}
