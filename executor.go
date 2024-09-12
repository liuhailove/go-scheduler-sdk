package gs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/liuhailove/gretry/retry"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	//"github.com/coreos/etcd/mvcc/mvccpb"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/liuhailove/go-scheduler-sdk/logging"
	"github.com/liuhailove/go-scheduler-sdk/utils"

	"github.com/coreos/etcd/clientv3"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/process"

	"github.com/liuhailove/gpools"
	gretry "github.com/liuhailove/gretry"
)

const (
	ETCD_SERVER_KEY = "go_scheduler_root/go_scheduler_namespace/go_scheduler_group/"
	// FetchIntervalSecond 拉取时间间隔
	FetchIntervalSecond = 2 * 1000
	// SendMetricUrl 发送metric url
	SendMetricUrl = "/api/metrics"
)

var (
	latestLoad        atomic.Value
	latestCpuUsage    atomic.Value
	latestMemoryUsage atomic.Value

	CurrentPID         = os.Getpid()
	currentProcess     atomic.Value
	currentProcessOnce sync.Once
	// 最近一次sendTime
	latestSendTime uint64
	// 最大拉取间隔
	maxLastFetchIntervalMs uint64 = 15 * 1000
)

// Executor 执行器
type Executor interface {
	// Init 初始化
	Init(...Option)
	// LogHandler 日志查询
	LogHandler(handler LogHandler)
	// RegTask 注册任务
	RegTask(pattern string, task TaskFunc)
	// RunTask 运行任务
	RunTask(writer http.ResponseWriter, request *http.Request)
	// KillTask 杀死任务
	KillTask(writer http.ResponseWriter, request *http.Request)
	// TaskLog 任务日志
	TaskLog(writer http.ResponseWriter, request *http.Request)
	// Run 运行服务
	Run() error
	// Stop 停止服务
	Stop()
	// RunningTask 获取运行中的任务
	RunningTask() map[string]*Task
	// GetAddress 此方法需要在init之后调用才有效，获取node中的调度server地址
	GetAddress() string
}

// NewExecutor 创建执行器
func NewExecutor(opts ...Option) Executor {
	return newExecutor(opts...)
}

func newExecutor(opts ...Option) *executor {
	options := newOptions(opts...)
	executor := &executor{
		opts: options,
	}
	return executor
}

type executor struct {
	opts           Options
	address        string
	regList        *taskList // 注册任务列表
	runList        *taskList // 正在执行任务列表
	waitingRunList *taskList // 待执行任务列表，在单机串行时，如果当前jobId正在执行，则会进入到此队列
	retryList      *taskList // 重试列表，在上报执行结果时，如果上报失败，会进入重试列表，重试成功后删除
	mu             sync.RWMutex
	log            logging.Logger

	logHandler LogHandler   //日志查询handler
	ln         net.Listener // 监听器
	httpClient http.Client  // http client

	runTaskPool         *gpools.Pool // 运行协程池
	reportDelayTaskPool *gpools.Pool // 上报延迟执行协程池

	// etcd
	err        error
	client     *clientv3.Client
	serviceMap sync.Map // 服务列表

	// 请求合并
	queue *Queue // 请求队列

	// 执行结果检查
	checkResultFunc CheckResultFunc // 执行结果检查
	// 空闲检查
	checkIdleFunc CheckIdleFunc
	// 串行key生成策略
	serialKeyGenFunc SerialKeyPostfixGenFunc
	// timer
	registryTimer         *time.Timer
	collapseCallbackTimer *time.Timer
	retryCallbackTimer    *time.Timer
	timerSyncServiceTimer *time.Timer

	// metric log相关
	// 日志刷新时间
	flushIntervalInSec uint32
	// 单个文件最大size
	metricLogSingleFileMaxSize uint64
	// metricLogMaxFileAmount 最大保存多少个file
	metricLogMaxFileAmount uint32
	// 日志目录
	logDir string

	// metric搜索
	searcher logging.MetricSearcher

	// routerFlag 路由标签
	routerFlag string
	// executorPort 执行端口
	executorPort string

	// taskWrappers 预处理方法
	taskWrappers []TaskWrapper
}

func (e *executor) Init(opts ...Option) {
	if len(opts) != 0 {
		for _, o := range opts {
			o(&e.opts)
		}
	}
	e.log = e.opts.l
	// 日志刷新时间
	e.flushIntervalInSec = e.opts.flushIntervalInSec
	// 单个文件最大size
	e.metricLogSingleFileMaxSize = e.opts.metricLogSingleFileMaxSize
	// metricLogMaxFileAmount 最大保存多少个file
	e.metricLogMaxFileAmount = e.opts.metricLogMaxFileAmount
	// 日志目录
	e.logDir = e.opts.logDir
	// 日志刷新时间
	e.flushIntervalInSec = e.opts.flushIntervalInSec
	// 路由标签
	e.routerFlag = e.opts.routerFlag
	// 设置端口
	if e.opts.ExecutorPort != "" {
		e.executorPort = e.opts.ExecutorPort
	}
	e.regList = &taskList{
		data: sync.Map{},
	}
	e.runList = &taskList{
		data: sync.Map{},
	}
	e.retryList = &taskList{
		data: sync.Map{},
	}
	e.waitingRunList = &taskList{
		data: sync.Map{},
	}
	var err error
	if e.opts.ExecutorPort != "" && e.opts.ExecutorIp != "" {
		e.address = e.opts.ExecutorIp + ":" + e.opts.ExecutorPort
	} else if e.opts.ExecutorIp != "" {
		executorIp, _ := getIp()
		e.address = executorIp + ":" + e.opts.ExecutorPort
	} else {
		var ln net.Listener
		e.log.Info("start listen")

		if ln, err = net.Listen("tcp4", ":0"); err != nil {
			e.log.Error("net.Listen err,%s", err)
			return
		}
		e.ln = ln
		host, err := getRealHost(ln)
		if err != nil {
			e.log.Error("getRealHost err,%s", err)
			return
		}
		e.address = host
	}
	e.httpClient = http.Client{
		Timeout: e.opts.Timeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
			MaxConnsPerHost:       400,
			MaxIdleConnsPerHost:   100,
		},
	}
	if e.opts.MaxConcurrencyNum == 0 {
		e.opts.MaxConcurrencyNum = uint16(runtime.NumCPU() * 5)
	}
	e.runTaskPool, _ = gpools.NewPool(2*runtime.NumCPU(), gpools.WithMaximumQueueTasks(50000), gpools.WithMaximumPoolSize(int(e.opts.MaxConcurrencyNum)), gpools.WithPanicHandler(func(err interface{}) {
		e.log.Error(">>>>>>>>>>>callback too fast, match threadpool rejected handler(run now). error msg：%#v", err)
	}))
	e.reportDelayTaskPool, _ = gpools.NewPool(1, gpools.WithMaximumQueueTasks(1000), gpools.WithMaximumPoolSize(2), gpools.WithPanicHandler(func(err interface{}) {
		e.log.Error(">>>>>>>>>>>report delay callback too fast, match thread pool rejected handler(run now). error msg：%#v", err)
	}))
	// 设置回查
	e.checkResultFunc = e.opts.checkResultFunc
	// 设置空闲检查
	e.checkIdleFunc = e.opts.checkIdleFunc
	// 设置串行key生成方法
	e.serialKeyGenFunc = e.opts.serialKeyPostfixGenFunc
	// 设置任务预处理方法
	e.taskWrappers = e.opts.taskWrappers
	// etcd设置
	if e.opts.ServerMode == ETCD_LOOKUP_MODE || e.opts.ServerMode == MIXED_MODE {
		if len(e.opts.Endpoints) == 0 {
			e.log.Error(" ServerMode==ETCD_LOOKUP_MODE,but Endpoints is nil")
			return
		}
		// 配置etcd
		config := clientv3.Config{Endpoints: e.opts.Endpoints, DialTimeout: time.Second * 30}
		//创建一个客户端
		if e.client, err = clientv3.New(config); err != nil {
			e.log.Error("connect etcd failed,%s", err)
			return
		}
		// 启动时同步服务列表
		e.syncService()
		// 监控服务变化
		go e.watch()
		go e.timerSyncService()
	}
	if e.opts.Collapse && e.opts.TimerDelayInMilliseconds > 0 {
		e.log.Info("open collapse,TimerDelayInMilliseconds=%d", e.opts.TimerDelayInMilliseconds)
		if e.opts.TimerDelayInMilliseconds < MinTimerDelayInMilliseconds {
			e.opts.TimerDelayInMilliseconds = MinTimerDelayInMilliseconds
		} else if e.opts.TimerDelayInMilliseconds > MaxTimerDelayInMilliseconds {
			e.opts.TimerDelayInMilliseconds = MaxTimerDelayInMilliseconds
		}
		e.queue = new(Queue)
		go e.collapseCallback()
	}
	e.log.Info("In Init ")
	p, err := process.NewProcess(int32(CurrentPID))
	if err != nil {
		e.log.Error("Fail to new process when initializing system metric,pid=%s,err=%+v", CurrentPID, err)
	} else {
		currentProcessOnce.Do(func() {
			currentProcess.Store(p)
		})
	}
	// 设置metric日志
	err = logging.InitTask(e.metricLogSingleFileMaxSize, e.metricLogMaxFileAmount, e.flushIntervalInSec, e.opts.RegistryKey, e.logDir, e.log)
	if err != nil {
		e.log.Error("Fail to init task  metric")
	}
	_ = e.initialSendMetric(e.flushIntervalInSec, e.logDir, e.opts.RegistryKey, false)
	go e.registry()
	go e.retryCallback()
	go e.runningWaitingTask()

}

// LogHandler 日志handler
func (e *executor) LogHandler(handler LogHandler) {
	e.logHandler = handler
}

func (e *executor) Run() (err error) {
	if e.opts.Sync {
		return e.run()
	} else {
		go e.run()
		return nil
	}
}

// Run 启动服务
func (e *executor) run() (err error) {
	e.log.Info("In run")
	// 创建路由器
	mux := http.NewServeMux()
	// 设置路由规则
	mux.HandleFunc("/run", e.runTask)
	mux.HandleFunc("/kill", e.killTask)
	mux.HandleFunc("/log", e.taskLog)
	mux.HandleFunc("/beat", e.beat)
	mux.HandleFunc("/idleBeat", e.idleBeat)
	mux.HandleFunc("/checkResult", e.checkResult)
	// 创建服务器
	server := &http.Server{
		Addr:         e.address,
		WriteTimeout: time.Second * 30,
		Handler:      mux,
	}
	// 监听端口并提供服务
	e.log.Info("Starting server at " + e.address)
	go e.listenAndServer(server)
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	e.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	if err := server.Shutdown(ctx); err != nil {
		e.log.Error("server closed,%v", err)
	}
	defer cancel()
	return nil
}

func (e *executor) listenAndServer(server *http.Server) {
	var err error
	if e.ln != nil {
		err = server.Serve(e.ln)
	} else {
		err = server.ListenAndServe()
	}
	if err != nil {
		e.log.Error("listenAndServer error，%v", err)
		return
	}
}

func (e *executor) Stop() {
	e.log.Info("last retry call back")
	// 关闭请求合并，立刻发送
	e.opts.Collapse = false
	// 最后一次自救
	e.retryCallbackFunc()
	e.log.Info("registry remove")
	// 服务移除
	e.registryRemove()
	if e.registryTimer != nil {
		e.registryTimer.Stop()
	}
	if e.timerSyncServiceTimer != nil {
		e.timerSyncServiceTimer.Stop()
	}
	if e.collapseCallbackTimer != nil {
		e.collapseCallbackTimer.Stop()
	}
	if e.retryCallbackTimer != nil {
		e.retryCallbackTimer.Stop()
	}
	if e.runList != nil {
		e.log.Info("running task  cancel")
		e.runList.Range(func(data interface{}) bool {
			var runTask = data.(*Task)
			runTask.Cancel()
			return true
		})
	}
	if e.ln != nil {
		_ = e.ln.Close()
	}
	e.log.Info("timer close")
}

// RegTask 注册任务
func (e *executor) RegTask(pattern string, task TaskFunc) {
	var t = &Task{}
	t.fn = task
	e.regList.Set(pattern, t)
	return
}

// 运行一个任务
func (e *executor) runTask(writer http.ResponseWriter, request *http.Request) {
	req, _ := ioutil.ReadAll(request.Body)
	param := &RunReq{}
	err := json.Unmarshal(req, &param)
	if err != nil {
		_, _ = writer.Write(returnCall(param, 500, []string{"params err"}))
		e.log.Error("param unmarshall error:" + string(req))
		return
	}
	e.log.Info("task log param:%s", fmt.Sprintf("%#v", param))
	param.GlueType = strings.ToUpper(param.GlueType)
	if param.GlueType == "BEAN" && !e.regList.Exists(param.ExecutorHandler) {
		_, _ = writer.Write(returnError("Task not registered"))
		e.log.Error("task [" + Int64ToStr(param.JobID) + "] not registered:" + param.ExecutorHandler)
		return
	}

	//阻塞策略处理,key,非并发模式: (jobId:jobId)；并发模式:(jobId:logId)
	if param.ExecutorBlockStrategy == coverEarly { //覆盖之前调度
		oldTask := e.runList.Get(Int64ToStr(param.JobID) + ":" + Int64ToStr(param.JobID))
		if oldTask != nil {
			e.log.Info("ExecutorBlockStrategy is coverEarly,task %v is cancel", oldTask)
			oldTask.Cancel()
			e.runList.DelPre(Int64ToStr(oldTask.Id))
		}
	} else if param.ExecutorBlockStrategy == discardLater { //丢弃后续调度 都进行阻塞
		if e.runList.Exists(Int64ToStr(param.JobID) + ":" + Int64ToStr(param.JobID)) {
			_, _ = writer.Write(returnError("There are tasks running"))
			e.log.Error("task [" + Int64ToStr(param.JobID) + "] has running:" + param.ExecutorHandler)
			return
		}
	} else if param.ExecutorBlockStrategy == serialExecution {
		if e.runList.Exists(e.serialKeyGen(param)) &&
			e.waitingRunList.Exists(Int64ToStr(param.JobID)+":"+Int64ToStr(param.LogID)) { // 单机串行,判断jobID:logID是否重复，如果重复，则返回已经运行中，导致这种重复的原因往往是网络重传导致的
			_, _ = writer.Write(returnError("There are tasks running"))
			e.log.Error("tasks[" + Int64ToStr(param.JobID) + "] are running:" + param.ExecutorHandler)
			return
		}
	} else if param.ExecutorBlockStrategy == concurrentExecution {
		// 并行执行
		if e.runList.Exists(Int64ToStr(param.JobID) + ":" + Int64ToStr(param.LogID)) {
			_, _ = writer.Write(returnError("There are tasks running"))
			e.log.Error("task [" + Int64ToStr(param.JobID) + "] has running:" + param.ExecutorHandler + ", maybe network retry")
			return
		}
	} else {
		_, _ = writer.Write(returnError("Wrong block strategy"))
		e.log.Error("task [" + Int64ToStr(param.JobID) + "] executor block strategy not exist:" + param.ExecutorHandler + ", strategy:" + param.ExecutorBlockStrategy)
		return
	}
	cxt := context.Background()
	regTask := e.regList.Get(param.ExecutorHandler)
	task := &Task{}
	if regTask != nil {
		task.fn = regTask.fn
	}
	if param.ExecutorTimeout > 0 {
		task.Ext, task.Cancel = context.WithTimeout(cxt, time.Duration(param.ExecutorTimeout)*time.Second)
	} else {
		task.Ext, task.Cancel = context.WithCancel(cxt)
	}
	switch param.GlueType {
	case Bean.GetDesc():
		task.GlueType = Bean
	case GlueShell.GetDesc():
		task.GlueType = GlueShell
	case GluePython.GetDesc():
		task.GlueType = GluePython
	default:
		_, _ = writer.Write(returnError("Wrong glue type"))
		e.log.Error("task [" + Int64ToStr(param.JobID) + "]glue type invalid:" + ",glue type:" + param.GlueType)
		return
	}
	task.Id = param.JobID
	task.Name = param.ExecutorHandler
	task.Param = param
	task.log = e.log
	task.LogId = param.LogID
	task.ExecutorBlockStrategy = param.ExecutorBlockStrategy
	var immediatelyRun = false
	if param.ExecutorBlockStrategy == concurrentExecution {
		e.runList.Set(Int64ToStr(task.Id)+":"+Int64ToStr(task.LogId), task)
		immediatelyRun = true
	} else if param.ExecutorBlockStrategy == serialExecution {
		if e.runList.Exists(e.serialKeyGen(param)) {
			e.waitingRunList.Set(Int64ToStr(task.Id)+":"+Int64ToStr(task.LogId), task)
			immediatelyRun = false
		} else {
			e.runList.Set(e.serialKeyGen(param), task)
			immediatelyRun = true
		}
	} else {
		e.runList.Set(Int64ToStr(task.Id)+":"+Int64ToStr(task.Id), task)
		immediatelyRun = true
	}
	if immediatelyRun {
		err = e.runTaskPool.Submit(func() {
			task.Run(func(code int64, msg []string) {
				e.doCallback(task, code, msg)
			}, e)
		})
		if err != nil {
			e.log.Error("e.runTaskPool.Submit error,%#v", err)
		}
		e.log.Info("task [" + Int64ToStr(param.JobID) + "] log [" + Int64ToStr(param.LogID) + "] begin running:" + param.ExecutorHandler)
	} else {
		e.log.Info("task [" + Int64ToStr(param.JobID) + "] log [" + Int64ToStr(param.LogID) + "]in waiting queue，waiting runn:" + param.ExecutorHandler)
	}
	_, _ = writer.Write(returnGeneral())
}

// 删除一个任务
func (e *executor) killTask(writer http.ResponseWriter, request *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	req, _ := ioutil.ReadAll(request.Body)
	param := &killReq{}
	_ = json.Unmarshal(req, &param)
	if !e.runList.ExistPre(Int64ToStr(param.JobID)) {
		_, _ = writer.Write(returnKill(param, 500))
		e.log.Error("task [" + Int64ToStr(param.JobID) + "] not running")
		return
	}
	tasks := e.runList.GetPre(Int64ToStr(param.JobID))
	for _, task := range tasks {
		task.Cancel()
		e.log.Info("task %v by killTask is cancel", task)
	}
	e.runList.DelPre(Int64ToStr(param.JobID))
	// 待执行任务也需要删除
	e.waitingRunList.DelPre(Int64ToStr(param.JobID))
	_, _ = writer.Write(returnGeneral())
}

// 任务日志
func (e *executor) taskLog(writer http.ResponseWriter, request *http.Request) {
	var res *LogRes
	data, err := ioutil.ReadAll(request.Body)
	req := &LogReq{}
	if err != nil {
		e.log.Error("task log request failed:" + err.Error())
		reqErrLogHandler(writer, req, err)
		return
	}
	err = json.Unmarshal(data, &req)
	if err != nil {
		e.log.Error("task log unmarshall failed:" + err.Error())
		reqErrLogHandler(writer, req, err)
		return
	}
	e.log.Info("task request param :%+v", req)
	if e.logHandler != nil {
		res = e.logHandler(req)
	} else {
		res = defaultLogHandler(req)
	}
	str, _ := json.Marshal(res)
	_, _ = writer.Write(str)
}

// 心跳检测
func (e *executor) beat(writer http.ResponseWriter, request *http.Request) {
	e.log.Info("heart beat")
	_, _ = writer.Write(returnGeneral())
}

// reportDelay 上报延迟执行消息
func (e *executor) reportDelay(task *Task, startExecutorTime int64) error {
	response, serverAddr, err := e.post("/api/reportDelay", string(returnReportDelay(task.Id, task.LogId, task.Param.LogDateTime, startExecutorTime, e.address)))
	if err != nil {
		e.log.Info("report delay error,res=%s,serverAddr=%s,err=%v", response, serverAddr, err)
		return err
	}
	defer response.Body.Close()
	return nil
}

// 忙碌检测
func (e *executor) idleBeat(writer http.ResponseWriter, request *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	req, _ := ioutil.ReadAll(request.Body)
	param := &idleBeatReq{}
	err := json.Unmarshal(req, &param)
	if err != nil {
		_, _ = writer.Write(returnIdleBeat(500))
		e.log.Error("param unmarshall error :" + string(req))
		return
	}
	if e.runList.ExistPre(Int64ToStr(param.JobID)) {
		_, _ = writer.Write(returnIdleBeat(500))
		e.log.Error("idleBeat task [" + Int64ToStr(param.JobID) + "] is running")
		return
	}
	if e.checkIdleFunc != nil && !e.checkIdleFunc(context.Background(), param.JobID) {
		_, _ = writer.Write(returnIdleBeat(500))
		e.log.Error("idleBeat CheckIdleFunc task [" + Int64ToStr(param.JobID) + "] is running")
		return
	}
	e.log.Info("idle beat param :%v", param)
	_, _ = writer.Write(returnGeneral())
}

// 注册执行器到调度中心
func (e *executor) registry() {
	e.log.Info("In registry ")
	e.registryTimer = time.NewTimer(time.Second * 0) //初始立即执行
	req := &Registry{
		RegistryGroup: "EXECUTOR",
		RegistryKey:   e.opts.RegistryKey,
		RegistryValue: "http://" + e.address,
		LoadStat:      e.retrieveLoadStat(),
		CpuStat:       e.retrieveProcessCpuStat(),
		MemoryStat:    e.retrieveProcessMemoryStat(),
		RouterFlag:    e.routerFlag,
	}
	param, err := json.Marshal(req)
	if err != nil {
		log.Fatal("executor registry marshall error:" + err.Error())
	}
	for {
		<-e.registryTimer.C
		e.registryTimer.Reset(time.Second * time.Duration(10)) //10秒心跳防止过期
		func() {
			req.LoadStat = e.retrieveLoadStat()
			req.CpuStat = e.retrieveProcessCpuStat()
			req.MemoryStat = e.retrieveProcessMemoryStat()
			param, _ = json.Marshal(req)
			result, _, err := e.post("/api/registry", string(param))
			if err != nil {
				e.log.Error("executor registry failed 1:" + err.Error())
				return
			}
			defer result.Body.Close()
			body, err := ioutil.ReadAll(result.Body)
			if err != nil {
				e.log.Error("executor registry failed 2:" + err.Error())
				return
			}
			res := &res{}
			_ = json.Unmarshal(body, &res)
			if res.Code != 200 {
				e.log.Error("executor registry failed 3:" + string(body))
				return
			}
			e.log.Debug("executor registry success")
		}()
	}
}

// 执行器注册摘除
func (e *executor) registryRemove() {
	req := &Registry{
		RegistryGroup: "EXECUTOR",
		RegistryKey:   e.opts.RegistryKey,
		RegistryValue: "http://" + e.address,
	}
	param, err := json.Marshal(req)
	if err != nil {
		e.log.Error("executor registry remove failed 1 :" + err.Error())
		return
	}
	res, _, err := e.post("/api/registryRemove", string(param))
	if err != nil {
		e.log.Error("executor registry remove failed 2" + err.Error())
		return
	}
	body, err := ioutil.ReadAll(res.Body)
	e.log.Info("executor registry remove success :" + string(body))
	defer res.Body.Close()
	// etcd关闭
	if e.client != nil {
		_ = e.client.Close()
	}
}

func (e *executor) doCallback(task *Task, code int64, msg []string) {
	// 回调server
	if e.opts.Collapse {
		e.log.Info("collapse callback task [" + Int64ToStr(task.Id) + "] log [" + Int64ToStr(task.LogId) + "] :" + task.Param.String())
		e.queue.Push(callbackParams{
			task: task,
			code: code,
			msg:  msg,
		})
	} else {
		e.callback(task, code, msg)
	}
}

// 请求合并
func (e *executor) collapseCallback() {
	e.collapseCallbackTimer = time.NewTimer(time.Millisecond * 0) // 初始立即执行
	defer e.collapseCallbackTimer.Stop()
	for {
		<-e.collapseCallbackTimer.C
		e.collapseCallbackTimer.Reset(time.Millisecond * time.Duration(e.opts.TimerDelayInMilliseconds)) // TimerDelayInMilliseconds时间后合并发送
		func() {
			items := e.queue.PopFrontAll()
			if len(items) > 0 {
				var params []callbackParams
				for _, item := range items {
					params = append(params, item.(callbackParams))
				}
				e.batchCallback(params)
			}
		}()
	}
}

// 回调任务列表
func (e *executor) batchCallback(params []callbackParams) {
	res, serverAddr, err := e.post("/api/callback", string(returnBatchCall(params)))
	if err != nil {
		e.log.Error("callback err : ", err.Error())
		for _, param := range params {
			param.task.msg = param.msg
			param.task.code = param.code
			if param.task.ExecutorBlockStrategy == concurrentExecution {
				e.retryList.Set(Int64ToStr(param.task.Id)+":"+Int64ToStr(param.task.LogId), param.task)
			} else if param.task.ExecutorBlockStrategy == serialExecution {
				var serialKey = e.serialKeyGen(param.task.Param)
				e.retryList.Set(serialKey, param.task)
			} else {
				e.retryList.Set(Int64ToStr(param.task.Id)+":"+Int64ToStr(param.task.Id), param.task)
			}
		}
		return
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		e.log.Error("callback ReadAll err : ", err.Error())
		for _, param := range params {
			param.task.msg = param.msg
			param.task.code = param.code
			if param.task.ExecutorBlockStrategy == concurrentExecution {
				e.retryList.Set(Int64ToStr(param.task.Id)+":"+Int64ToStr(param.task.LogId), param.task)
			} else if param.task.ExecutorBlockStrategy == serialExecution {
				var serialKey = e.serialKeyGen(param.task.Param)
				e.retryList.Set(serialKey, param.task)
			} else {
				e.retryList.Set(Int64ToStr(param.task.Id)+":"+Int64ToStr(param.task.Id), param.task)
			}
		}
		return
	}
	var key string
	for _, param := range params {
		if param.task.ExecutorBlockStrategy == concurrentExecution {
			key = Int64ToStr(param.task.Id) + ":" + Int64ToStr(param.task.LogId)
		} else if param.task.ExecutorBlockStrategy == serialExecution {
			key = e.serialKeyGen(param.task.Param)
		} else {
			key = Int64ToStr(param.task.Id) + ":" + Int64ToStr(param.task.Id)
		}
		e.runList.Del(key)
		e.retryList.ExistAndDel(key)
	}
	e.log.Info("task call back success:serverAddr=" + serverAddr + ";" + string(body))
}

// 回调任务列表
func (e *executor) callback(task *Task, code int64, msg []string) {
	// 设置执行结果，目的是为了重试
	e.log.Info("callback task [" + Int64ToStr(task.Id) + "] log [" + Int64ToStr(task.LogId) + "]:" + task.Param.String())
	task.code = code
	task.msg = msg
	res, serverAddr, err := e.post("/api/callback", string(returnCall(task.Param, code, msg)))
	if err != nil {
		e.log.Error("callback err : ", err.Error())
		if task.ExecutorBlockStrategy == concurrentExecution {
			e.retryList.Set(Int64ToStr(task.Id)+":"+Int64ToStr(task.LogId), task)
		} else if task.ExecutorBlockStrategy == serialExecution {
			e.retryList.Set(e.serialKeyGen(task.Param), task)
		} else {
			e.retryList.Set(Int64ToStr(task.Id)+":"+Int64ToStr(task.Id), task)
		}
		return
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		e.log.Error("callback ReadAll err : ", err.Error())
		if task.ExecutorBlockStrategy == concurrentExecution {
			e.retryList.Set(Int64ToStr(task.Id)+":"+Int64ToStr(task.LogId), task)
		} else if task.ExecutorBlockStrategy == serialExecution {
			var serialKey = e.serialKeyGen(task.Param)
			e.retryList.Set(serialKey, task)
		} else {
			e.retryList.Set(Int64ToStr(task.Id)+":"+Int64ToStr(task.Id), task)
		}
		return
	}
	var key string
	if task.ExecutorBlockStrategy == concurrentExecution {
		key = Int64ToStr(task.Id) + ":" + Int64ToStr(task.LogId)
	} else if task.ExecutorBlockStrategy == serialExecution {
		key = e.serialKeyGen(task.Param)
	} else {
		key = Int64ToStr(task.Id) + ":" + Int64ToStr(task.Id)
	}
	e.retryList.ExistAndDel(key)
	e.runList.Del(key)
	e.log.Info("task call back success, [" + Int64ToStr(task.Id) + "] log [" + Int64ToStr(task.LogId) + "] serverAddr=" + serverAddr + "," + task.Param.String() + ";" + string(body))
}

// 重试回调任务列表
func (e *executor) retryCallback() {
	e.retryCallbackTimer = time.NewTimer(time.Second * 0) // 初始立即执行
	defer e.retryCallbackTimer.Stop()
	for {
		<-e.retryCallbackTimer.C
		e.retryCallbackTimer.Reset(time.Second * time.Duration(5)) // 5 秒重试一次
		e.retryCallbackFunc()
	}
}
func (e *executor) retryCallbackFunc() {
	e.retryList.Range(
		func(data interface{}) bool {
			var task = data.(*Task)
			e.log.Info("retryCallback,logId=" + strconv.FormatInt(task.LogId, 10))
			e.doCallback(task, task.code, task.msg)
			return true
		})
}

// 待执行队列执行
func (e *executor) runningWaitingTask() {
	for {
		var needSleep = e.loopSchedule()
		if needSleep {
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (e *executor) loopSchedule() (needSleep bool) {
	defer func() {
		if err := recover(); err != nil {
			e.log.Error(string(debug.Stack()))
			needSleep = true
		}
	}()
	if e.waitingRunList.Empty() {
		needSleep = true
		return
	}
	e.waitingRunList.Range(func(data interface{}) bool {
		task := data.(*Task)
		// 如果运行队列中仍存在，说明任务还没有执行完成，继续等待重试
		if e.runList.NoExistAndSet(e.serialKeyGen(task.Param), task) {
			e.log.Info("waiting task running,logId=" + strconv.FormatInt(task.LogId, 10))
			err := e.runTaskPool.Submit(func() {
				task.Run(func(code int64, msg []string) {
					e.doCallback(task, code, msg)
				}, e)
			})
			if err != nil {
				e.log.Error("e.runTaskPool.Submit error,%#v", err)
			} else {
				e.waitingRunList.Del(Int64ToStr(task.Id) + ":" + Int64ToStr(task.LogId))
			}
		}
		return true
	})
	needSleep = false
	return
}

// post
func (e *executor) post(action, body string) (resp *http.Response, servAddr string, err error) {
	var request *http.Request
	if e.opts.ServerMode == FIX_SERVER_MODE || e.opts.ServerMode == HISTORY_SERVER_MODE {
		servAddr = e.opts.ServerAddr
		request, err = http.NewRequest("POST", servAddr+action, strings.NewReader(body))
	} else if e.opts.ServerMode == ETCD_LOOKUP_MODE {
		addrArr := e.loadServerAddr()
		if len(addrArr) == 0 {
			// 手动同步一下
			e.syncService()
			addrArr = e.loadServerAddr()
			if len(addrArr) == 0 {
				return nil, "", errors.New(" not exist any available server addr")
			}
		}
		servAddr = GetServiceAddr(addrArr, e.opts.LoadBalanceMode)
		request, err = http.NewRequest("POST", servAddr+action, strings.NewReader(body))
	} else if e.opts.ServerMode == MIXED_MODE {
		addrArr := e.loadServerAddr()
		if len(addrArr) == 0 {
			// 手动同步一下
			e.syncService()
			addrArr = e.loadServerAddr()
			if len(addrArr) == 0 {
				addrArr = []string{e.opts.ServerAddr}
			}
		}
		servAddr = GetServiceAddr(addrArr, e.opts.LoadBalanceMode)
		request, err = http.NewRequest("POST", servAddr+action, strings.NewReader(body))
	}
	if err != nil {
		e.log.Error("post error: servAddr=" + servAddr + ",error=" + err.Error())
		return nil, "", err
	}
	request.Close = true
	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
	request.Header.Set("XXL-JOB-ACCESS-TOKEN", e.opts.AccessToken)
	resp, err = e.httpClient.Do(request)
	return resp, servAddr, err
}

// RunTask 运行任务
func (e *executor) RunTask(writer http.ResponseWriter, request *http.Request) {
	e.runTask(writer, request)
}

// KillTask 删除任务
func (e *executor) KillTask(writer http.ResponseWriter, request *http.Request) {
	e.killTask(writer, request)
}

// TaskLog 任务日志
func (e *executor) TaskLog(writer http.ResponseWriter, request *http.Request) {
	e.taskLog(writer, request)
}

// CheckResult 执行结果检查
func (e *executor) checkResult(writer http.ResponseWriter, request *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			e.log.Error(string(debug.Stack()))
			_, _ = writer.Write(returnError("not exist"))
		}
	}()
	e.mu.Lock()
	defer e.mu.Unlock()
	req, _ := ioutil.ReadAll(request.Body)
	param := &RunReq{}
	err := json.Unmarshal(req, &param)
	if err != nil {
		_, _ = writer.Write(returnCall(param, 500, []string{"params err"}))
		e.log.Error("check result params error:" + string(req))
		return
	}
	e.log.Info("check result param:%v", param)
	exist := e.checkResultFunc(context.Background(), param)
	if exist {
		_, _ = writer.Write(returnGeneral())
	} else {
		_, _ = writer.Write(returnError("not exist"))
	}
}

// 重试回调任务列表
func (e *executor) timerSyncService() {
	e.timerSyncServiceTimer = time.NewTimer(time.Second * 300) // 5分钟拉一次
	defer e.timerSyncServiceTimer.Stop()
	for {
		<-e.timerSyncServiceTimer.C
		e.timerSyncServiceTimer.Reset(time.Second * time.Duration(300)) // 5分钟拉一次
		func() {
			e.syncService()
		}()
	}
}

// syncService 同步一下服务地址
func (e *executor) syncService() {
	defer func() {
		if err := recover(); err != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			e.log.Error("worker exits from panic: %s\n", string(buf[:n]))
		}
	}()
	if e.client != nil {
		//用于读写etcd的键值对
		kv := clientv3.NewKV(e.client)
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*30)
		defer cancel()
		getResp, err := kv.Get(ctx, ETCD_SERVER_KEY, clientv3.WithPrefix())
		if err != nil {
			e.log.Error("get kv failed,err=" + err.Error())
			return
		}
		// 此处不能直接清空，因为会导致正在使用的方法获取不到地址，因此需要找到要remove的地址，逐个清楚
		var newKeys []string
		for _, keyValue := range getResp.Kvs {
			if keyValue.Lease <= 0 {
				_, err = kv.Delete(context.TODO(), string(keyValue.Key))
				if err != nil {
					data, _ := json.Marshal(keyValue)
					e.log.Error("kv.Delete error,keyValue=" + string(data))
				}
				continue
			}
			e.serviceMap.Store(string(keyValue.Key), string(keyValue.Value))
			newKeys = append(newKeys, string(keyValue.Key))
		}
		var removeKeys []string
		e.serviceMap.Range(func(key, value interface{}) bool {
			if !Contains(newKeys, key.(string)) {
				removeKeys = append(removeKeys, key.(string))
			}
			return true
		})
		for _, removeKey := range removeKeys {
			e.serviceMap.Delete(removeKey)
		}
	}
}

// loadServerAddr 加载服务地址
func (e *executor) loadServerAddr() []string {
	addrs := make([]string, 0)
	e.serviceMap.Range(func(key, value interface{}) bool {
		addrs = append(addrs, value.(string))
		return true
	})
	return addrs
}

// watch etcd变更监控
func (e *executor) watch() {
	if e.client != nil && len(e.opts.Endpoints) > 0 {
		var watchRespChan <-chan clientv3.WatchResponse
		var watchResp clientv3.WatchResponse
		//var event *clientv3.Event
		// 创建一个watcher
		watcher := clientv3.NewWatcher(e.client)

		watchRespChan = watcher.Watch(context.TODO(), ETCD_SERVER_KEY, clientv3.WithPrefix())
		// 处理kv变化事件
		for watchResp = range watchRespChan {
			// 由于兼容问题，以下代码注释
			for _, event := range watchResp.Events {
				switch event.Type {
				default:
					e.log.Info("event.Type" + event.Type.String() + ",key:" + string(event.Kv.Key) + " change to " + string(event.Kv.Value) + ", Revision:" + Int64ToStr(event.Kv.ModRevision))
					e.syncService()
				}
			}
		}
	}
}

// RunningTask 获取运行中的任务
func (e *executor) RunningTask() map[string]*Task {
	return e.runList.GetAll()
}

// serialKeyGen 串行策略key生成方法
func (e *executor) serialKeyGen(param *RunReq) string {
	if e.serialKeyGenFunc != nil {
		return Int64ToStr(param.JobID) + ":" + e.serialKeyGenFunc(param)
	}
	return Int64ToStr(param.JobID) + ":" + Int64ToStr(param.JobID)
}

// 获取应用host
func getRealHost(ln net.Listener) (host string, err error) {
	adds, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	var localIPV4 string
	var nonLocalIPV4 string
	for _, addr := range adds {
		if ipNet, ok := addr.(*net.IPNet); ok && ipNet.IP.To4() != nil {
			if ipNet.IP.IsLoopback() {
				localIPV4 = ipNet.IP.String()
			} else {
				nonLocalIPV4 = ipNet.IP.String()
				break
			}
		}
	}
	if nonLocalIPV4 != "" {
		host = fmt.Sprintf("%s:%d", nonLocalIPV4, ln.Addr().(*net.TCPAddr).Port)
	} else {
		host = fmt.Sprintf("%s:%d", localIPV4, ln.Addr().(*net.TCPAddr).Port)
	}

	return
}

// getIp 获取服务的地址
func getIp() (ip string, err error) {
	adds, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	var localIPV4 string
	var nonLocalIPV4 string
	for _, addr := range adds {
		if ipNet, ok := addr.(*net.IPNet); ok && ipNet.IP.To4() != nil {
			if ipNet.IP.IsLoopback() {
				localIPV4 = ipNet.IP.String()
			} else {
				nonLocalIPV4 = ipNet.IP.String()
				break
			}
		}
	}
	if nonLocalIPV4 != "" {
		ip = nonLocalIPV4
	} else {
		ip = localIPV4
	}
	return
}

func (e *executor) retrieveLoadStat() float64 {
	loadStat, err := load.Avg()
	if err != nil {
		e.log.Error("[retrieveLoadStat] Failed to retrieve current system load,err=" + err.Error())
		if latestLoad.Load() != nil {
			return latestLoad.Load().(float64)
		}
		return 0
	}
	// 记录最近内存使用率
	latestLoad.Store(loadStat.Load1)
	return loadStat.Load1
}

// retrieveProcessCpuStat gets current process's cpu usage in Bytes
func (e *executor) retrieveProcessCpuStat() float64 {
	curProcess := currentProcess.Load()
	if curProcess == nil {
		p, err := process.NewProcess(int32(CurrentPID))
		if err != nil {
			e.log.Error("[retrieveProcessCpuStat] Failed to retrieve current cpu stat,err=" + err.Error())
			return 0
		}
		currentProcessOnce.Do(func() {
			currentProcess.Store(p)
		})
		curProcess = currentProcess.Load()
	}
	p := curProcess.(*process.Process)
	percent, err := p.Percent(0)
	if err != nil {
		e.log.Error("[retrieveProcessCpuStat] Failed to retrieve current cpu stat,err=" + err.Error())
		if latestCpuUsage.Load() != nil {
			return latestCpuUsage.Load().(float64)
		}
		return 0
	}
	// 记录最近内存使用率
	latestCpuUsage.Store(percent)
	return percent
}

// retrieveProcessMemoryStat gets current process's memory usage in Bytes
func (e *executor) retrieveProcessMemoryStat() int64 {
	curProcess := currentProcess.Load()
	if curProcess == nil {
		p, err := process.NewProcess(int32(CurrentPID))
		if err != nil {
			e.log.Error("[retrieveProcessMemoryStat] Failed to retrieve current memory stat,err=" + err.Error())
			return 0
		}
		currentProcessOnce.Do(func() {
			currentProcess.Store(p)
		})
		curProcess = currentProcess.Load()
	}
	p := curProcess.(*process.Process)
	memInfo, err := p.MemoryInfo()
	var rss int64
	if memInfo != nil {
		rss = int64(memInfo.RSS)
	}
	if err != nil {
		e.log.Error("[retrieveProcessMemoryStat] Failed to retrieve current memory stat,err=" + err.Error())
		if latestMemoryUsage.Load() != nil {
			return latestMemoryUsage.Load().(int64)
		}
		return 0
	}
	// 记录最近内存使用率
	latestMemoryUsage.Store(rss)
	return rss
}

func (e *executor) initialSendMetric(sendIntervalInSec uint32, logBaseDir, appName string, logUsePid bool) error {
	if e.searcher == nil {
		var err error
		e.searcher, err = logging.NewDefaultMetricSearcher(logBaseDir, logging.FormMetricFileName(appName, logUsePid))
		if err != nil {
			return errors.New("Error when retrieving metrics" + err.Error())
		}
	}
	//延迟5s执行，等待配置文件的初始化
	var metricTimer = time.NewTimer(time.Second * 5)
	go func() {
		for {
			<-metricTimer.C
			metricTimer.Reset(time.Second * time.Duration(sendIntervalInSec)) //interval秒心跳防止过期
			e.sendMetrics()
		}
	}()
	return nil
}

func (e *executor) sendMetrics() {
	_, err := e.sendMetric()
	if err != nil {
		e.log.Info("[SendMetricInitFunc] WARN: SendMetric error,err" + err.Error())
	}
}

// sendMetric 向server发送metric信息
func (e *executor) sendMetric() (bool, error) {
	var now = utils.CurrentTimeMillis()
	// trim milliseconds
	var lastFetchMs = now - maxLastFetchIntervalMs
	// 和发送的下一秒对比
	if lastFetchMs < latestSendTime+1000*1 {
		lastFetchMs = latestSendTime + 1000*1
	}
	lastFetchMs = lastFetchMs / 1000 * 1000
	var endTime = lastFetchMs + FetchIntervalSecond
	// 不希望跟踪的太快，因为要等文件写
	if endTime > now-4000*1 {
		// too near
		return false, nil
	}
	latestSendTime = endTime
	// 和当前版本对比，如果不一致，需要拉取规则变更
	metricItemArr, err := e.searcher.FindByTimeAndLog(lastFetchMs, endTime, -1)
	if err != nil {
		return false, errors.New("Error when retrieving metrics" + err.Error())
	}
	if metricItemArr == nil || len(metricItemArr) == 0 {
		return true, nil
	}
	builder := strings.Builder{}
	for _, item := range metricItemArr {
		thinStr, _ := item.ToThinString()
		builder.WriteString(thinStr)
		builder.WriteString("\n")
	}
	go func() {
		var retryTemplate = gretry.NewRetryTemplateBuilder().
			MaxAttemptsRtyPolicy(10).
			UniformRandomBackoff(100, 1000).
			Build()
		var _, err = retryTemplate.Execute(&SendMetricRetryCallback{
			executor: e,
			body:     builder.String(),
		})
		if err != nil {
			e.log.Info("[sendMetric] Error: SendMetric error, err" + err.Error())
		}
	}()
	return true, nil
}

type SendMetricRetryCallback struct {
	executor *executor
	body     string
}

func (m SendMetricRetryCallback) DoWithRetry(ctx retry.RtyContext) interface{} {
	_, _, err := m.executor.post(SendMetricUrl, m.body)
	if err != nil {
		m.executor.log.Info("[sendMetric] Error: SendMetric error, err" + err.Error() + ",ctx:" + strconv.Itoa(int(ctx.GetRetryCount())))
	}
	if err != nil {
		panic(err.Error())
	}
	return nil
}

// GetAddress 此方法需要在init之后调用才有效
func (e *executor) GetAddress() string {
	return e.address
}
