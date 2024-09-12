package gs

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/liuhailove/go-scheduler-sdk/logging"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	currPath, _      = os.Getwd()
	scriptSourcePath = path.Join(currPath, "script_source")
)

// TaskFunc 任务执行函数
type TaskFunc func(ctx context.Context, param *RunReq) ([]string, error)

// TaskWrapper 包装 TaskWrapper 并返回等效项,任务执行前处理，一般可以处理通用的逻辑，如context设置、打印参数、限流设置等
type TaskWrapper func(TaskFunc) TaskFunc

// Task 任务
type Task struct {
	Id                    int64
	Name                  string
	Ext                   context.Context
	Param                 *RunReq
	fn                    TaskFunc
	Cancel                context.CancelFunc
	StartTime             int64
	EndTime               int64
	LogId                 int64    // 任务LogId
	ExecutorBlockStrategy string   //阻塞策略
	GlueType              GlueType //任务类型
	//日志
	log logging.Logger
	// 执行结果
	code int64
	msg  []string
}

// Run 运行任务
func (t *Task) Run(callback func(code int64, msg []string), e *executor) {
	defer func(cancel func()) {
		if err := recover(); err != nil {
			t.log.Info(t.Info()+" panic: %v", err)
			debug.PrintStack() //堆栈跟踪
			t.log.Error(string(debug.Stack()))
			callback(500, []string{"task panic:" + fmt.Sprintf("%v", err)})
		}
		cancel()
	}(t.Cancel)
	var msg []string
	var err error
	// 计算业务开始执行时间
	if t.Param.BusinessStartExecutorToleranceThresholdInMin > 0 {
		var currentTimeMs = time.Now().UnixNano() / 1e6
		var timeElapseInMs = currentTimeMs - t.Param.LogDateTime
		if timeElapseInMs > int64(t.Param.BusinessStartExecutorToleranceThresholdInMin*60*1000) {
			err = e.reportDelayTaskPool.Submit(func() {
				_ = e.reportDelay(t, currentTimeMs)
			})
			if err != nil {
				e.log.Error("e.runTaskPool.Submit in reportDelayTaskPool error,%#v", err)
			}
		}
	}
	// 对处理方法进行包裹
	for i := len(e.taskWrappers); i > 0; i-- {
		t.fn = e.taskWrappers[i-1](t.fn)
	}
	// 调用业务
	if t.GlueType.GetScriptFlag() {
		msg, err = t.runScriptTask(t.Ext, t.Param)
	} else {
		msg, err = t.fn(t.Ext, t.Param)
	}

	if err == nil {
		callback(200, msg)
	} else {
		t.log.Info(t.Info()+" err: %v", err)
		debug.PrintStack() //堆栈跟踪
		t.log.Error(string(debug.Stack()))
		callback(500, []string{"task error:" + fmt.Sprintf("%v", err)})
	}
	return
}

// Info 任务信息
func (t *Task) Info() string {
	return "任务ID[" + Int64ToStr(t.Id) + "]任务名称[" + t.Name + "]参数：" + t.Param.ExecutorParams
}
func (t *Task) runScriptTask(ctx context.Context, param *RunReq) (msg []string, err error) {
	//job key 非并发模式: (jobId_jobId)；并发模式:(jobId_logId)
	//脚本文件 文件名 jobKey_glueUpdatetime_suffix 如:1_1_1637228411.sh
	var jobKey string
	if param.ExecutorBlockStrategy == concurrentExecution {
		jobKey = Int64ToStr(param.JobID) + "_" + Int64ToStr(param.LogID)
	} else {
		jobKey = Int64ToStr(param.JobID) + "_" + Int64ToStr(param.JobID)
	}

	//1.清除旧的脚本文件
	err = t.cleanOldScriptFile(jobKey)
	if err != nil {
		t.log.Error(t.Info()+" err: 清除旧的脚本文件失败:%v", err)
		return
	}
	//2.创建新的脚本文件
	scriptFilePath, err := t.buildNewScriptFile(param, jobKey, t.GlueType.suffix)
	if err != nil {
		t.log.Error(t.Info()+" err: 创建新的脚本文件失败:%v", err)
		return
	}
	//3.命令行调用脚本文件
	err = t.execScriptFile(ctx, param, scriptFilePath, t.GlueType)
	if err != nil {
		t.log.Error(t.Info()+" err: 执行脚本文件失败:%v", err)
		return
	}
	//4.返回
	return
}

func (t *Task) cleanOldScriptFile(jobKey string) (err error) {
	//判断目录是否存在
	if FileIsExist(scriptSourcePath) {
		//遍历目录下所有文件
		//删除该目录下相应的旧脚本文件
		files, err := ioutil.ReadDir(scriptSourcePath)
		if err != nil {
			t.log.Error(" err: 读取文件夹失败:%v", err)
			return err
		}
		for _, file := range files {
			if strings.HasPrefix(file.Name(), jobKey) {
				err = os.Remove(path.Join(scriptSourcePath, file.Name()))
				if err != nil {
					t.log.Error(" err:删除文件%s失败:%v", file.Name(), err)
					return err
				}
			}
		}
	}
	return
}

func (t *Task) buildNewScriptFile(param *RunReq, jobKey string, suffix string) (scriptFilePath string, err error) {
	//判断目录是否存在，若不存在则创建
	if !FileIsExist(scriptSourcePath) {
		err = os.MkdirAll(scriptSourcePath, 0777)
		if err != nil {
			t.log.Error(" err:创建目录%s失败:%v", scriptSourcePath, err)
			return
		}
	}
	//创建新的脚本文件 文件名 jobKey_glueUpdatetime_suffix 如:1_1_1637228411.sh
	scriptFileName := jobKey + "_" + strconv.FormatInt(param.GlueUpdatetime, 10) + suffix
	scriptFilePath = path.Join(scriptSourcePath, scriptFileName)
	f, err := os.Create(scriptFilePath)
	defer f.Close()
	if err != nil {
		t.log.Error(" err:创建脚本文件%s失败:%v", scriptFilePath, err)
		return
	}
	_, err = f.Write([]byte(param.GlueSource))
	if err != nil {
		t.log.Error(" err:写入脚本文件%s失败:%v", scriptFilePath, err)
		return
	}
	return
}

func (t *Task) execScriptFile(ctx context.Context, param *RunReq, scriptFilePath string, glueType GlueType) (err error) {
	paramBytes, err := json.Marshal(param)
	if err != nil {
		t.log.Error(" err:序列化参数失败:%v", err)
		return
	}
	// script params：0=scriptFilePath,1=param、2=分片序号、3=分片总数
	scriptParam := make([]string, 4)
	scriptParam[0] = string(scriptFilePath)
	scriptParam[1] = string(paramBytes)
	scriptParam[2] = strconv.FormatInt(param.BroadcastIndex, 10)
	scriptParam[3] = strconv.FormatInt(param.BroadcastTotal, 10)
	cmd := exec.CommandContext(ctx, glueType.GetCmd(), scriptParam...)
	t.log.Info("cmd执行命令：%v", cmd.Args)
	stdout, err := cmd.StdoutPipe() //获取标准输出对象，可以从该对象中读取输出结果
	if err != nil {
		return
	}
	stderr, err := cmd.StderrPipe() //获取错误输出对象，可以从该对象中读取错误输出结果
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	//添加两个任务，一个为读取标准输出流到日志文件，一个为读取错误输出流到日志文件
	wg.Add(2)
	go t.read(ctx, &wg, stdout, "Info")
	go t.read(ctx, &wg, stderr, "Error")

	err = cmd.Start()
	// 等待任务结束
	wg.Wait()
	err = cmd.Wait()
	if err != nil {
		return errors.New("执行脚本任务失败")
	}
	return
}
func (t *Task) read(ctx context.Context, wg *sync.WaitGroup, std io.ReadCloser, messageType string) {
	reader := bufio.NewReader(std)
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			readString, err := reader.ReadString('\n')
			if err != nil || err == io.EOF {
				std.Close() //关闭输出流
				return
			}
			if messageType == "Info" {
				t.log.Info(readString)
			} else {
				t.log.Error(readString)
			}

		}
	}
}
