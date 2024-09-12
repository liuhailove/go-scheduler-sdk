package task

import (
	"bytes"
	"context"
	"fmt"
	gs "github.com/liuhailove/go-scheduler-sdk"
	"runtime"
	"strconv"
	"time"
)

// CustomerShardingTaskFunc 任务执行函数
func CustomerShardingTaskFunc(cxt context.Context, param *gs.RunReq) ([]string, error) {
	arr := make([]string, 0)
	for i := 0; i < 2; i++ {
		num := int64(i) + param.BroadcastIndex*2
		arr = append(arr, strconv.FormatInt(param.BroadcastIndex, 10)+"-"+strconv.FormatInt(num, 10))
	}
	return arr, nil
}

// CustomerPrintTaskFunc 任务执行函数
func CustomerPrintTaskFunc(cxt context.Context, param *gs.RunReq) ([]string, error) {
	fmt.Println(fmt.Sprintf("gid=%d,logId=%d,param=%s", GetGoroutineID(), param.LogID, param.ExecutorParams))
	fmt.Println(fmt.Sprintf("inner start:logId=%d,startTime=%d", param.LogID, time.Now().Second()))
	time.Sleep(5 * time.Second)
	fmt.Println(fmt.Sprintf("inner end:logId=%d,endTime=%d", param.LogID, time.Now().Second()))
	return []string{"success"}, nil
}

func GetGoroutineID() uint64 {
	b := make([]byte, 64)
	runtime.Stack(b, false)
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func MySerialKeyPostfixGenFunc(param *gs.RunReq) string {
	//var s = fmt.Sprintf("串行策略，paaram=%s,key=%s", param.ExecutorParams, strings.Split(param.ExecutorParams, "-")[0])
	//fmt.Println(s)
	//return strconv.FormatInt(param.LogID%1000, 10)
	//return strings.Split(param.ExecutorParams, "-")[0]
	return strconv.FormatInt(param.JobID, 10)
}
