package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/liuhailove/go-scheduler-sdk/base"
	"github.com/liuhailove/go-scheduler-sdk/utils"
	"runtime"
	"strings"
)

const (
	GlobalCallerDepth = 4
	MaxKvLen          = 6
)

// RecordLogWithLog 执行日记记录
func RecordLogWithLog(cxt context.Context, jobId, logId int64, msg string, keysAndValues ...interface{}) {
	metricItem := &base.MetricItem{JobId: jobId, LogId: logId, Timestamp: utils.CurrentTimeMillis(), Msg: msg}
	file, line := caller(GlobalCallerDepth)
	metricItem.Caller = fmt.Sprintf("%s:%d", file, line)
	kvLen := len(keysAndValues)
	for i := 0; i < kvLen && i < MaxKvLen; {
		k := keysAndValues[i]
		var v interface{}
		if i+1 >= kvLen {
			v = ""
		} else {
			v = keysAndValues[i+1]
		}
		kStr, kIsStr := k.(string)
		if !kIsStr {
			kStr = fmt.Sprintf("%+v", k)
		}
		var vStr string
		switch v.(type) {
		case string:
			vStr = v.(string)
		case error:
			vStr = v.(error).Error()
		default:
			if vbs, err := json.Marshal(v); err != nil {
				vStr = fmt.Sprintf("%+v", v)
			} else {
				vStr = string(vbs)
			}
		}
		if i == 0 {
			metricItem.Key1 = kStr
			metricItem.Value1 = vStr
		} else if i == 2 {
			metricItem.Key2 = kStr
			metricItem.Value2 = vStr
		} else if i == 4 {
			metricItem.Key3 = kStr
			metricItem.Value3 = vStr
		}
		i = i + 2
	}
	AggregateIntoMap(metricItem)
}

// RecordLog 执行日记记录
func RecordLog(ctx context.Context, msg string, keysAndValues ...interface{}) {
	var logIdf = ctx.Value("logId")
	if logIdf == nil {
		logger.Error("ctx does not contain logId")
		return
	}
	var jobIdf = ctx.Value("jobId")
	if logId, ok := logIdf.(int64); ok {
		RecordLogWithLog(ctx, jobIdf.(int64), logId, msg, keysAndValues...)
	} else {
		logger.Error("ctx logId is not int64 type")
	}
}

func caller(depth int) (file string, line int) {
	_, file, line, ok := runtime.Caller(depth)
	if !ok {
		file = "???"
		line = 0
	}

	// extract
	if osType := runtime.GOOS; osType == "windows" {
		file = strings.ReplaceAll(file, "\\", "/")
	}
	idx := strings.LastIndex(file, "/")
	file = file[idx+1:]
	return
}
