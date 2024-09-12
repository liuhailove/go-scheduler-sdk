package gs

import (
	"encoding/json"
	"os"
	"strconv"
	"time"
	"unsafe"
)

var (
	GeneralMsg = "{\"code\":200,\"msg\":\"\"}"
)

// Int64ToStr int64 to str
func Int64ToStr(i int64) string {
	return strconv.FormatInt(i, 10)
}

// 执行任务回调
func returnCall(req *RunReq, code int64, msg []string) []byte {
	data := call{
		&callElement{
			JobID:                req.JobID,
			LogID:                req.LogID,
			ClientVersion:        "v1.3.2-release",
			LogDateTim:           req.LogDateTime,
			HandleFinishDateTime: time.Now().UnixNano() / 1e6,
			ExecuteResult: &ExecuteResult{
				Code: code,
				Msg:  msg,
			},
			HandleCode: int(code),
			HandleMsg:  msg,
		},
	}
	str, _ := json.Marshal(data)
	return str
}

// returnReportDelay 返回延迟上报结构体
func returnReportDelay(jobId, logId, logDateTim, currentDateTim int64, address string) []byte {
	data := &reportDelay{
		JobID:          jobId,
		LogID:          logId,
		LogDateTim:     logDateTim,
		CurrentDateTim: currentDateTim,
		Address:        address,
	}
	jsonBytes, _ := json.Marshal(data)
	return jsonBytes
}

// 执行任务回调
func returnBatchCall(params []callbackParams) []byte {
	data := call{}
	for _, param := range params {
		data = append(data, &callElement{
			LogID:         param.task.Param.LogID,
			LogDateTim:    param.task.Param.LogDateTime,
			ClientVersion: "v1.3.2-release",
			ExecuteResult: &ExecuteResult{
				Code: param.code,
				Msg:  param.msg,
			},
			HandleCode: int(param.code),
			HandleMsg:  param.msg,
		})
	}
	str, _ := json.Marshal(data)
	return str
}

// 杀死任务返回
func returnKill(req *killReq, code int64) []byte {
	msg := ""
	if code != 200 {
		msg = "Task kill err"
	}
	data := res{
		Code: code,
		Msg:  msg,
	}
	str, _ := json.Marshal(data)
	return str
}

// 忙碌返回
func returnIdleBeat(code int64) []byte {
	msg := ""
	if code != 200 {
		msg = "Task is busy"
	}
	data := res{
		Code: code,
		Msg:  msg,
	}
	str, _ := json.Marshal(data)
	return str
}

// 通用返回
func returnGeneral() []byte {
	return str2Bytes(GeneralMsg)
}

// 返回失败
func returnError(msg string) []byte {
	data := &res{
		Code: 500,
		Msg:  msg,
	}
	str, _ := json.Marshal(data)
	return str
}

func FileIsExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

// Contains 判断是否包含
func Contains(arr []string, str string) bool {
	if arr == nil {
		return false
	}
	for _, element := range arr {
		if str == element {
			return true
		}
	}
	return false
}

func str2Bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}
