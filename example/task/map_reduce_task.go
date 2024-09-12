package task

import (
	"context"
	"encoding/json"
	"fmt"
	gs "github.com/liuhailove/go-scheduler-sdk"
	"strconv"
)

type MpParam struct {
	DbIdx    int
	TableIdx int
	RowStart int
	Limit    int
}

const DB_NUM = 3
const DB_TABLE_NUM = 10

// DbShardingMapReduceTaskFunc 任务执行函数
func DbShardingMapReduceTaskFunc(cxt context.Context, param *gs.RunReq) ([]string, error) {
	if param.BroadcastIndex%DB_NUM == 0 {
		return []string{"1"}, nil
	} else if param.BroadcastIndex%DB_NUM == 1 {
		return []string{"2"}, nil
	} else if param.BroadcastIndex%DB_NUM == 2 {
		return []string{"3"}, nil
	} else if param.BroadcastIndex%DB_NUM == 3 {
		return []string{"4"}, nil
	} else if param.BroadcastIndex%DB_NUM == 4 {
		return []string{"5"}, nil
	} else if param.BroadcastIndex%DB_NUM == 5 {
		return []string{"6"}, nil
	} else if param.BroadcastIndex%DB_NUM == 6 {
		return []string{"7"}, nil
	} else {
		return []string{"8"}, nil
	}

}

// TableShardingMapReduceTaskFunc 表任务执行函数
func TableShardingMapReduceTaskFunc(cxt context.Context, param *gs.RunReq) ([]string, error) {
	nextExecutorParams := make([]string, 0)
	dbIdx, _ := strconv.Atoi(param.ExecutorParams)
	for i := 0; i < DB_TABLE_NUM; i++ {
		nextExecutorParams = append(nextExecutorParams, marshal(MpParam{DbIdx: dbIdx, TableIdx: i}))
	}
	return nextExecutorParams, nil
}

// RowShardingMapReduceTaskFunc 数据分批任务执行函数
func RowShardingMapReduceTaskFunc(cxt context.Context, param *gs.RunReq) ([]string, error) {
	mpParam := new(MpParam)
	json.Unmarshal([]byte(param.ExecutorParams), &mpParam)
	totalRows := 10001
	limit := 1000
	nextExecutorParams := make([]string, 0)
	for i := 0; i < totalRows/limit; i++ {
		nextExecutorParams = append(nextExecutorParams, marshal(MpParam{mpParam.DbIdx, mpParam.TableIdx, i * limit, limit}))
	}
	if totalRows%limit != 0 {
		nextExecutorParams = append(nextExecutorParams, marshal(MpParam{mpParam.DbIdx, mpParam.TableIdx, (totalRows / limit) * limit, limit}))
	}
	return nextExecutorParams, nil
}

// RowSumShardingMapReduceTaskFunc 分批统计任务执行函数
func RowSumShardingMapReduceTaskFunc(cxt context.Context, param *gs.RunReq) ([]string, error) {
	mpParam := new(MpParam)
	json.Unmarshal([]byte(param.ExecutorParams), &mpParam)
	sum := mpParam.DbIdx * mpParam.TableIdx * mpParam.Limit
	return []string{strconv.Itoa(sum)}, nil
}

var totalSum = 0

// RowReduceShardingMapReduceTaskFunc 最终统计任务执行函数
func RowReduceShardingMapReduceTaskFunc(cxt context.Context, param *gs.RunReq) ([]string, error) {
	rowSum, _ := strconv.Atoi(param.ExecutorParams)
	totalSum += rowSum
	fmt.Println(totalSum)
	return []string{"ok"}, nil
}

func marshal(mp MpParam) string {
	data, _ := json.Marshal(mp)
	return string(data)
}

// LargeShardingMapReduceTaskFunc 任务执行函数
func LargeShardingMapReduceTaskFunc(cxt context.Context, param *gs.RunReq) ([]string, error) {
	arr := make([]string, 0)
	for i := 0; i < 10000; i++ {
		num := int64(i) + param.BroadcastIndex*10000
		arr = append(arr, strconv.FormatInt(num, 10))
	}
	//if param.BroadcastIndex%DB_NUM == 0 {
	//	return arr, nil
	//} else if param.BroadcastIndex%DB_NUM == 1 {
	//	return arr, nil
	//} else {
	//	return arr, nil
	//}
	return arr, nil
}
