package task

import (
	"context"
	"fmt"
	xxl "github.com/liuhailove/go-scheduler-sdk"
	"time"
)

func TestP(cxt context.Context, param *xxl.RunReq) (msg []string, err error) {
	fmt.Println(time.Now().String() + ":test three task" + param.ExecutorHandler + " param：" + param.ExecutorParams + " log_id:" + xxl.Int64ToStr(param.LogID))
	time.Sleep(time.Second * 10)
	return []string{"test parent done"}, nil
}
