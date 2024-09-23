package task

import (
	"context"
	"errors"
	"fmt"
	gs "github.com/liuhailove/go-scheduler-sdk"
	"github.com/liuhailove/go-scheduler-sdk/logging"
	"time"
)

var cnt = 0
var zero = 1

type A struct {
	A1 string
	V1 string
}

func Test(ctx context.Context, param *gs.RunReq) (msg []string, err error) {
	fmt.Println("query second key", ctx.Value("secondKey"))
	//time.Sleep(20 * time.Second)
	for i := 0; i < 10; i++ {
		//
		//fmt.Println(time.Now().String() + ":test one task" + param.ExecutorHandler + " param：" + param.ExecutorParams + " log_id:" + Int64ToStr(param.LogID))
		//time.Sleep(1 * time.Millisecond)
		var a = A{}
		a.V1 = "a"

		logging.RecordLogWithLog(ctx, param.JobID, param.LogID, "FirstProcess", "process.first", i)

		a.A1 = "b"

		//time.Sleep(2 * time.Millisecond)
		//logging.RecordLogWithLog(ctx, param.LogID, "SecondProcess", "process.first", 0, "process.second", 1)
	}

	//time.Sleep(1 * time.Second)
	//
	//logging.RecordLogWithLog(ctx, param.LogID, "100%", "process", 100, "process1", 200)
	//
	//ctx = context.WithValue(ctx, "logId", param.LogID)
	//logging.RecordLog(ctx, "成功", "process", 150)
	//fmt.Println("time:"+time.Now().String()+";count=%d", cnt)
	//cnt++

	return []string{"test 1 done", "test 2 done"}, nil
}

func TaskBillGenerate(cxt context.Context, param *gs.RunReq) (msg []string, err error) {
	fmt.Println(time.Now().String() + ":test one task" + param.ExecutorHandler + " param：" + param.ExecutorParams + " log_id:" + gs.Int64ToStr(param.LogID))
	cnt++
	if cnt%3 == 0 {
		zero--
		fmt.Println(cnt / zero)
	} else if cnt%2 == 0 {
		err = errors.New("not 2")
		return nil, err
	}
	//time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
	return []string{"test done"}, nil
}
