package task

import (
	"context"
	"fmt"
	gs "github.com/liuhailove/go-scheduler-sdk"
	"github.com/liuhailove/go-scheduler-sdk/logging"
	"math/rand"
	"strconv"
	"time"
)

func Test2(ctx context.Context, param *gs.RunReq) (msg []string, err error) {
	fmt.Println("in test 2")
	str := fmt.Sprintf("param %s", param.ExecutorParams)
	fmt.Println(str)
	fmt.Println(param.LogID)

	for i := 0; i < 1000; i++ {
		fmt.Println("idx:" + strconv.Itoa(i))
		rand.Seed(time.Now().UnixNano())
		randNum := rand.Intn(100)
		time.Sleep(time.Millisecond * time.Duration(randNum))
		logging.RecordLogWithLog(ctx, param.JobID, param.LogID, "进度10%", "process", i)
	}

	//offlineAutoReportTimer := time.NewTicker(1 * time.Second)
	//for {
	//	select {
	//	case <-cxt.Done():
	//		fmt.Println("task 123" + param.ExecutorHandler + "被手动终止")
	//		return
	//	case <-offlineAutoReportTimer.C:
	//		fmt.Println("hello")
	//		//default:
	//		//	fmt.Println()
	//		//num++
	//		//time.Sleep(10 * time.Second)
	//		//fmt.Println("test one task"+param.ExecutorHandler+" param："+param.ExecutorParams+"执行行", num)
	//		//if num > 10 {
	//		//	fmt.Println("test one task" + param.ExecutorHandler + " param：" + param.ExecutorParams + "执行完毕！")
	//		//	return
	//		//}
	//	}
	//}

	return
}
