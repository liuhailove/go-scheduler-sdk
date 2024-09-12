package task

import (
	"context"
	"fmt"
	gs "github.com/liuhailove/go-scheduler-sdk"
	"github.com/liuhailove/go-scheduler-sdk/logging"
	"math/rand"
	"time"
)

func Test3(ctx context.Context, param *gs.RunReq) (msg []string, err error) {
	fmt.Println(time.Now().String() + ":test three task" + param.ExecutorHandler + " param：" + param.ExecutorParams + " log_id:" + gs.Int64ToStr(param.LogID))
	time.Sleep(time.Second * 5)
	for i := 0; i < 1; i++ {
		rand.Seed(time.Now().UnixNano())
		randNum := rand.Intn(200)
		time.Sleep(time.Millisecond * time.Duration(randNum))
		logging.RecordLogWithLog(ctx, param.JobID, param.LogID, "Test3", "process", i)
	}
	return []string{"test three done"}, nil
}
