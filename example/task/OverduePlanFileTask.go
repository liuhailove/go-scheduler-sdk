package task

import (
	"context"
	gs "github.com/liuhailove/go-scheduler-sdk"
)

func OverduePlanFileTask(ctx context.Context, param *gs.RunReq) (msg []string, err error) {
	return []string{"OverduePlanFileTask"}, nil
}
