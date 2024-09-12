package task

import (
	"context"
	gs "github.com/liuhailove/go-scheduler-sdk"
)

func TaskFeeCalculate(ctx context.Context, param *gs.RunReq) (msg []string, err error) {
	return []string{"TaskFeeCalculate"}, nil
}
