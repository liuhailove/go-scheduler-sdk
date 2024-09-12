package task

import (
	"context"
	gs "github.com/liuhailove/go-scheduler-sdk"
)

func TaskBizDateChange(ctx context.Context, param *gs.RunReq) (msg []string, err error) {
	return []string{"TaskBizDateChange"}, nil
}
