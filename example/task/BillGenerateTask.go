package task

import (
	"context"
	xxl "github.com/liuhailove/go-scheduler-sdk"
)

func BillGenerateTask(ctx context.Context, param *xxl.RunReq) (msg []string, err error) {
	return []string{"BillGenerateTask"}, nil
}
