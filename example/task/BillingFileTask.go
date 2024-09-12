package task

import (
	"context"
	xxl "github.com/liuhailove/go-scheduler-sdk"
)

func BillingFileTask(ctx context.Context, param *xxl.RunReq) (msg []string, err error) {
	return []string{"BillingFileTask"}, nil
}
