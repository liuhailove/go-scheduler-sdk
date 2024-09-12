package task

import (
	"context"
	xxl "github.com/liuhailove/go-scheduler-sdk"
)

func Panic(cxt context.Context, param *xxl.RunReq) (msg []string, err error) {
	panic("test panic")
	return
}
