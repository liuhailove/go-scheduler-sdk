package task

import (
	"context"
	gs "github.com/liuhailove/go-scheduler-sdk"
)

func Panic(cxt context.Context, param *gs.RunReq) (msg []string, err error) {
	panic("test panic")
	return
}
