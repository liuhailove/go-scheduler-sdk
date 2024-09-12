package gs

import "context"

// CheckIdleFunc 空闲检查，true：空闲，false:忙碌，只有在业务自己实现空闲检查时，才有用到此方法
// 备注：调度中心的路由策略需要时忙碌转移
type CheckIdleFunc func(cxt context.Context, jobID int64) bool

// DefaultCheckIdle 默认空闲检查，返回真，
func DefaultCheckIdle(cxt context.Context, jobID int64) bool {
	return true
}
