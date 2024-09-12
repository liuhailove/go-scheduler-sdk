package gs

import "context"

// CheckResultFunc 执行结果检查，true：处理成功，false:处理失败,此方法只在以下场景才会调用
// 1.调度中心配置了需要结果检查
// 2.执行结果上报时，虽然调度中心响应了成功，但是最终却丢失了（此场景只发生在调度中心出现故障时才发生，如crash或者kill -9）
type CheckResultFunc func(cxt context.Context, param *RunReq) bool

// DefaultCheckResult 默认结果检查处理，
// 如果用户没有自己处理返回结果，则默认返回处理失败
func DefaultCheckResult(cxt context.Context, param *RunReq) bool {
	return false
}
