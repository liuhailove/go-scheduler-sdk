package gs

// SerialKeyPostfixGenFunc 串行执行策略时，串行key的生成策略，
// 默认情况下以JobId:JobId作为串行依据，然而有些场景下业务希望
// 又可以按照策略并行执行，比如同一个用户下希望是串行执行
// 不同用户下并行执行
type SerialKeyPostfixGenFunc func(param *RunReq) string

// DefaultSerialKeyPostfixGenFunc 默认串行key生成方法，以JobId:JobId作为Key
func DefaultSerialKeyPostfixGenFunc(param *RunReq) string {
	return Int64ToStr(param.JobID)
}
