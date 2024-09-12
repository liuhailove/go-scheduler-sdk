package gs

import (
	"github.com/liuhailove/go-scheduler-sdk/logging"
	"time"
)

// ServerMode 连接server的模式
type ServerMode int

const (
	// HISTORY_SERVER_MODE 固定调度中心模式
	HISTORY_SERVER_MODE ServerMode = 0
	// FIX_SERVER_MODE 固定调度中心模式
	FIX_SERVER_MODE ServerMode = 1
	// ETCD_LOOKUP_MODE etcd自动查询模式
	ETCD_LOOKUP_MODE ServerMode = 2
	// MIXED_MODE 混合模式，此种模式优先使用etcd，如果etcd不可用，则使用固定IP
	MIXED_MODE ServerMode = 3
)

// DEFAULT_TIME_OUT 默认超时时间
const DEFAULT_TIME_OUT = time.Second * 10

// LoadBalanceMode 负载均衡模式，仅仅在ServerMode为ETCD_LOOKUP_MODE生效
type LoadBalanceMode int

const (
	DEFAULT_MODE LoadBalanceMode = 0
	// RODOM_MODE 随机
	RODOM_MODE LoadBalanceMode = 1
	// ROUND_MODE 轮询
	ROUND_MODE LoadBalanceMode = 2
	// FIRST_MODE 第一个
	FIRST_MODE LoadBalanceMode = 3
	// LAST_MODE 最后一个
	LAST_MODE LoadBalanceMode = 4
)

const (
	// DefaultMetricLogSingleFileMaxSize 10Mb
	DefaultMetricLogSingleFileMaxSize uint64 = 10485760
	// DefaultMetricLogMaxFileAmount 保存最大文件数
	DefaultMetricLogMaxFileAmount uint32 = 10
	// DefaultLogDir 默认日志目录
	DefaultLogDir = "gslog"
	// DefaultFlushIntervalInSec 日志刷新间隔
	DefaultFlushIntervalInSec uint32 = 1
)

type Options struct {
	ServerAddr               string          `json:"server_addr"`                 // 调度中心地址
	AccessToken              string          `json:"access_token"`                // 请求令牌
	Timeout                  time.Duration   `json:"timeout"`                     // 接口超时时间
	ExecutorIp               string          `json:"executor_ip"`                 // 本地(执行器)IP(可自行获取)
	ExecutorPort             string          `json:"executor_port"`               // 本地(执行器)端口
	RegistryKey              string          `json:"registry_key"`                // 执行器名称
	LogDir                   string          `json:"log_dir"`                     // 日志目录
	Sync                     bool            `json:"sync"`                        // 是否同步启动，默认为false
	Endpoints                []string        `json:"endpoints"`                   // etcd地址
	ServerMode               ServerMode      `json:"server_mode"`                 // 连接server的模式
	LoadBalanceMode          LoadBalanceMode `json:"load_balance_mode"`           // 负载均衡模式
	MaxConcurrencyNum        uint16          `json:"max_concurrency_num"`         // 最发并发数，默认为CPU*2
	Collapse                 bool            `json:"collapse"`                    // 开启请求合并
	TimerDelayInMilliseconds int             `json:"timer_delay_in_milliseconds"` // 请求合并延迟时间

	l               logging.Logger  //日志处理
	checkResultFunc CheckResultFunc //执行结果检查
	// 空闲检查
	checkIdleFunc CheckIdleFunc
	// 串行key生成策略
	serialKeyPostfixGenFunc SerialKeyPostfixGenFunc

	// metric log相关
	// 日志刷新间隔
	flushIntervalInSec uint32
	// 单个文件最大size
	metricLogSingleFileMaxSize uint64
	// metricLogMaxFileAmount 最大保存多少个file
	metricLogMaxFileAmount uint32
	// 日志目录
	logDir string

	// routerFlag 路由标签，此值会影响Server的路由策略
	routerFlag string

	// 任务前置处理
	taskWrappers []TaskWrapper
}

func newOptions(opts ...Option) Options {
	opt := Options{
		RegistryKey:                DefaultRegistryKey,
		Collapse:                   false,
		TimerDelayInMilliseconds:   DefaultTimerDelayInMilliseconds,
		Timeout:                    DEFAULT_TIME_OUT,
		metricLogSingleFileMaxSize: DefaultMetricLogSingleFileMaxSize,
		metricLogMaxFileAmount:     DefaultMetricLogMaxFileAmount,
		logDir:                     DefaultLogDir,
		flushIntervalInSec:         DefaultFlushIntervalInSec,
	}

	for _, o := range opts {
		o(&opt)
	}

	if opt.l == nil {
		opt.l = &logging.DefaultLogger{}
	}

	if opt.checkResultFunc == nil {
		opt.checkResultFunc = DefaultCheckResult
	}

	if opt.checkIdleFunc == nil {
		opt.checkIdleFunc = DefaultCheckIdle
	}

	if opt.serialKeyPostfixGenFunc == nil {
		opt.serialKeyPostfixGenFunc = DefaultSerialKeyPostfixGenFunc
	}

	return opt
}

type Option func(o *Options)

var (
	DefaultRegistryKey              = "golang-jobs"
	DefaultTimerDelayInMilliseconds = 50
	MinTimerDelayInMilliseconds     = 10
	MaxTimerDelayInMilliseconds     = 1000
)

// ServerAddr 设置调度中心地址
func ServerAddr(addr string) Option {
	return func(o *Options) {
		o.ServerAddr = addr
	}
}

// AccessToken 请求令牌
func AccessToken(token string) Option {
	return func(o *Options) {
		o.AccessToken = token
	}
}

// RegistryKey 设置执行器标识
func RegistryKey(registryKey string) Option {
	return func(o *Options) {
		o.RegistryKey = registryKey
	}
}

// LoadBalance 设置负载均衡模式
func LoadBalance(mode LoadBalanceMode) Option {
	return func(o *Options) {
		o.LoadBalanceMode = mode
	}
}

// SetServerMode 设置连接服务的模式
func SetServerMode(mode ServerMode) Option {
	return func(o *Options) {
		o.ServerMode = mode
	}
}

// Endpoints 设置ETCD
func Endpoints(endpoints []string) Option {
	return func(o *Options) {
		o.Endpoints = endpoints
	}
}

// SetLogger 设置日志处理器
func SetLogger(l logging.Logger) Option {
	return func(o *Options) {
		o.l = l
	}
}

// SetSync 设置是否同步
func SetSync(sync bool) Option {
	return func(o *Options) {
		o.Sync = sync
	}
}

func SetMaxConcurrencyNum(maxConcurrencyNum uint16) Option {
	return func(o *Options) {
		o.MaxConcurrencyNum = maxConcurrencyNum
	}
}

func SetCollapse(collapse bool) Option {
	return func(o *Options) {
		o.Collapse = collapse
	}
}
func SetTimerDelayInMilliseconds(timerDelayInMilliseconds int) Option {
	return func(o *Options) {
		o.TimerDelayInMilliseconds = timerDelayInMilliseconds
	}
}

// Timeout 设置超时时间
func Timeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.Timeout = timeout
	}
}

// SetCheckResultFunc 设置结果检查方法
func SetCheckResultFunc(resultFunc CheckResultFunc) Option {
	return func(o *Options) {
		o.checkResultFunc = resultFunc
	}
}

// SetCheckIdleFunc 设置空闲检查方法
func SetCheckIdleFunc(checkIdleFunc CheckIdleFunc) Option {
	return func(o *Options) {
		o.checkIdleFunc = checkIdleFunc
	}
}

// SetSerialKeyPostfixGenFunc 设置串行key生成方法
func SetSerialKeyPostfixGenFunc(serialKeyPostfixGenFunc SerialKeyPostfixGenFunc) Option {
	return func(o *Options) {
		o.serialKeyPostfixGenFunc = serialKeyPostfixGenFunc
	}
}

// SetMetricLogMaxFileAmount 设置最大保存多少个file
func SetMetricLogMaxFileAmount(metricLogMaxFileAmount uint32) Option {
	return func(o *Options) {
		o.metricLogMaxFileAmount = metricLogMaxFileAmount
	}
}

// SetLogDir 日志目录
func SetLogDir(logDir string) Option {
	return func(o *Options) {
		o.logDir = logDir
	}
}

// SetMetricLogSingleFileMaxSize 设置单个metric文件大小
func SetMetricLogSingleFileMaxSize(metricLogSingleFileMaxSize uint64) Option {
	return func(o *Options) {
		o.metricLogSingleFileMaxSize = metricLogSingleFileMaxSize
	}
}

// SetFlushIntervalInSec 设置日志刷新时间
func SetFlushIntervalInSec(flushIntervalInSec uint32) Option {
	return func(o *Options) {
		o.flushIntervalInSec = flushIntervalInSec
	}
}

func SetRouterFlag(routerFlag string) Option {
	return func(o *Options) {
		o.routerFlag = routerFlag
	}
}

func ExecutorIp(executorIp string) Option {
	return func(o *Options) {
		o.ExecutorIp = executorIp
	}
}

func ExecutorPort(executorPort string) Option {
	return func(o *Options) {
		o.ExecutorPort = executorPort
	}
}

// WithTaskWrapper 设置预处理方法Wrapper
func WithTaskWrapper(taskWrapper TaskWrapper) Option {
	return func(o *Options) {
		o.taskWrappers = append(o.taskWrappers, taskWrapper)
	}
}
