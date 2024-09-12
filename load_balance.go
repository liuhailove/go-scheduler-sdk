package gs

import (
	"math/rand"
	"sync/atomic"
)

// LoadBalancer 负载均衡接口
type LoadBalancer interface {
	// Balance 均衡策略，返回被负载的service
	Balance(services []string) string
}

// RandomBalancer 随机负载均衡算法
type RandomBalancer struct {
}

func (r *RandomBalancer) Balance(services []string) string {
	if len(services) == 0 {
		return ""
	}
	idx := rand.Intn(len(services))
	return services[idx]
}

// RoundBalancer 轮询负载均衡算法
type RoundBalancer struct {
	count atomic.Value
}

func (r *RoundBalancer) Balance(services []string) string {
	if len(services) == 0 {
		return ""
	}
	// 为空，大于一百万，重置一下
	if r.count.Load() == nil || r.count.Load().(int32) > 1000000 {
		r.count.Store(rand.Int31n(100))
	}
	value := r.count.Load().(int32)
	r.count.Store(atomic.AddInt32(&value, 1))
	return services[r.count.Load().(int32)%int32(len(services))]
}

// FirstBalancer 选择第一个
type FirstBalancer struct {
}

func (f FirstBalancer) Balance(services []string) string {
	if len(services) == 0 {
		return ""
	}
	return services[0]
}

// LastBalancer 选择最后一个
type LastBalancer struct {
}

func (l LastBalancer) Balance(services []string) string {
	if len(services) == 0 {
		return ""
	}
	return services[len(services)-1]
}

// 负载均衡策略
var randomBalancer RandomBalancer
var roundBalancer RoundBalancer
var firstBalancer FirstBalancer
var lastBalancer LastBalancer

// GetServiceAddr 加载服务地址
func GetServiceAddr(services []string, mode LoadBalanceMode) string {
	if mode == RODOM_MODE {
		return randomBalancer.Balance(services)
	} else if mode == ROUND_MODE {
		return roundBalancer.Balance(services)
	} else if mode == FIRST_MODE {
		return firstBalancer.Balance(services)
	} else if mode == LAST_MODE {
		return lastBalancer.Balance(services)
	}
	return roundBalancer.Balance(services)
}
