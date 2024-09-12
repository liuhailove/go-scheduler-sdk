package utils

import "sync/atomic"

type AtomicBool struct {
	// 默认0，意味着false
	flag int32
}

func (b *AtomicBool) CompareAndSet(old, new bool) bool {
	if old == new {
		return true
	}
	var oldInt, newInt int32
	if old {
		oldInt = 1
	}
	if new {
		newInt = 1
	}
	return atomic.CompareAndSwapInt32(&(b.flag), oldInt, newInt)
}

func (b *AtomicBool) Set(value bool) {
	i := int32(0)
	if value {
		i = 1
	}
	atomic.StoreInt32(&(b.flag), i)
}

func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&(b.flag)) != 0
}
