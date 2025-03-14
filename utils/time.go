package utils

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	TimeFormat         = "2006-01-02 15:04:05"
	DateFormat         = "2006-01-02"
	UnixTimeUnitOffset = uint64(time.Millisecond / time.Nanosecond)
)

var (
	_ Clock = &RealClock{}
	_ Clock = &MockClock{}

	_ Ticker = &RealTicker{}
	_ Ticker = &MockTicker{}

	_ TickerCreator = &RealTickerCreator{}
	_ TickerCreator = &MockTickerCreator{}
)

var (
	currentClock         *atomic.Value
	currentTickerCreator *atomic.Value
)

func init() {
	// 开始时钟任务
	StartTimeTicker()

	// 设置时钟
	realClock := NewRealClock()
	currentClock = new(atomic.Value)
	SetClock(realClock)

	realTickerCreator := NewRealTickerCreator()
	currentTickerCreator = new(atomic.Value)
	SetTickerCreator(realTickerCreator)
}

type Clock interface {
	// Now 当前时间
	Now() time.Time
	// Sleep 休眠
	Sleep(d time.Duration)
	// CurrentTimeMillis 当前时间戳，毫秒
	CurrentTimeMillis() uint64
	// CurrentTimeNano 当前时间戳，纳秒
	CurrentTimeNano() uint64
}

// clockWrapper is used for atomic operation.
type clockWrapper struct {
	clock Clock
}

// RealClock wraps some APIs of time package.
type RealClock struct {
}

func NewRealClock() *RealClock {
	return &RealClock{}
}

func (r RealClock) Now() time.Time {
	return time.Now()
}

func (r RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

func (r RealClock) CurrentTimeMillis() uint64 {
	tickerNow := CurrentTimeMillsWithTicker()
	if tickerNow > uint64(0) {
		return tickerNow
	}
	return uint64(time.Now().UnixNano()) / UnixTimeUnitOffset
}

func (r RealClock) CurrentTimeNano() uint64 {
	return uint64(r.Now().UnixNano())
}

// MockClock is used for testing.
type MockClock struct {
	lock sync.RWMutex
	now  time.Time
}

func NewMockClock() *MockClock {
	return &MockClock{now: time.Now()}
}

func (t *MockClock) Now() time.Time {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.now
}

func (t *MockClock) Sleep(d time.Duration) {
	if d < 0 {
		return
	}
	t.lock.Lock()
	t.now = t.now.Add(d)
	t.lock.Unlock()
	t.Sleep(time.Millisecond)
}

func (t *MockClock) CurrentTimeMillis() uint64 {
	return uint64(t.Now().UnixNano()) / UnixTimeUnitOffset
}

func (t *MockClock) CurrentTimeNano() uint64 {
	return uint64(t.now.UnixNano())
}

// Ticker interface encapsulates operations like time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealTicker wraps time.Ticker
type RealTicker struct {
	t *time.Ticker
}

func NewRealTicker(d time.Duration) *RealTicker {
	return &RealTicker{
		t: time.NewTicker(d),
	}
}

func (t *RealTicker) C() <-chan time.Time {
	return t.t.C
}

func (t *RealTicker) Stop() {
	t.t.Stop()
}

// MockTicker is usually used for testing.
// MockTicker and MockClock are usually used together.
type MockTicker struct {
	lock   sync.Mutex
	period time.Duration
	c      chan time.Time
	last   time.Time
	stop   chan struct{}
}

func NewMockTicker(d time.Duration) *MockTicker {
	t := &MockTicker{
		period: d,
		c:      make(chan time.Time, 1),
		last:   Now(),
		stop:   make(chan struct{}),
	}
	go t.checkLoop()

	return t
}

func (t *MockTicker) C() <-chan time.Time {
	return t.c
}

func (t *MockTicker) Stop() {
	close(t.stop)
}

func (t *MockTicker) check() {
	t.lock.Lock()
	defer t.lock.Unlock()

	now := Now()
	for next := t.last.Add(t.period); !next.After(now); next = next.Add(t.period) {
		t.last = next
		select {
		case <-t.stop:
			return
		case t.c <- t.last:
		default:

		}
	}
}

// TickerCreator is used to create Ticker
type TickerCreator interface {
	NewTicker(d time.Duration) Ticker
}

// tickerCreatorWrapper is used for atomic operation.
type tickerCreatorWrapper struct {
	tickerCreator TickerCreator
}

// RealTickerCreator is used to create RealTicker which wraps time.Ticker.
type RealTickerCreator struct {
}

func NewRealTickerCreator() *RealTickerCreator {
	return &RealTickerCreator{}
}

func (tc *RealTickerCreator) NewTicker(d time.Duration) Ticker {
	return NewRealTicker(d)
}

func (t *MockTicker) checkLoop() {
	ticker := time.NewTicker(time.Millisecond)
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
		}
		t.check()
	}
}

// MockTickerCreator is used create MockTicker which is usually used for testing.
// MockTickerCreator and MockClock are usually used together.
type MockTickerCreator struct {
}

func NewMockTickerCreator() *MockTickerCreator {
	return &MockTickerCreator{}
}

func (tc *MockTickerCreator) NewTicker(d time.Duration) Ticker {
	return NewMockTicker(d)
}

// SetClock sets the clock used by util package.
// In general, no need to set it. It is usually used for testing.
func SetClock(c Clock) {
	currentClock.Store(&clockWrapper{c})
}

// CurrentClock returns the current clock used by util package.
func CurrentClock() Clock {
	return currentClock.Load().(*clockWrapper).clock
}

func SetTickerCreator(tc TickerCreator) {
	currentTickerCreator.Store(&tickerCreatorWrapper{tc})
}

// CurrentTickerCreator returns the current ticker creator used by util package.
func CurrentTickerCreator() TickerCreator {
	return currentTickerCreator.Load().(*tickerCreatorWrapper).tickerCreator
}

func NewTicker(d time.Duration) Ticker {
	return CurrentTickerCreator().NewTicker(d)
}

// FormatTimeMillis formats Unix timestamp (ms) to time string.
func FormatTimeMillis(tsMillis uint64) string {
	return time.Unix(0, int64(tsMillis*UnixTimeUnitOffset)).Format(TimeFormat)
}

// FormatDate formats Unix timestamp (ms) to date string
func FormatDate(tsMillis uint64) string {
	return time.Unix(0, int64(tsMillis*UnixTimeUnitOffset)).Format(DateFormat)
}

// CurrentTimeMillis Returns the current Unix timestamp in milliseconds.
func CurrentTimeMillis() uint64 {
	return CurrentClock().CurrentTimeMillis()
}

// CurrentTimeNano Returns the current Unix timestamp in nanoseconds.
func CurrentTimeNano() uint64 {
	return CurrentClock().CurrentTimeNano()
}

// Now returns the current local time.
func Now() time.Time {
	return CurrentClock().Now()
}

// Sleep pauses the current goroutine for at least the duration d.
// A negative or zero duration causes Sleep to return immediately.
func Sleep(d time.Duration) {
	CurrentClock().Sleep(d)
}
