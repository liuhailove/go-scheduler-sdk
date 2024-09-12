package logging

import (
	"github.com/liuhailove/go-scheduler-sdk/base"
	"github.com/liuhailove/go-scheduler-sdk/utils"
	"sort"
	"sync"
	"time"
)

type metricTimeMap = map[uint64][]*base.MetricItem

const (
	logFlushQueueSize = 60
	maxCacheSecond    = 5 * 1000
)

var (
	// The timestamp of the last fetching. The time unit is ms (= second * 1000).
	lastFetchTime int64 = -1
	writeChan           = make(chan metricTimeMap, logFlushQueueSize)
	stopChan            = make(chan struct{})
	initOnce      sync.Once

	// 当前map
	currentMetricTimeMap = make(map[uint64][]*base.MetricItem)

	metricWriter MetricLogWriter

	logger Logger

	mut sync.RWMutex
)

// InitTask 日志任务初始化
// metricLogSingleFileMaxSize 单个文件最大size
// metricLogMaxFileAmount 最大保存多少个file
func InitTask(metricLogSingleFileMaxSize uint64, metricLogMaxFileAmount, flushInterval uint32, appName, logDir string, log Logger) (err error) {
	initOnce.Do(func() {
		logger = log
		metricWriter, err = NewDefaultMetricLogWriterOfApp(metricLogSingleFileMaxSize, metricLogMaxFileAmount, appName, logDir)
		if err != nil {
			logger.Error("Failed to initialize the MetricLogWriter in aggregator.InitTask(),err=" + err.Error())
			return
		}

		// Schedule the log flushing task
		go RunWithRecover(writeTaskLoop, logger)

		// Schedule the log aggregating task
		ticker := utils.NewTicker(time.Duration(flushInterval) * time.Second)
		go RunWithRecover(func() {
			for {
				select {
				case <-ticker.C():
					doAggregate()
				case <-stopChan:
					ticker.Stop()
					return
				}
			}
		}, logger)

	})
	return err
}

func writeTaskLoop() {
	for {
		select {
		case m := <-writeChan:
			keys := make([]uint64, 0, len(m))
			for t := range m {
				keys = append(keys, t)
			}
			// Sort the time
			sort.Slice(keys, func(i, j int) bool {
				return keys[i] < keys[j]
			})
			for _, t := range keys {
				err := metricWriter.Write(t, m[t])
				if err != nil {
					logger.Error("[MetricAggregatorTask] fail tp write metric in aggregator.writeTaskLoop(),err=" + err.Error())
				}
			}
		}
	}
}

func doAggregate() {
	curTime := utils.CurrentTimeMillis()
	curTime = curTime - curTime%1000

	if int64(curTime) <= lastFetchTime {
		return
	}
	maps := make(metricTimeMap)
	aggregateIntoMap(maps, currentMetricTimeMap, curTime)

	lastFetchTime = int64(curTime)
	if len(maps) > 0 {
		writeChan <- maps
	}
}

func isItemTimestampInTime(ts uint64, currentSecStart uint64) bool {
	// The bucket should satisfy: windowStart between [lastFetchTime, curStart)
	return int64(ts) >= lastFetchTime && ts < currentSecStart
}

func aggregateIntoMap(mm metricTimeMap, mmap map[uint64][]*base.MetricItem, currentTime uint64) {
	mut.Lock()
	defer mut.Unlock()

	for ts, item := range mmap {
		if isItemTimestampInTime(ts, currentTime) {
			items, exists := mm[ts]
			if exists {
				mm[ts] = append(items, item...)
			} else {
				mm[ts] = item
			}
		}
	}
}

// AggregateIntoMap 汇聚到Map
func AggregateIntoMap(metric *base.MetricItem) {
	mut.Lock()
	defer mut.Unlock()

	items, exists := currentMetricTimeMap[metric.Timestamp]
	if exists {
		currentMetricTimeMap[metric.Timestamp] = append(items, metric)
	} else {
		currentMetricTimeMap[metric.Timestamp] = []*base.MetricItem{metric}
	}

	for t := range currentMetricTimeMap {
		if t < metric.Timestamp-maxCacheSecond {
			delete(currentMetricTimeMap, t)
		}
	}
}
