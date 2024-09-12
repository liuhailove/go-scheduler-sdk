package base

import (
	"errors"
	"fmt"
	"github.com/liuhailove/go-scheduler-sdk/utils"
	"strconv"
	"strings"
)

const metricPartSeparator = "BCC28710C96347CDA63B01E75762BA5B"

// MetricItem represents the data of metric log per line.
type MetricItem struct {
	JobId int64 `json:"jobId"`
	// LogId 日志ID
	LogId int64 `json:"logId"`
	// Timestamp 时间戳
	Timestamp uint64 `json:"timestamp"`
	// Caller 调用方
	Caller string `json:"caller"`
	// Msg 业务写入消息
	Msg string `json:"msg"`

	// 附加记录信息，是一种格式化信息
	Key1   string `json:"key1"`
	Value1 string `json:"value1"`
	Key2   string `json:"key2"`
	Value2 string `json:"value2"`
	Key3   string `json:"key3"`
	Value3 string `json:"value3"`
}

type MetricItemRetriever interface {
	MetricsOnCondition(predicate TimePredicate) []*MetricItem
}

func (m *MetricItem) ToFatString() (string, error) {
	b := strings.Builder{}
	timeStr := utils.FormatTimeMillis(m.Timestamp)
	_, err := fmt.Fprintf(&b, "%d"+metricPartSeparator+"%s"+metricPartSeparator+
		"%d"+metricPartSeparator+"%d"+metricPartSeparator+"%s"+metricPartSeparator+"%s"+metricPartSeparator+"%s"+
		metricPartSeparator+"%s"+metricPartSeparator+"%s"+metricPartSeparator+"%s"+metricPartSeparator+"%s",
		m.Timestamp, timeStr, m.JobId, m.LogId, m.Msg,
		m.Key1, m.Value1, m.Key2, m.Value2,
		m.Key3, m.Value3)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func (m *MetricItem) ToThinString() (string, error) {
	b := strings.Builder{}
	_, err := fmt.Fprintf(&b, "%d"+metricPartSeparator+
		"%d"+metricPartSeparator+"%d"+metricPartSeparator+"%s"+metricPartSeparator+"%s"+metricPartSeparator+"%s"+
		metricPartSeparator+"%s"+metricPartSeparator+"%s"+metricPartSeparator+"%s"+metricPartSeparator+"%s",
		m.Timestamp, m.JobId, m.LogId, m.Msg,
		m.Key1, m.Value1, m.Key2, m.Value2,
		m.Key3, m.Value3)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func MetricItemFromFatString(line string) (*MetricItem, error) {
	if len(line) == 0 {
		return nil, errors.New("invalid metric line: empty string")
	}
	item := &MetricItem{}
	arr := strings.Split(line, metricPartSeparator)
	if len(arr) < 11 {
		return nil, errors.New("invalid metric line: invalid format")
	}
	ts, err := strconv.ParseUint(arr[0], 10, 64)
	if err != nil {
		return nil, err
	}
	item.Timestamp = ts
	jobId, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		return nil, err
	}
	item.JobId = jobId
	logId, err := strconv.ParseInt(arr[3], 10, 64)
	if err != nil {
		return nil, err
	}
	item.LogId = logId
	item.Msg = arr[4]
	item.Key1 = arr[5]
	item.Value1 = arr[6]
	item.Key2 = arr[7]
	item.Value2 = arr[8]
	item.Key3 = arr[9]
	item.Value3 = arr[10]
	return item, nil
}
