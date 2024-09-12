package logging

import (
	"errors"
	"fmt"
	"github.com/liuhailove/go-scheduler-sdk/utils"
	"strings"
	"testing"
)

var (
	// 最近一次sendTime
	latestSendTime uint64
	// 最大拉取间隔
	myMaxLastFetchIntervalMs uint64 = 15 * 1000
	myFetchIntervalSecond    uint64 = 6 * 1000
)

func TestName(t *testing.T) {

	var err error
	searcher, err := NewDefaultMetricSearcher("/Users/honggang.liu/IdeaProjects/go-scheduler-executor-go/gslog", FormMetricFileName("my-golang-jobs-2", false))
	if err != nil {
		panic(err)
	}
	//var threeHour uint64 = 1000 * 60 * 60 * 3
	//var now = utils.CurrentTimeMillis() - threeHour
	var start = uint64(1695276000000)
	var end = uint64(1695282300000)
	for i := 0; i < 1000000; i++ {
		MySendMetric(searcher, start)
		if start > end {
			fmt.Println("break")
			break
		}
		start += 1000
	}
}

var cnt = 0

// sendMetric 向server发送metric信息
func MySendMetric(searcher MetricSearcher, now uint64) (bool, error) {
	// trim milliseconds
	var lastFetchMs = now - myMaxLastFetchIntervalMs

	// 和发送的下一秒对比
	if lastFetchMs < latestSendTime+1000*1 {
		lastFetchMs = latestSendTime + 1000*1
	}
	lastFetchMs = lastFetchMs / 1000 * 1000
	var endTime = lastFetchMs + myFetchIntervalSecond
	// 不希望跟踪的太快，因为要等文件写
	if endTime > now-4000*1 {
		// too near
		return false, nil
	}
	//if endTime >= 1695192281000 {
	//	fmt.Println("data")
	//}
	latestSendTime = endTime
	// 和当前版本对比，如果不一致，需要拉取规则变更
	var str2 = fmt.Sprintf("start:%s,end=%s,st=%d,et=%d", utils.FormatTimeMillis(lastFetchMs), utils.FormatTimeMillis(endTime), lastFetchMs/1000, endTime/1000)
	fmt.Println(str2)
	metricItemArr, err := searcher.FindByTimeAndLog(lastFetchMs, endTime, -1)
	if err != nil {
		return false, errors.New("Error when retrieving metrics" + err.Error())
	}
	if metricItemArr == nil || len(metricItemArr) == 0 {
		return true, nil
	}
	builder := strings.Builder{}
	for _, item := range metricItemArr {
		thinStr, _ := item.ToThinString()
		builder.WriteString(thinStr)
		builder.WriteString("\n")
	}
	str := fmt.Sprintf("now=%s,len,%d", utils.FormatTimeMillis(now), len(metricItemArr))
	fmt.Println(str)
	cnt = cnt + len(metricItemArr)
	totalStr := fmt.Sprintf("total:%d", cnt)
	fmt.Println(totalStr)
	return true, nil
}

func TestUitls(t *testing.T) {
	fmt.Println(utils.FormatTimeMillis(1695192275763))
}
