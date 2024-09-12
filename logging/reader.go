package logging

import (
	"bufio"
	"fmt"
	"github.com/liuhailove/go-scheduler-sdk/base"
	"github.com/liuhailove/gretry/logging"
	"github.com/pkg/errors"
	"io"
	"os"
)

const maxItemAmount = 100000

// MetricLogReader metric 日志读接口
type MetricLogReader interface {
	// ReadMetrics 读取metrics
	// nameList 文件列表
	// fileNo 编号
	// startOffset 偏移量
	// maxLines 最大行
	ReadMetrics(nameList []string, fileNo uint32, startOffset uint64, maxLines uint32) ([]*base.MetricItem, error)

	// ReadMetricsByEndTime 读取metrics
	// nameList 文件列表
	// fileNo 编号
	// startOffset 偏移量
	// maxLines 最大行
	// beginMs，endMs 开始截至时间
	// logId 日志ID
	ReadMetricsByEndTime(nameList []string, fileNo uint32, startOffset uint64, beginMs, endMs uint64, logId int64) ([]*base.MetricItem, error)
}

// Not thread-safe itself, but guarded by the outside MetricSearcher.
type defaultMetricLogReader struct {
}

func (d *defaultMetricLogReader) ReadMetrics(nameList []string, fileNo uint32, startOffset uint64, maxLines uint32) ([]*base.MetricItem, error) {
	if len(nameList) == 0 {
		return make([]*base.MetricItem, 0), nil
	}
	// startOffset: the offset of the first file to read
	items, shouldContinue, err := d.readMetricsInOneFile(nameList[fileNo], startOffset, maxLines, 0, 0)
	if err != nil {
		return nil, err
	}
	if !shouldContinue {
		return items, nil
	}
	fileNo++
	// Continue reading until the size or time does not satisfy the condition
	for {
		if int(fileNo) >= len(nameList) || len(items) >= int(maxLines) {
			// No files to read.
			break
		}
		arr, shouldContinue, err := d.readMetricsInOneFile(nameList[fileNo], 0, maxLines, getLatestSecond(items), uint32(len(items)))
		if err != nil {
			return nil, err
		}
		items = append(items, arr...)
		if !shouldContinue {
			break
		}
		fileNo++
	}
	return items, nil
}

func (d *defaultMetricLogReader) ReadMetricsByEndTime(nameList []string, fileNo uint32, startOffset uint64, beginMs, endMs uint64, logId int64) ([]*base.MetricItem, error) {
	if len(nameList) == 0 {
		return make([]*base.MetricItem, 0), nil
	}
	// startOffset: the offset of the first file to read
	items, shouldContinue, err := d.readMetricsInOneFileByEndTime(nameList[fileNo], startOffset, beginMs, endMs, logId, 0)
	if err != nil {
		return nil, err
	}
	if !shouldContinue {
		return items, nil
	}
	fileNo++
	// Continue reading until the size or time does not satisfy the condition
	for {
		if int(fileNo) >= len(nameList) {
			// No files to read.
			break
		}
		arr, shouldContinue, err := d.readMetricsInOneFileByEndTime(nameList[fileNo], 0, beginMs, endMs, logId, uint32(len(items)))
		if err != nil {
			return nil, err
		}
		items = append(items, arr...)
		if !shouldContinue {
			break
		}
		fileNo++
	}
	return items, nil
}

// readMetricsInOneFile 从文件中读取metrics
// filename 文件名称
// offset 偏移量
// maxLines 最大行
// lastSec 最近
// prevSize 已经读取的size
func (d *defaultMetricLogReader) readMetricsInOneFile(filename string, offset uint64, maxLines uint32, lastSec uint64, prevSize uint32) ([]*base.MetricItem, bool, error) {
	file, err := openFileAndSeekTo(filename, offset)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	bufReader := bufio.NewReaderSize(file, 8192)
	items := make([]*base.MetricItem, 0, 1024)
	for {
		line, err := readLine(bufReader)
		if err != nil {
			if err == io.EOF {
				return items, true, nil
			}
			return nil, false, errors.Wrap(err, "error when reading lines from file")
		}
		item, err := base.MetricItemFromFatString(line)
		if err != nil {
			logger.Error("Failed to convert MetricItem to string in defaultMetricLogReader.readMetricsInOneFile(),err=" + err.Error())
			continue
		}
		tsSec := item.Timestamp / 1000

		if prevSize+uint32(len(items)) >= maxLines && tsSec != lastSec {
			return items, false, nil
		}
		items = append(items, item)
		lastSec = tsSec
	}
}

// readMetricsInOneFileByEndTime 在文件中根据截至时间读取metric
// filename 文件名称
// offset 偏移量
// beginMs，endMs 开始、截至时间
// logId 日志ID
// prevSize 已经读取的size
func (d *defaultMetricLogReader) readMetricsInOneFileByEndTime(filename string, offset uint64, beginMs, endMs uint64, logId int64, prevSize uint32) ([]*base.MetricItem, bool, error) {
	beginSec := beginMs / 1000
	endSec := endMs / 1000
	file, err := openFileAndSeekTo(filename, offset)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	bufReader := bufio.NewReaderSize(file, 8192)
	items := make([]*base.MetricItem, 0, 1024)
	for {
		line, err := readLine(bufReader)
		if err != nil {
			if err == io.EOF {
				return items, true, nil
			}
			return nil, false, errors.Wrap(err, "error when reading lines from file")
		}
		item, err := base.MetricItemFromFatString(line)
		if err != nil {
			logging.Error(err, "Invalid line of metric file in defaultMetricLogReader.readMetricsInOneFileByEndTime()", "fileLine", line)
			continue
		}
		tsSec := item.Timestamp / 1000
		// currentSecond should in [beginSec, endSec]
		if tsSec < beginSec || tsSec > endSec {
			return items, false, nil
		}

		// empty resource name indicates "fetch all"
		if logId <= 0 || logId == item.LogId {
			items = append(items, item)
		}
		// Max items limit to avoid infinite reading
		if len(items)+int(prevSize) >= maxItemAmount {
			logging.Warn(fmt.Sprintf("Exceed Max items, real limes=%d, maxItem=%d", len(items)+int(prevSize), maxItemAmount))
			return items, false, nil
		}
	}
}

// readLine 读取行
func readLine(bufReader *bufio.Reader) (string, error) {
	buf := make([]byte, 0, 256)
	for {
		line, ne, err := bufReader.ReadLine()
		if err != nil {
			return "", err
		}
		buf = append(buf, line...)
		if !ne {
			return string(buf), err
		}
		// buffer size < line size, so we need to read until the `ne` flag is false.
	}
}

// getLatestSecond 获取最近的item秒
func getLatestSecond(items []*base.MetricItem) uint64 {
	if items == nil || len(items) == 0 {
		return 0
	}
	return items[len(items)-1].Timestamp / 1000
}

// openFileAndSeekTo 打开文件并定位到offset
func openFileAndSeekTo(filename string, offset uint64) (*os.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open file: "+filename)
	}

	// Set position to the offset recorded in the idx file
	_, err = file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "failed to fseek to offset %d", offset)
	}
	return file, nil
}

func newDefaultMetricLogReader() MetricLogReader {
	return &defaultMetricLogReader{}
}
