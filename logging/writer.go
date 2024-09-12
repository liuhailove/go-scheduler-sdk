package logging

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/liuhailove/go-scheduler-sdk/base"
	"github.com/liuhailove/go-scheduler-sdk/utils"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type DefaultMetricLogWriter struct {
	baseDir      string // 目录
	baseFilename string //文件名称

	maxSingleSize uint64 // 单文件最大size
	maxFileAmount uint32 // 最大文件数

	timezoneOffsetSec int64 //秒
	latestOpSec       int64 // 最后操作秒

	curMetricFile    *os.File // 当前metric操作的文件
	curMetricIdxFile *os.File // 当前metric所对应的idx的文件

	// 文件输出流
	metricOut *bufio.Writer
	idxOut    *bufio.Writer

	mux *sync.RWMutex

	logger Logger
}

func (d *DefaultMetricLogWriter) Write(ts uint64, items []*base.MetricItem) error {
	if len(items) == 0 {
		return nil
	}
	if ts <= 0 {
		return errors.New(fmt.Sprintf("%s: %d", "Invalid timestamp: ", ts))
	}
	if d.curMetricFile == nil || d.curMetricIdxFile == nil {
		return errors.New("file handle not initialized")
	}
	// Update all metric items to the given timestamp.
	for _, item := range items {
		item.Timestamp = ts
	}

	d.mux.Lock()
	defer d.mux.Unlock()
	timeSec := int64(ts / 1000)
	if timeSec < d.latestOpSec {
		// ignore
		return nil
	}
	if timeSec > d.latestOpSec {
		pos, err := utils.FilePosition(d.curMetricFile)
		if err != nil {
			return errors.Wrap(err, "cannot get current pos of the metric file")
		}
		if err = d.writeIndex(timeSec, pos); err != nil {
			return errors.Wrap(err, "cannot write metric idx file")
		}
		if d.isNewDay(d.latestOpSec, timeSec) {
			if err = d.rollToNextFile(ts); err != nil {
				return errors.Wrap(err, "failed to roll the metric log")
			}
		}
	}
	// Write and flush
	if err := d.writeItemsAndFlush(items); err != nil {
		return errors.Wrap(err, "failed to write and flush metric items")
	}
	if err := d.rollFileIfSizeExceeded(ts); err != nil {
		return errors.Wrap(err, "failed to pre-check the rolling condition of metric logs")
	}
	if timeSec > d.latestOpSec {
		// Update the latest timeSec.
		d.latestOpSec = timeSec
	}

	return nil
}

// Close 关闭log writer相关文件
func (d *DefaultMetricLogWriter) Close() error {
	d.mux.Lock()
	defer d.mux.Unlock()

	if d.curMetricIdxFile != nil {
		d.curMetricIdxFile.Close()
	}

	if d.curMetricFile != nil {
		return d.curMetricFile.Close()
	}
	return nil
}

func (d *DefaultMetricLogWriter) writeItemsAndFlush(items []*base.MetricItem) error {
	for _, item := range items {
		s, err := item.ToFatString()
		if err != nil {
			logger.Info("[writeItemsAndFlush] Failed to convert MetricItem to string,logId=" + strconv.FormatInt(item.LogId, 10) + ",err=" + err.Error())
			continue
		}

		// Append the LF line separator.
		bs := []byte(s + "\n")
		_, err = d.metricOut.Write(bs)
		if err != nil {
			return nil
		}
	}
	return d.metricOut.Flush()
}

// rollFileIfSizeExceeded 日志滚动，当超过单个日志最大值时，开始滚动日志
func (d *DefaultMetricLogWriter) rollFileIfSizeExceeded(time uint64) error {
	if d.curMetricFile == nil {
		return nil
	}
	stat, err := d.curMetricFile.Stat()
	if err != nil {
		return err
	}
	if uint64(stat.Size()) >= d.maxSingleSize {
		return d.rollToNextFile(time)
	}
	return nil
}

// rollToNextFile 滚动到下一个日志文件
func (d *DefaultMetricLogWriter) rollToNextFile(time uint64) error {
	newFilename, err := d.nextFileNameOfTime(time)
	if err != nil {
		return err
	}
	return d.closeCurAndNewFile(newFilename)
}

// writeIndex 写索引
func (d *DefaultMetricLogWriter) writeIndex(time, offset int64) error {
	out := d.idxOut
	if out == nil {
		return errors.New("index buffered writer not ready")
	}
	// Use BigEndian here to keep consistent with DataOutputStream in Java.
	err := binary.Write(out, binary.BigEndian, time)
	if err != nil {
		return err
	}
	err = binary.Write(out, binary.BigEndian, offset)
	if err != nil {
		return err
	}
	return out.Flush()
}

// removeDeprecatedFiles 移除过期的文件
func (d *DefaultMetricLogWriter) removeDeprecatedFiles() error {
	files, err := listMetricFiles(d.baseDir, d.baseFilename)
	if err != nil || len(files) == 0 {
		return err
	}

	amountToRemove := len(files) - int(d.maxFileAmount) + 1
	for i := 0; i < amountToRemove; i++ {
		filename := files[i]
		idxFilename := formMetricIdxFileName(filename)
		err = os.Remove(filename)
		if err != nil {
			logger.Error("Failed to remove metric log file in DefaultMetricLogWriter.removeDeprecatedFiles(),filename=" + filename + ",err=" + err.Error())
		} else {
			logger.Info("[MetricWriter] Metric log file removed in DefaultMetricLogWriter.removeDeprecatedFiles(),filename=" + filename)
		}

		err = os.Remove(idxFilename)
		if err != nil {
			logger.Error("Failed to remove metric log file in DefaultMetricLogWriter.removeDeprecatedFiles(),idxFilename=" + idxFilename + ",err=" + err.Error())
		} else {
			logger.Info("[MetricWriter] Metric index file removed,idxFilename=" + idxFilename)
		}
	}
	return err
}

// nextFileNameOfTime 根据时间戳，计算下一个文件名称
func (d *DefaultMetricLogWriter) nextFileNameOfTime(time uint64) (string, error) {
	dateStr := utils.FormatDate(time)
	filePattern := d.baseFilename + "." + dateStr
	list, err := listMetricFilesConditional(d.baseDir, filePattern, func(fn string, p string) bool {
		return strings.Contains(fn, p)
	})
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return filepath.Join(d.baseDir, filePattern), nil
	}
	last := list[len(list)-1]
	var n uint32 = 0
	items := strings.Split(last, ".")
	if len(items) > 0 {
		v, err := strconv.ParseUint(items[len(items)-1], 10, 32)
		if err == nil {
			n = uint32(v)
		}
	}
	return filepath.Join(d.baseDir, fmt.Sprintf("%s.%d", filePattern, n+1)), nil
}

// closeCurAndNewFile 关闭当前文件并新建文件
func (d *DefaultMetricLogWriter) closeCurAndNewFile(filename string) error {
	err := d.removeDeprecatedFiles()
	if err != nil {
		return err
	}

	if d.curMetricFile != nil {
		if err = d.curMetricFile.Close(); err != nil {
			logger.Error("Failed to close metric log file in DefaultMetricLogWriter.closeCurAndNewFile(),curMetricFile=" + d.curMetricFile.Name() + ",err=" + err.Error())
		}
	}
	if d.curMetricIdxFile != nil {
		if err = d.curMetricIdxFile.Close(); err != nil {
			logger.Error("Failed to close metric index file in DefaultMetricLogWriter.closeCurAndNewFile(),curMetricIdxFile=" + d.curMetricIdxFile.Name() + ",err=" + err.Error())
		}
	}
	// Create new metric log file, whether it exists or not.
	mf, err := os.Create(filename)
	if err != nil {
		return err
	}
	logger.Info("[MetricWriter] New metric log file created,filename=" + filename)
	idxFile := formMetricIdxFileName(filename)
	mif, err := os.Create(idxFile)
	if err != nil {
		return err
	}
	logger.Info("[MetricWriter] New metric log index file created,idxFile=" + idxFile)

	d.curMetricFile = mf
	d.metricOut = bufio.NewWriter(mf)

	d.curMetricIdxFile = mif
	d.idxOut = bufio.NewWriter(mif)

	return nil
}

func (d *DefaultMetricLogWriter) initialize(log Logger) error {
	logger = log
	// Create the dir if not exists.
	err := utils.CreateDirIfNotExists(d.baseDir)
	if err != nil {
		return err
	}
	if d.curMetricFile != nil {
		return nil
	}
	ts := utils.CurrentTimeMillis()
	if err := d.rollToNextFile(ts); err != nil {
		return errors.Wrap(err, "failed to initialize metric log writer")
	}
	d.latestOpSec = int64(ts / 1000)
	return nil
}

func (d *DefaultMetricLogWriter) isNewDay(lastSec, sec int64) bool {
	prevDayTs := (lastSec + d.timezoneOffsetSec) / 86400
	newDayTs := (sec + d.timezoneOffsetSec) / 86400
	return newDayTs > prevDayTs
}

func NewDefaultMetricLogWriterOfApp(maxSize uint64, maxFileAmount uint32, appName, logDir string) (MetricLogWriter, error) {
	if maxSize == 0 || maxFileAmount == 0 {
		return nil, errors.New("invalid maxSize or maxFileAmount")
	}
	_, offset := utils.Now().Zone()

	baseDir := logDir
	baseFileName := FormMetricFileName(appName, false)

	writer := &DefaultMetricLogWriter{
		baseDir:           baseDir,
		baseFilename:      baseFileName,
		maxSingleSize:     maxSize,
		maxFileAmount:     maxFileAmount,
		timezoneOffsetSec: int64(offset),
		latestOpSec:       0,
		mux:               new(sync.RWMutex),
	}
	err := writer.initialize(logger)
	return writer, err
}
