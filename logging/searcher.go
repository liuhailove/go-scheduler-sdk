package logging

import (
	"encoding/binary"
	"github.com/liuhailove/go-scheduler-sdk/base"
	"github.com/liuhailove/go-scheduler-sdk/utils"
	"github.com/pkg/errors"
	"io"
	"os"
	"strconv"
	"sync"
)

const (
	offsetNotFound = -1
)

type DefaultMetricSearcher struct {
	// metric文件读取
	reader MetricLogReader

	// 目录
	baseDir string
	// 文件名称
	baseFilename string

	// 文件位置
	cachedPos *filePosition

	mux *sync.Mutex
}

type filePosition struct {
	metricFilename string
	idxFilename    string

	curOffsetInIdx uint64
	curSecInIdx    uint64
}

// FindByTimeAndLog 按照时间和日志ID查询metric
func (d *DefaultMetricSearcher) FindByTimeAndLog(beginTimeMs, endTimeMs uint64, logId int64) ([]*base.MetricItem, error) {
	return d.searchOffsetAndRead(beginTimeMs, func(filenames []string, fileNo uint32, offset uint64) (items []*base.MetricItem, err error) {
		return d.reader.ReadMetricsByEndTime(filenames, fileNo, offset, beginTimeMs, endTimeMs, logId)
	})
}

func (d *DefaultMetricSearcher) FindFromTimeWithMaxLines(beginTimeMs uint64, maxLines uint32) ([]*base.MetricItem, error) {
	return d.searchOffsetAndRead(beginTimeMs, func(filenames []string, fileNo uint32, offset uint64) (items []*base.MetricItem, err error) {
		return d.reader.ReadMetrics(filenames, fileNo, offset, maxLines)
	})
}

func (d *DefaultMetricSearcher) searchOffsetAndRead(beginTimeMs uint64, doRead func([]string, uint32, uint64) ([]*base.MetricItem, error)) ([]*base.MetricItem, error) {
	filenames, err := listMetricFiles(d.baseDir, d.baseFilename)
	if err != nil {
		return nil, err
	}
	// Try to position the latest file index and offset from the cache (fast-path).
	// If cache is not up-to-date, we'll read from the initial position (offset 0 of the first file).
	//offsetStart, fileNo, metricFileName, err := d.getOffsetStartAndFileIdx(filenames, beginTimeMs)
	offsetStart, fileNo, _, err := uint64(0), uint32(0), "", nil
	if err != nil {
		logger.Info("[searchOffsetAndRead] Failed to getOffsetStartAndFileIdx,beginTimeMs=" + strconv.FormatInt(int64(beginTimeMs), 10) + ",err=" + err.Error())
	}
	fileAmount := uint32(len(filenames))
	for i := fileNo; i < fileAmount; i++ {
		filename := filenames[i]
		// Retrieve the start offset that is valid for given condition.
		// If offset = -1, it indicates that current file (i) does not satisfy the condition.
		var offset int64
		//if metricFileName == filename {
		//	offset, err = d.findOffsetToStart(filename, beginTimeMs, offsetStart)
		//} else {
		//	offset, err = d.findOffsetToStart(filename, beginTimeMs, 0)
		//}
		offset, err = d.findOffsetToStart(filename, beginTimeMs, offsetStart)
		if err != nil {
			logger.Info("[searchOffsetAndRead] Failed to findOffsetToStart, will try next file,beginTimeMs=" + strconv.FormatInt(int64(beginTimeMs), 10) + ",filename=" + filename + ",offsetStart=" + strconv.FormatInt(int64(offsetStart), 10) + ",err=" + err.Error())
			continue
		}
		if offset >= 0 {
			// 从当前文件中读取metric
			return doRead(filenames, i, uint64(offset))
		}
	}
	return make([]*base.MetricItem, 0), nil
}

// getOffsetStartAndFileIdx 查找偏移量的起点已经文件号
func (d *DefaultMetricSearcher) getOffsetStartAndFileIdx(filenames []string, beginTimeMs uint64) (offsetInIdx uint64, i uint32, metricFilename string, err error) {
	cacheOk, err := d.isPositionInTimeFor(beginTimeMs)
	if err != nil {
		return
	}
	if cacheOk {
		for j, v := range filenames {
			if v == d.cachedPos.metricFilename {
				i = uint32(j)
				offsetInIdx = d.cachedPos.curOffsetInIdx
				metricFilename = v
				break
			}
		}
	}
	return
}

// findOffsetToStart 找到文件的开始offset
func (d *DefaultMetricSearcher) findOffsetToStart(filename string, beginTimeMs uint64, lastPos uint64) (int64, error) {
	d.cachedPos.idxFilename = ""
	d.cachedPos.metricFilename = ""

	idxFilename := formMetricIdxFileName(filename)
	if _, err := os.Stat(idxFilename); err != nil {
		return 0, err
	}
	beginSec := beginTimeMs / 1000
	file, err := os.Open(idxFilename)
	if err != nil {
		return 0, errors.Wrap(err, "failed to open metric idx file: "+idxFilename)
	}
	defer file.Close()

	// Set position to the offset recorded in the idx file
	_, err = file.Seek(int64(lastPos), io.SeekStart)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to fseek idx to offset %d", lastPos)
	}
	curPos, err := utils.FilePosition(file)
	if err != nil {
		return 0, nil
	}
	d.cachedPos.curOffsetInIdx = uint64(curPos)
	var sec uint64 = 0
	var offset int64 = 0
	for {
		err = binary.Read(file, binary.BigEndian, &sec)
		if err != nil {
			if err == io.EOF {
				// EOF but offset hasn't been found yet, which indicates the expected position is not in current file
				return offsetNotFound, nil
			}
			return 0, err
		}
		if sec >= beginSec {
			break
		}
		err = binary.Read(file, binary.BigEndian, &offset)
		if err != nil {
			return 0, err
		}
		curPos, err := utils.FilePosition(file)
		if err != nil {
			return 0, nil
		}
		d.cachedPos.curOffsetInIdx = uint64(curPos)
	}
	err = binary.Read(file, binary.BigEndian, &offset)
	if err != nil {
		return 0, err
	}
	// Cache the idx filename and position
	d.cachedPos.metricFilename = filename
	d.cachedPos.idxFilename = idxFilename
	d.cachedPos.curSecInIdx = sec

	return offset, nil
}

// isPositionInTimeFor 查看开始时间是否在被缓存的位置中
func (d *DefaultMetricSearcher) isPositionInTimeFor(beginTimeMs uint64) (bool, error) {
	if beginTimeMs/1000 < d.cachedPos.curSecInIdx {
		return false, nil
	}
	idxFilename := d.cachedPos.idxFilename
	if idxFilename == "" {
		return false, nil
	}
	if _, err := os.Stat(idxFilename); err != nil {
		return false, err
	}
	idxFile, err := openFileAndSeekTo(idxFilename, d.cachedPos.curOffsetInIdx)
	if err != nil {
		return false, err
	}
	defer idxFile.Close()
	var sec uint64
	err = binary.Read(idxFile, binary.BigEndian, &sec)
	if err != nil {
		return false, err
	}
	return sec == d.cachedPos.curSecInIdx, nil
}

func NewDefaultMetricSearcher(baseDir, baseFilename string) (MetricSearcher, error) {
	if baseDir == "" {
		return nil, errors.New("empty base directory")
	}
	if baseFilename == "" {
		return nil, errors.New("empty base filename pattern")
	}
	if baseDir[len(baseDir)-1] != os.PathSeparator {
		baseDir = baseDir + string(os.PathSeparator)
	}
	reader := newDefaultMetricLogReader()
	return &DefaultMetricSearcher{
		reader:       reader,
		baseDir:      baseDir,
		baseFilename: baseFilename,
		cachedPos:    &filePosition{},
		mux:          new(sync.Mutex),
	}, nil
}
