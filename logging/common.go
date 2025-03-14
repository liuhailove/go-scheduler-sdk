package logging

import (
	"github.com/liuhailove/go-scheduler-sdk/base"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	// MetricFileNameSuffix metric文件后缀.
	MetricFileNameSuffix = "metrics.log"
	// MetricIdxSuffix metric index文件后缀.
	MetricIdxSuffix = ".idx"
	// FileLockSuffix 文件锁后缀.
	FileLockSuffix = ".lck"
	// FilePidPrefix represents the pid flag of filename.
	FilePidPrefix = "pid"

	metricFilePattern = `\.[0-9]{4}-[0-9]{2}-[0-9]{2}(\.[0-9]*)?`
)

var metricFileRegex = regexp.MustCompile(metricFilePattern)

// MetricLogWriter 写入并且刷metric item到metric log
type MetricLogWriter interface {
	Write(ts uint64, items []*base.MetricItem) error
}

// MetricSearcher searches metric items from the metric log file under given condition.
type MetricSearcher interface {
	// FindByTimeAndLog 根据时间范围和logId查询metric
	FindByTimeAndLog(beginTimeMs, endTimMs uint64, logId int64) ([]*base.MetricItem, error)

	// FindFromTimeWithMaxLines 根据时间范围和最大行查询
	FindFromTimeWithMaxLines(beginTimeMs uint64, maxLines uint32) ([]*base.MetricItem, error)
}

// FormMetricFileName Generate the metric file name from the service name.
func FormMetricFileName(serviceName string, withPid bool) string {
	dot := "."
	separator := "-"
	if strings.Contains(serviceName, dot) {
		serviceName = strings.ReplaceAll(serviceName, dot, separator)
	}
	filename := serviceName + separator + MetricFileNameSuffix
	if withPid {
		pid := os.Getpid()
		filename = filename + ".pid" + strconv.Itoa(pid)
	}
	return filename
}

// Generate the metric index filename from the metric log filename.
func formMetricIdxFileName(metricFilename string) string {
	return metricFilename + MetricIdxSuffix
}

// filenameMatches 文件名匹配
func filenameMatches(filename, baseFilename string) bool {
	if !strings.HasPrefix(filename, baseFilename) {
		return false
	}
	part := filename[len(baseFilename):]
	// part is like: ".yyyy-MM-dd.number", eg. ".2018-12-24.11"
	return metricFileRegex.MatchString(part)
}

func listMetricFilesConditional(baseDir string, filePattern string, predicate func(string, string) bool) ([]string, error) {
	dir, err := ioutil.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	arr := make([]string, 0, len(dir))
	for _, f := range dir {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if predicate(name, filePattern) && !strings.HasSuffix(name, MetricIdxSuffix) && !strings.HasPrefix(name, FileLockSuffix) {
			// Put the absolute path into the slice.
			arr = append(arr, filepath.Join(baseDir, name))
		}
	}
	if len(arr) > 1 {
		sort.Slice(arr, filenameComparator(arr))
	}
	return arr, nil
}

// List metrics files
// baseDir: the directory of metrics files
// filePattern: metric file pattern
func listMetricFiles(baseDir, filePattern string) ([]string, error) {
	return listMetricFilesConditional(baseDir, filePattern, filenameMatches)
}

func filenameComparator(arr []string) func(i, j int) bool {
	return func(i, j int) bool {
		name1 := filepath.Base(arr[i])
		name2 := filepath.Base(arr[j])

		a1 := strings.Split(name1, `.`)
		a2 := strings.Split(name2, `.`)

		dateStr1 := a1[2]
		dateStr2 := a2[2]

		// in case of file name contains pid, skip it, like gs-Admin-metrics.log.pid22568.2018-12-24
		if strings.HasPrefix(a1[2], FilePidPrefix) {
			dateStr1 = a1[3]
			dateStr2 = a2[3]
		}

		// compare date first
		if dateStr1 != dateStr2 {
			return dateStr1 < dateStr2
		}

		// same date, compare the file number
		t := len(name1) - len(name2)
		if t != 0 {
			return t < 0
		}
		return name1 < name2
	}
}
