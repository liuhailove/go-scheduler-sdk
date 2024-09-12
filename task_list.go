package gs

import (
	"strings"
	"sync"
)

// 任务列表 [JobID]执行函数,并行执行时[+LogID]
type taskList struct {
	data sync.Map
}

// Set 设置数据
func (t *taskList) Set(key string, val *Task) {
	t.data.Store(key, val)
}

// NoExistAndSet 不存在则设置
func (t *taskList) NoExistAndSet(key string, val *Task) bool {
	_, loaded := t.data.LoadOrStore(key, val)
	return !loaded
}

// Get 获取数据
func (t *taskList) Get(key string) *Task {
	val, ok := t.data.Load(key)
	if ok {
		return val.(*Task)
	}
	return nil
}

// GetPre 获取数据
func (t *taskList) GetPre(key string) []*Task {
	tasks := make([]*Task, 0)
	t.data.Range(func(k, value interface{}) bool {
		if key == strings.Split(k.(string), ":")[0] {
			tasks = append(tasks, value.(*Task))
		}
		return true
	})
	return tasks
}

// GetAll 获取数据
func (t *taskList) GetAll() map[string]*Task {
	var taskMap = make(map[string]*Task, 0)
	t.data.Range(func(key, value interface{}) bool {
		taskMap[key.(string)] = value.(*Task)
		return true
	})
	return taskMap
}

// Del 设置数据
func (t *taskList) Del(key string) {
	t.data.Delete(key)
}

// DelPre 删除数据
func (t *taskList) DelPre(key string) {
	t.data.Range(func(k, value interface{}) bool {
		if key == strings.Split(k.(string), ":")[0] {
			t.data.Delete(k)
		}
		return true
	})
}

// Exists Key是否存在
func (t *taskList) Exists(key string) bool {
	var exist = false
	t.data.Range(func(k, value interface{}) bool {
		if k.(string) == key {
			exist = true
			return false
		}
		return true
	})
	return exist
}

// ExistPre Key是否存在
func (t *taskList) ExistPre(key string) bool {
	var exist = false
	t.data.Range(func(k, value interface{}) bool {
		if key == strings.Split(k.(string), ":")[0] {
			exist = true
			return false
		}
		return true
	})
	return exist
}

// ExistAndDel 存在就删除
func (t *taskList) ExistAndDel(key string) bool {
	var loaded = false
	t.data.Range(func(k, value interface{}) bool {
		if k == key {
			t.data.Delete(key)
			loaded = true
			return false
		}
		return true
	})
	return loaded
}

func (t *taskList) Range(fn func(data interface{}) bool) {
	t.data.Range(func(key, value interface{}) bool {
		return fn(value)
	})
}

func (t *taskList) Empty() bool {
	var empty = true
	t.data.Range(func(key, value interface{}) bool {
		empty = false
		return false
	})
	return empty
}
