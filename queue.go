package gs

import (
	"container/list"
	"sync"
)

type Queue struct {
	mu   sync.RWMutex
	list list.List
}

// Push 入队
func (q *Queue) Push(val interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.list.PushBack(val)
}

// PopFrontAll 全部出队
func (q *Queue) PopFrontAll() []interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	var arr []interface{}
	var next *list.Element
	for e := q.list.Front(); e != nil; e = next {
		next = e.Next()
		q.list.Remove(e)
		arr = append(arr, e.Value)
	}
	if len(arr) == 0 || arr[0] == nil {
		return nil
	}
	return arr
}
