package godelayqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// WHEEL_SIZE The time length of the round is currently set to one hour, that is,
	// it takes 1 hour for each cycle of the time wheel;
	// the minimum granularity of each step on the default time wheel is 1 second.
	WHEEL_SIZE                      = 3600
	REFRESH_POINTER_DEFAULT_SECONDS = 5
)

// BuildExecutor factory method
type BuildExecutor func(taskType string) Executor

type SlotRecorder map[string]int

type ActionEvent func()

var onceNew sync.Once
var onceStart sync.Once

var mutex = &sync.RWMutex{}

var delayQueueInstance *delayQueue

type wheel struct {
	// all tasks of the time wheel saved in linked table
	NotifyTasks *Task
}

type delayQueue struct {
	// circular queue
	TimeWheel    [WHEEL_SIZE]wheel
	CurrentIndex uint // time wheel current pointer
	Persistence
	// task executor
	TaskExecutor BuildExecutor

	TaskQueryTable SlotRecorder
}

// GetDelayQueue singleton method use redis as persistence layer
func GetDelayQueue(serviceBuilder BuildExecutor) *delayQueue {
	onceNew.Do(func() {
		delayQueueInstance = &delayQueue{
			Persistence:    &taskDoNothingDb{},
			TaskExecutor:   serviceBuilder,
			TaskQueryTable: make(SlotRecorder),
		}
	})
	return delayQueueInstance
}

// GetDelayQueueWithPersis singleton method use other persistence layer
func GetDelayQueueWithPersis(serviceBuilder BuildExecutor, persistence Persistence) *delayQueue {
	if persistence == nil {
		log.Fatalf("persistance is null")
	}
	onceNew.Do(func() {
		delayQueueInstance = &delayQueue{
			Persistence:    persistence,
			TaskExecutor:   serviceBuilder,
			TaskQueryTable: make(SlotRecorder),
		}
	})
	return delayQueueInstance
}

func (dq *delayQueue) Start() {
	// ensure only one time wheel has been created
	onceStart.Do(dq.init)
}

func (dq *delayQueue) init() {
	// load task from cache
	dq.loadTasksFromDb()

	// update pointer
	dq.CurrentIndex = uint(dq.Persistence.GetWheelTimePointer())

	// start time wheel
	go func() {
		for {
			select {
			case <-time.After(time.Second * 1):
				if dq.CurrentIndex >= WHEEL_SIZE {
					dq.CurrentIndex = dq.CurrentIndex % WHEEL_SIZE
				}
				taskLinkHead := dq.TimeWheel[dq.CurrentIndex].NotifyTasks
				headIndex := dq.CurrentIndex

				dq.CurrentIndex++

				// fetch linked list
				prev := taskLinkHead
				p := taskLinkHead
				for p != nil {
					if p.CycleCount == 0 {
						taskId := p.Id
						// Open a new go routing for notifications, speed up each traversal,
						// and ensure that the time wheel will not be slowed down
						// If there is an exception in the task, try to let the specific business object handle it,
						// and the delay queue does not handle the specific business exception.
						// This can ensure the business simplicity of the delay queue and avoid problems that are difficult to maintain.
						// If there is a problem with a specific business and you need to be notified repeatedly,
						// you can add the task back to the queue.
						go dq.ExecuteTask(p.TaskType, p.TaskParams)
						// delete task
						// if the first node
						if prev == p {
							dq.TimeWheel[headIndex].NotifyTasks = p.Next
							prev = p.Next
							p = p.Next
						} else {
							// if it is not the first node
							prev.Next = p.Next
							p = p.Next
						}
						// remove the task from the persistent object
						dq.Persistence.Delete(taskId)
						// remove task from query table
						mutex.Lock()
						delete(dq.TaskQueryTable, taskId)
						mutex.Unlock()
					} else {
						p.CycleCount--
						prev = p
						p = p.Next
					}
				}
			}
		}
	}()

	// async to update timewheel pointer
	go func() {
		// refresh pinter internal seconds
		refreshInternal, _ := strconv.Atoi(GetEvnWithDefaultVal("REFRESH_POINTER_INTERNAL", fmt.Sprintf("%d", REFRESH_POINTER_DEFAULT_SECONDS)))
		if refreshInternal < REFRESH_POINTER_DEFAULT_SECONDS {
			refreshInternal = REFRESH_POINTER_DEFAULT_SECONDS
		}
		for {
			select {
			case <-time.After(time.Second * REFRESH_POINTER_DEFAULT_SECONDS):
				err := dq.Persistence.SaveWheelTimePointer(int(dq.CurrentIndex))
				log.Println(err)
			}
		}

	}()
}

func (dq *delayQueue) loadTasksFromDb() {
	tasks := dq.Persistence.GetList()
	if tasks != nil && len(tasks) > 0 {
		for _, task := range tasks {
			delaySeconds := (task.CycleCount * WHEEL_SIZE) + task.WheelPosition
			if delaySeconds > 0 {
				tk, _ := dq.internalPush(time.Duration(delaySeconds)*time.Second, task.Id, task.TaskType, task.TaskParams, false)
				if tk != nil {
					dq.TaskQueryTable[task.Id] = task.WheelPosition
				}
			}
		}
	}
}

// Push Add a task to the delay queue
func (dq *delayQueue) Push(delaySeconds time.Duration, taskType string, taskParams interface{}) (task *Task, err error) {
	var pms string
	result, ok := taskParams.(string)
	if !ok {
		tp, _ := json.Marshal(taskParams)
		pms = string(tp)
	} else {
		pms = result
	}

	task, err = dq.internalPush(delaySeconds, "", taskType, pms, true)
	if err == nil {
		mutex.Lock()
		dq.TaskQueryTable[task.Id] = task.WheelPosition
		mutex.Unlock()
	}

	return
}

func (dq *delayQueue) internalPush(delaySeconds time.Duration, taskId string, taskType string, taskParams string, needPresis bool) (*Task, error) {
	if int(delaySeconds.Seconds()) == 0 {
		errorMsg := fmt.Sprintf("the delay time cannot be less than 1 second, current is: %v", delaySeconds)
		return nil, errors.New(errorMsg)
	}

	// Start timing from the current time pointer
	seconds := int(delaySeconds.Seconds())
	calculateValue := int(dq.CurrentIndex) + seconds

	cycle := calculateValue / WHEEL_SIZE
	index := calculateValue % WHEEL_SIZE

	if taskId == "" {
		u := uuid.New()
		taskId = u.String()
	}
	task := &Task{
		Id:            taskId,
		CycleCount:    cycle,
		WheelPosition: index,
		TaskType:      taskType,
		TaskParams:    taskParams,
	}

	if cycle > 0 && index <= int(dq.CurrentIndex) {
		cycle--
		task.CycleCount = cycle
	}

	mutex.Lock()
	if dq.TimeWheel[index].NotifyTasks == nil {
		dq.TimeWheel[index].NotifyTasks = task
	} else {
		// Insert a new task into the head of the linked list.
		// Since there is no order relationship between tasks,
		// this implementation is the easiest
		head := dq.TimeWheel[index].NotifyTasks
		task.Next = head
		dq.TimeWheel[index].NotifyTasks = task
	}
	mutex.Unlock()

	if needPresis {
		dq.Persistence.Save(task)
	}

	return task, nil
}

// ExecuteTask execute task
func (dq *delayQueue) ExecuteTask(taskType, taskParams string) error {
	if dq.TaskExecutor != nil {
		executor := dq.TaskExecutor(taskType)
		if executor != nil {
			log.Printf("Execute task: %s with params: %s\n", taskType, taskParams)

			return executor.DoDelayTask(taskParams)
		} else {
			return errors.New("executor is nil")
		}
	} else {
		return errors.New("task build executor is nil")
	}

}

// WheelTaskQuantity Get the number of tasks on a time wheel
func (dq *delayQueue) WheelTaskQuantity(index int) int {
	tasks := dq.TimeWheel[index].NotifyTasks
	if tasks == nil {
		return 0
	}
	k := 0
	for p := tasks; p != nil; p = p.Next {
		k++
	}

	return k
}

func (dq *delayQueue) GetTask(taskId string) *Task {
	mutex.Lock()
	val, ok := dq.TaskQueryTable[taskId]
	mutex.Unlock()
	if !ok {
		return nil
	} else {
		tasks := dq.TimeWheel[val].NotifyTasks
		for p := tasks; p != nil; p = p.Next {
			if p.Id == taskId {
				return p
			}
		}
		return nil
	}
}

func (dq *delayQueue) UpdateTask(taskId, taskType, taskParams string) error {
	task := dq.GetTask(taskId)
	if task == nil {
		return errors.New("task not found")
	}
	task.TaskType = taskType
	task.TaskParams = taskParams

	// update cache
	dq.Persistence.Save(task)

	return nil
}

func (dq *delayQueue) DeleteTask(taskId string) error {
	mutex.Lock()
	defer mutex.Unlock()
	val, ok := dq.TaskQueryTable[taskId]
	if !ok {
		return errors.New("task not found")
	} else {
		p := dq.TimeWheel[val].NotifyTasks
		prev := p
		for p != nil {
			if p.Id == taskId {
				// if current node is root node
				if p == prev {
					dq.TimeWheel[val].NotifyTasks = p.Next
				} else {
					prev.Next = p.Next
				}
				// clear cache
				delete(dq.TaskQueryTable, taskId)
				dq.Persistence.Delete(taskId)
				p = nil
				prev = nil

				break
			} else {
				prev = p
				p = p.Next
			}
		}
		return nil
	}
}

func (dq *delayQueue) RemoveAllTasks() error {
	dq.TaskQueryTable = make(SlotRecorder)
	for i := 0; i < len(dq.TimeWheel); i++ {
		dq.TimeWheel[i].NotifyTasks = nil
	}
	dq.Persistence.RemoveAll()
	return nil
}
