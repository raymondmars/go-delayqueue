package godelayqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// The time length of the round is currently set to one hour, that is,
	// it takes 1 hour for each cycle of the time wheel;
	// the minimum granularity of each step on the default time wheel is 1 second.
	WHEEL_SIZE = 3600
)

// factory method
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

// singleton method use redis as persistence layer
func GetDelayQueue(serviceBuilder BuildExecutor) *delayQueue {
	onceNew.Do(func() {
		delayQueueInstance = &delayQueue{
			Persistence:    getRedisDb(),
			TaskExecutor:   serviceBuilder,
			TaskQueryTable: make(map[string]int),
		}
	})
	return delayQueueInstance
}

// singleton method use other persistence layer
func GetDelayQueueWithPersis(serviceBuilder BuildExecutor, persistence Persistence) *delayQueue {
	if persistence == nil {
		log.Fatalf("persistance is null")
	}
	onceNew.Do(func() {
		delayQueueInstance = &delayQueue{
			Persistence:    persistence,
			TaskExecutor:   serviceBuilder,
			TaskQueryTable: make(map[string]int),
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
						delete(dq.TaskQueryTable, taskId)

					} else {
						p.CycleCount--
						prev = p
						p = p.Next
					}
				}
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

// Add a task to the delay queue
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
	if needPresis {
		dq.Persistence.Save(task)
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

	return task, nil
}

// execute task
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

// Get the number of tasks on a time wheel
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
	if val, ok := dq.TaskQueryTable[taskId]; !ok {
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
