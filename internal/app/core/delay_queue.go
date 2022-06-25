package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/raymondmars/go-delayqueue/internal/app/notify"
	"github.com/raymondmars/go-delayqueue/internal/pkg/common"
)

const (
	// The time length of the round is currently set to one hour, that is,
	// it takes 1 hour for each cycle of the time wheel;
	// the minimum granularity of each step on the default time wheel is 1 second.
	WHEEL_SIZE                      = 3600
	REFRESH_POINTER_DEFAULT_SECONDS = 5
)

// factory method
type BuildExecutor func(taskMode notify.NotifyMode) notify.Executor

type SlotRecorder map[string]int

type ActionEvent func()

var onceNew sync.Once
var onceStart sync.Once

var mutex = &sync.RWMutex{}

var delayQueueInstance *DelayQueue

type wheel struct {
	// all tasks of the time wheel saved in linked table
	NotifyTasks *Task
}

type DelayQueue struct {
	// circular queue
	TimeWheel    [WHEEL_SIZE]wheel
	CurrentIndex uint // time wheel current pointer
	Persistence
	// task executor
	TaskExecutor BuildExecutor

	TaskQueryTable SlotRecorder
	// ready flag
	IsReady bool
}

// singleton method use redis as persistence layer
func GetDelayQueue(serviceBuilder BuildExecutor) *DelayQueue {
	onceNew.Do(func() {
		delayQueueInstance = &DelayQueue{
			Persistence:    getRedisDb(),
			TaskExecutor:   serviceBuilder,
			TaskQueryTable: make(SlotRecorder),
			IsReady:        false,
		}
	})
	return delayQueueInstance
}

// singleton method use other persistence layer
func GetDelayQueueWithPersis(serviceBuilder BuildExecutor, persistence Persistence) *DelayQueue {
	if persistence == nil {
		log.Fatalf("persistance is null")
	}
	onceNew.Do(func() {
		delayQueueInstance = &DelayQueue{
			Persistence:    persistence,
			TaskExecutor:   serviceBuilder,
			TaskQueryTable: make(SlotRecorder),
			IsReady:        false,
		}
	})
	return delayQueueInstance
}

func (dq *DelayQueue) Start() {
	// ensure only one time wheel has been created
	onceStart.Do(dq.init)
}

func (dq *DelayQueue) init() {
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
						go dq.ExecuteTask(p.TaskMode, p.TaskData)
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
		refreshInternal, _ := strconv.Atoi(common.GetEvnWithDefaultVal("REFRESH_POINTER_INTERNAL", fmt.Sprintf("%d", REFRESH_POINTER_DEFAULT_SECONDS)))
		if refreshInternal < REFRESH_POINTER_DEFAULT_SECONDS {
			refreshInternal = REFRESH_POINTER_DEFAULT_SECONDS
		}
		for {
			select {
			case <-time.After(time.Second * REFRESH_POINTER_DEFAULT_SECONDS):
				err := dq.Persistence.SaveWheelTimePointer(int(dq.CurrentIndex))
				if err != nil {
					log.Println(err)
				}
			}
		}

	}()

	dq.IsReady = true
}

func (dq *DelayQueue) loadTasksFromDb() {
	tasks := dq.Persistence.GetList()
	if tasks != nil && len(tasks) > 0 {
		for _, task := range tasks {
			delaySeconds := (task.CycleCount * WHEEL_SIZE) + task.WheelPosition
			if delaySeconds > 0 {
				tk, _ := dq.internalPush(time.Duration(delaySeconds)*time.Second, task.Id, task.TaskMode, task.TaskData, false)
				if tk != nil {
					dq.TaskQueryTable[task.Id] = task.WheelPosition
				}
			}
		}
	}
}

// Add a task to the delay queue
func (dq *DelayQueue) Push(delaySeconds time.Duration, taskMode notify.NotifyMode, taskData interface{}) (task *Task, err error) {
	var pms string
	result, ok := taskData.(string)
	if !ok {
		tp, _ := json.Marshal(taskData)
		pms = string(tp)
	} else {
		pms = result
	}

	task, err = dq.internalPush(delaySeconds, "", taskMode, pms, true)
	if err == nil {
		mutex.Lock()
		dq.TaskQueryTable[task.Id] = task.WheelPosition
		mutex.Unlock()
	}

	return
}

func (dq *DelayQueue) internalPush(delaySeconds time.Duration, taskId string, taskMode notify.NotifyMode, taskData string, needPresis bool) (*Task, error) {
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
		TaskMode:      taskMode,
		TaskData:      taskData,
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

// execute task
func (dq *DelayQueue) ExecuteTask(taskMode notify.NotifyMode, taskData string) error {
	if dq.TaskExecutor != nil {
		executor := dq.TaskExecutor(taskMode)
		if executor != nil {
			log.Printf("Execute task: %d with params: %s\n", taskMode, taskData)

			return executor.DoDelayTask(taskData)
		} else {
			return errors.New("executor is nil")
		}
	} else {
		return errors.New("task build executor is nil")
	}

}

// Get the number of tasks on a time wheel
func (dq *DelayQueue) WheelTaskQuantity(index int) int {
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

func (dq *DelayQueue) GetTask(taskId string) *Task {
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

func (dq *DelayQueue) UpdateTask(taskId string, taskMode notify.NotifyMode, taskData string) error {
	task := dq.GetTask(taskId)
	if task == nil {
		return errors.New("task not found")
	}
	task.TaskMode = taskMode
	task.TaskData = taskData

	// update cache
	dq.Persistence.Save(task)

	return nil
}

func (dq *DelayQueue) DeleteTask(taskId string) error {
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

func (dq *DelayQueue) RemoveAllTasks() error {
	dq.TaskQueryTable = make(SlotRecorder)
	for i := 0; i < len(dq.TimeWheel); i++ {
		dq.TimeWheel[i].NotifyTasks = nil
	}
	dq.Persistence.RemoveAll()
	return nil
}
