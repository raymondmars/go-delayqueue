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
	//轮的时间长度，目前设置为一个小时，也就是时间轮每循环一次需要1小时；默认时间轮上面的每走一步的最小粒度为1秒。
	WHEEL_SIZE = 3600
)

//定义一个获取实现具体任务对象的工厂方法，该方法需要在业务项目中定义并实现
type BuildExecutor func(taskType string) Executor

var onceNew sync.Once
var onceStart sync.Once

var delayQueueInstance *delayQueue

type wheel struct {
	//所有在时间轮上的任务均采用链表形式存储
	//如果采用数组，对应已经执行过的任务，会造成不必要的空间浪费或者数组移动造成的时间复杂度
	NotifyTasks *Task
}

type delayQueue struct {
	//循环队列
	TimeWheel    [WHEEL_SIZE]wheel
	CurrentIndex uint //时间轮当前指针
	Persistence
	//任务工厂方法指针, 需要在对象创建时初始化它
	TaskExecutor BuildExecutor
}

//单列方法,使用默认的redis持久方案
func GetDelayQueue(serviceBuilder BuildExecutor) *delayQueue {
	//保证只初始化一次
	onceNew.Do(func() {
		delayQueueInstance = &delayQueue{
			Persistence:  getRedisDb(),
			TaskExecutor: serviceBuilder,
		}
	})
	return delayQueueInstance
}

//单列方法，使用外部传入的持久方案
func GetDelayQueueWithPersis(serviceBuilder BuildExecutor, persistence Persistence) *delayQueue {
	if persistence == nil {
		log.Fatalf("persistance is null")
	}
	//保证只初始化一次
	onceNew.Do(func() {
		delayQueueInstance = &delayQueue{
			Persistence:  persistence,
			TaskExecutor: serviceBuilder,
		}
	})
	return delayQueueInstance
}

func (dq *delayQueue) Start() {
	//保证只会有一个时间轮计时器
	onceStart.Do(dq.init)
}

func (dq *delayQueue) init() {
	go func() {
		//从缓存中加载持久化的任务
		dq.loadTasksFromDb()
		for {
			select {
			//默认时间轮上的最小粒度为1秒
			case <-time.After(time.Second * 1):

				if dq.CurrentIndex >= WHEEL_SIZE {
					dq.CurrentIndex = dq.CurrentIndex % WHEEL_SIZE
				}

				taskLinkHead := dq.TimeWheel[dq.CurrentIndex].NotifyTasks
				//遍历链表
				//当前节点前一指针
				prev := taskLinkHead
				//当前节点指针
				p := taskLinkHead
				for p != nil {
					if p.CycleCount == 0 {
						taskId := p.Id
						//开启新的go routing 去做通知，加快每次遍历的速度，确保不会拖慢时间轮的运行
						//如果任务有异常，尽量让具体的业务对象去处理，延迟队列不处理具体业务异常，
						//这样可以保证延迟队列的业务单纯性，避免难以维护的问题。如果具体业务出现问题，需要重复通知，可以将任务重新加入队列即可。
						go dq.ExecuteTask(p.TaskType, p.TaskParams)
						//删除链表节点 task
						//如果是第一个节点
						if prev == p {
							dq.TimeWheel[dq.CurrentIndex].NotifyTasks = p.Next
							prev = p.Next
							p = p.Next
						} else {
							//如果不是第一个节点
							prev.Next = p.Next
							p = p.Next
						}
						//从持久对象上删除该任务
						dq.Persistence.Delete(taskId)

					} else {
						p.CycleCount--
						prev = p
						p = p.Next
					}

				}

				dq.CurrentIndex++

			}
		}
	}()
}

func (dq *delayQueue) loadTasksFromDb() {
	tasks := dq.Persistence.GetList()
	if tasks != nil && len(tasks) > 0 {
		for _, task := range tasks {
			// fmt.Printf("%v\n", task)
			delaySeconds := ((task.CycleCount + 1) * WHEEL_SIZE) + task.WheelPosition
			if delaySeconds > 0 {
				dq.internalPush(time.Duration(delaySeconds)*time.Second, task.Id, task.TaskType, task.TaskParams, false)
			}
		}
	}
}

//将任务加入延迟队列
func (dq *delayQueue) Push(delaySeconds time.Duration, taskType string, taskParams interface{}) error {

	var pms string
	result, ok := taskParams.(string)
	if !ok {
		tp, _ := json.Marshal(taskParams)
		pms = string(tp)
	} else {
		pms = result
	}

	return dq.internalPush(delaySeconds, "", taskType, pms, true)
}

func (dq *delayQueue) internalPush(delaySeconds time.Duration, taskId string, taskType string, taskParams string, notNeedPresis bool) error {
	if int(delaySeconds.Seconds()) == 0 {
		errorMsg := fmt.Sprintf("the delay time cannot be less than 1 second, current is: %v", delaySeconds)
		log.Println(errorMsg)
		return errors.New(errorMsg)
	}
	//从当前时间指针处开始计时
	calculateValue := int(dq.CurrentIndex) + int(delaySeconds.Seconds())

	cycle := calculateValue / WHEEL_SIZE
	if cycle > 0 {
		cycle--
	}
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
	if dq.TimeWheel[index].NotifyTasks == nil {
		dq.TimeWheel[index].NotifyTasks = task
		// log.Println(dq.TimeWheel[index].NotifyTasks)
	} else {
		//将新任务插入链表头，由于任务之间没有顺序关系，这种实现最为简单
		head := dq.TimeWheel[index].NotifyTasks
		task.Next = head
		dq.TimeWheel[index].NotifyTasks = task
		// log.Println(dq.TimeWheel[index].NotifyTasks)
	}
	if notNeedPresis {
		//持久化任务
		dq.Persistence.Save(task)
	}

	return nil
}

//通过工厂方法获取具体实现，然后调用方法，执行任务
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

//延迟队列上，某个时间轮上的任务数量
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
