package godelayqueue

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const (
	//轮的时间长度，目前设置为一个小时，也就是时间轮每循环一次需要1小时；默认时间轮上面的每走一步的最小粒度为1秒。
	WHEEL_SIZE = 3600
)

var onceNew sync.Once
var onceStart sync.Once

var delayQueueInstance *delayQueue

type wheel struct {
	// NotifyTasks []*Task
	//所有在时间轮上的任务均采用链表形式存储
	//如果采用数组，对应已经执行过的任务，会造成不必要的空间浪费或者数组移动造成的时间复杂度
	NotifyTasks *Task
}

type delayQueue struct {
	//循环队列
	TimeWheel    [WHEEL_SIZE]wheel
	CurrentIndex uint
}

//单列方法
func GetDelayQueue() *delayQueue {
	//保证只初始化一次
	onceNew.Do(func() {
		delayQueueInstance = &delayQueue{}
	})
	return delayQueueInstance
}

func (dq *delayQueue) Start() {
	//保证只会有一个时间轮计时器
	onceStart.Do(dq.init)
}

func (dq *delayQueue) init() {
	go func() {
		for {
			select {
			//默认时间轮上的最小粒度为1秒
			case <-time.After(time.Second * 1):
				pointer := dq.CurrentIndex % WHEEL_SIZE

				taskLinkHead := dq.TimeWheel[pointer].NotifyTasks
				//遍历链表
				//当前节点前一指针
				prev := taskLinkHead
				//当前节点指针
				p := taskLinkHead
				for p != nil {
					if p.CycleCount == 0 {
						//开启新的go routing 去做通知，加快每次遍历的速度，确保不会拖慢时间轮的运行
						//如果任务有异常，尽量让具体的业务对象去处理，延迟队列不处理具体业务异常，
						//这样可以保证延迟队列的业务单纯性，避免难以维护的问题。如果具体业务出现问题，需要重复通知，可以将任务重新加入队列即可。
						go p.Execute()
						//删除链表节点 task
						//如果是第一个节点
						if prev == p {
							dq.TimeWheel[pointer].NotifyTasks = p.Next
							prev = p.Next
							p = p.Next
						} else {
							//如果不是第一个节点
							prev.Next = p.Next
							p = p.Next
						}
					} else {
						p.CycleCount--
						prev = p
						p = p.Next
					}

				}
				dq.CurrentIndex++
				fmt.Println(pointer)
			}
		}
	}()
}

//将任务加入延迟队列
func (dq *delayQueue) Push(delaySeconds int, taskType string, taskParams interface{}, executeFactory BuildExecutor) error {

	//从当前时间指针处开始计时
	calculateValue := int(dq.CurrentIndex) + delaySeconds

	cycle := calculateValue / WHEEL_SIZE
	index := calculateValue % WHEEL_SIZE

	pms, _ := json.Marshal(taskParams)

	task := &Task{
		CycleCount:   cycle,
		TaskType:     taskType,
		TaskParams:   string(pms),
		TaskExecutor: executeFactory,
	}
	if dq.TimeWheel[index].NotifyTasks == nil {
		dq.TimeWheel[index].NotifyTasks = task
	} else {
		//将新任务插入链表头，由于任务之间没有顺序关系，这种实现最为简单
		head := dq.TimeWheel[index].NotifyTasks
		task.Next = head
		dq.TimeWheel[index].NotifyTasks = task
	}

	return nil
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
