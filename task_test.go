package godelayqueue

import (
	"fmt"
	"testing"
)

type businessNotify struct {
}

//implement Executor interface
func (bn *businessNotify) DoDelayTask(contents string) error {
	fmt.Println(fmt.Sprintf("Do task.....%s", contents))
	return nil
}

//a factory method to build executor
func commonFactory(taskType string) Executor {
	//do filter business service by taskType ...
	//...
	return &businessNotify{}
}
func TestTaskExecute(t *testing.T) {
	task := &Task{
		CycleCount:   1,
		TaskType:     "RetryNotify",
		TaskParams:   `{name: "raymond"}`,
		TaskExecutor: commonFactory,
	}
	task.Execute()
}

func TestRunDelayQueue(t *testing.T) {
	c := make(chan struct{})

	queue := GetDelayQueue()
	queue.Start()

	go func() {
		//Simulate pushing tasks to the time wheel
		q := GetDelayQueue()
		q.Push(5, "RetryNotify", `{name: "raymond"}`, commonFactory)
		q.Push(15, "GoodComments", `{name: "raymond"}`, commonFactory)
		q.Push(25, "TaskOne", `{name: "raymond"}`, commonFactory)
		q.Push(35, "TaskThree", `{name: "raymond"}`, commonFactory)
		q.Push(45, "TaskFour", `{name: "raymond"}`, commonFactory)

	}()

	c <- struct{}{}
}
