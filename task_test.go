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

func TestRunDelayQueue(t *testing.T) {
	c := make(chan struct{})
	// redis := getRedisDb()
	// redis.RemoveAll()

	queue := GetDelayQueue(commonFactory)
	queue.Start()

	go func() {
		//Simulate pushing tasks to the time wheel
		q := GetDelayQueue(commonFactory)
		q.Push(5, "RetryNotify", `{name: "raymond"}`)
		q.Push(15, "GoodComments", `{name: "raymond"}`)
		q.Push(25, "TaskOne", `{name: "raymond"}`)
		q.Push(35, "TaskThree", `{name: "raymond"}`)
		q.Push(45, "TaskFour", `{name: "raymond"}`)

	}()

	c <- struct{}{}
}
