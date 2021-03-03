package godelayqueue

import (
	"fmt"
	"log"
	"testing"
)

type businessNotify struct {
}

//implement Executor interface
func (bn *businessNotify) DoDelayTask(contents string) error {
	log.Println(fmt.Sprintf("Do task.....%s", contents))
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

	// go func() {
	// 	//Simulate pushing tasks to the time wheel
	// 	q := GetDelayQueue(commonFactory)
	// 	q.Push(time.Second*300, "RetryNotify", "hello,raymond.20")
	// 	q.Push(time.Second*360, "RetryNotify", "hello,raymond.30")
	// 	// q.Push(time.Second*40, "RetryNotify", "hello,raymond.40")
	// 	// q.Push(time.Second*50, "RetryNotify", "hello,raymond.50")
	// 	// q.Push(time.Second*60, "RetryNotify", "hello,raymond.60")
	// 	// q.Push(time.Second*70, "RetryNotify", "hello,raymond.70")

	// }()
	c <- struct{}{}
}
