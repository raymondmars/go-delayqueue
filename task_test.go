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
	// 	q.Push(time.Second*120, "RetryNotify", "hello,raymond.120")
	// 	q.Push(time.Second*2*3600, "RetryNotify", "hello,raymond.two hours 2*3600")
	// 	q.Push(time.Second*3*3600, "RetryNotify", "hello,raymond.three hours 3*3600")

	// }()
	c <- struct{}{}
}
