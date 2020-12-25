package godelayqueue

import (
	"fmt"
	"log"
	"testing"
	"time"
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

	go func() {
		//Simulate pushing tasks to the time wheel
		q := GetDelayQueue(commonFactory)
		q.Push(time.Second*10, "RetryNotify", "hello,raymond.")

	}()
	c <- struct{}{}
}
