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
	return &businessNotify{}
}

func TestRunDelayQueue(t *testing.T) {
	c := make(chan struct{})
	redis := getRedisDb()
	redis.RemoveAll()

	queue := GetDelayQueue(commonFactory)
	queue.Start()

	q := GetDelayQueue(commonFactory)

	go func() {
		q.Push(time.Second*10, "RetryNotify", "hello,raymond - 10")
	}()
	go func() {
		q.Push(time.Second*15, "RetryNotify", "hello,raymond - 15")
	}()
	go func() {
		q.Push(time.Second*20, "RetryNotify", "hello,raymond - 20")
	}()

	go func() {
		select {
		case <-time.After(time.Second * 21):
			<-c
		}
	}()

	c <- struct{}{}

}
