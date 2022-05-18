package godelayqueue

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testNotify struct{}

// implement Executor interface
func (tn *testNotify) DoDelayTask(contents string) error {
	log.Println(fmt.Sprintf("Do task.....%s", contents))
	return nil
}

// a factory method to build executor
func testFactory(taskType string) Executor {
	return &testNotify{}
}

func TestCanntPushTask(t *testing.T) {
	q := GetDelayQueue(testFactory)
	_, err := q.Push(999*time.Millisecond, "test", "")
	assert.Equal(t, err.Error(), "the delay time cannot be less than 1 second, current is: 999ms")
}

func TestPushTaskInCorrectPosition(t *testing.T) {
	q := GetDelayQueue(testFactory)

	var wg sync.WaitGroup
	for i := 1; i <= WHEEL_SIZE; i++ {
		wg.Add(1)
		go func(index int) {
			q.Push(time.Duration(index)*time.Second, "test", "")
			wg.Done()
		}(i)
	}
	wg.Wait()

	for i := 1; i <= WHEEL_SIZE; i++ {
		assert.Equal(t, 1, q.WheelTaskQuantity(i%WHEEL_SIZE))
	}
}

func TestConcurrentPush(t *testing.T) {
	q := GetDelayQueue(testFactory)
	targetSeconds := 50
	taskCounts := 10000
	var wg sync.WaitGroup
	for i := 0; i < taskCounts; i++ {
		wg.Add(1)
		go func() {
			q.Push(time.Duration(targetSeconds)*time.Second, "test", "")
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, taskCounts, q.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
}

func TestGetTask(t *testing.T) {
	q := GetDelayQueue(testFactory)
	tk1, _ := q.Push(10*time.Second, "test1", "hello1")
	tk2, _ := q.Push(10*time.Second, "test2", "hello2")
	tk3, _ := q.Push(20*time.Second, "test3", "hello3")

	assert.Equal(t, q.GetTask(tk1.Id).TaskType, "test1")
	assert.Equal(t, q.GetTask(tk1.Id).TaskParams, "hello1")

	assert.Equal(t, q.GetTask(tk2.Id).TaskType, "test2")
	assert.Equal(t, q.GetTask(tk2.Id).TaskParams, "hello2")

	assert.Equal(t, q.GetTask(tk3.Id).TaskType, "test3")
	assert.Equal(t, q.GetTask(tk3.Id).TaskParams, "hello3")
}
