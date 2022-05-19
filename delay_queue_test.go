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

type testDb struct{}

func (td *testDb) Save(task *Task) error {
	return nil
}

func (td *testDb) GetList() []*Task {
	return []*Task{}
}

func (td *testDb) Delete(taskId string) error {
	return nil
}

var dq *delayQueue

func testBeforeSetUp() {
	presisDb := &testDb{}
	dq = &delayQueue{
		Persistence:    presisDb,
		TaskExecutor:   testFactory,
		TaskQueryTable: make(SlotRecorder),
	}
}

func TestCanntPushTask(t *testing.T) {
	testBeforeSetUp()
	_, err := dq.Push(999*time.Millisecond, "test", "")
	assert.Equal(t, err.Error(), "the delay time cannot be less than 1 second, current is: 999ms")
}

func TestPushTaskInCorrectPosition(t *testing.T) {
	testBeforeSetUp()
	var wg sync.WaitGroup
	for i := 1; i <= WHEEL_SIZE; i++ {
		wg.Add(1)
		go func(index int) {
			dq.Push(time.Duration(index)*time.Second, "test", "")
			wg.Done()
		}(i)
	}
	wg.Wait()

	for i := 1; i <= WHEEL_SIZE; i++ {
		assert.Equal(t, 1, dq.WheelTaskQuantity(i%WHEEL_SIZE))
	}
}

func TestConcurrentPush(t *testing.T) {
	testBeforeSetUp()
	targetSeconds := 50
	taskCounts := 10000
	var wg sync.WaitGroup
	for i := 0; i < taskCounts; i++ {
		wg.Add(1)
		go func() {
			dq.Push(time.Duration(targetSeconds)*time.Second, "test", "")
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, taskCounts, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
}

func TestExecuteTask(t *testing.T) {
	testBeforeSetUp()
	dq.Start()
	targetSeconds := 2
	tk, _ := dq.Push(time.Duration(targetSeconds)*time.Second, "test", "hello,world")
	assert.NotNil(t, tk)

	// wait to task be executed
	time.Sleep(time.Duration(targetSeconds+2) * time.Second)
	assert.Equal(t, 0, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
	assert.Nil(t, dq.GetTask(tk.Id))
}

func TestGetTask(t *testing.T) {
	testBeforeSetUp()
	tk1, _ := dq.Push(10*time.Second, "test1", "hello1")
	tk2, _ := dq.Push(10*time.Second, "test2", "hello2")
	tk3, _ := dq.Push(20*time.Second, "test3", "hello3")

	assert.Equal(t, dq.GetTask(tk1.Id).TaskType, "test1")
	assert.Equal(t, dq.GetTask(tk1.Id).TaskParams, "hello1")

	assert.Equal(t, dq.GetTask(tk2.Id).TaskType, "test2")
	assert.Equal(t, dq.GetTask(tk2.Id).TaskParams, "hello2")

	assert.Equal(t, dq.GetTask(tk3.Id).TaskType, "test3")
	assert.Equal(t, dq.GetTask(tk3.Id).TaskParams, "hello3")
}
