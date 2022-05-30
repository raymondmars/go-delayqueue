package godelayqueue

import (
	"fmt"
	"log"
	"strconv"
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

type testDoNothingDb struct{}

func (td *testDoNothingDb) Save(task *Task) error {
	return nil
}

func (td *testDoNothingDb) GetList() []*Task {
	return []*Task{}
}

func (td *testDoNothingDb) Delete(taskId string) error {
	return nil
}

func (td *testDoNothingDb) RemoveAll() error {
	return nil
}

var dq *delayQueue

func testBeforeSetUp() {
	presisDb := &testDoNothingDb{}
	dq = &delayQueue{
		Persistence:    presisDb,
		TaskExecutor:   testFactory,
		TaskQueryTable: make(SlotRecorder),
	}
}

func testWithRedisBeforeSetUp() {
	dq = &delayQueue{
		Persistence:    getRedisDb(),
		TaskExecutor:   testFactory,
		TaskQueryTable: make(SlotRecorder),
	}
	dq.RemoveAllTasks()
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

func TestConcurrentGetTask(t *testing.T) {
	testBeforeSetUp()
	targetSeconds := 50
	taskCounts := 10000
	var taskIds [10000]string
	var wg sync.WaitGroup
	total := 0
	for i := 0; i < taskCounts; i++ {
		tk, _ := dq.Push(time.Duration(targetSeconds)*time.Second, "test", i)
		taskIds[i] = tk.Id
		total = total + i
	}

	innerTotal := 0
	for i := 0; i < taskCounts; i++ {
		wg.Add(1)
		go func(index int) {
			tk := dq.GetTask(taskIds[index])
			if tk != nil {
				p, _ := strconv.Atoi(tk.TaskParams)
				mutex.Lock()
				innerTotal = innerTotal + p
				mutex.Unlock()
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, taskCounts, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
	assert.Equal(t, total, innerTotal)
}

func TestUpdateTask(t *testing.T) {
	testBeforeSetUp()
	tk1, _ := dq.Push(10*time.Second, "test1", "hello1")

	assert.Equal(t, dq.GetTask(tk1.Id).TaskType, "test1")
	assert.Equal(t, dq.GetTask(tk1.Id).TaskParams, "hello1")

	err := dq.UpdateTask(tk1.Id, "test100", "hello100")
	assert.Nil(t, err)

	assert.Equal(t, dq.GetTask(tk1.Id).TaskType, "test100")
	assert.Equal(t, dq.GetTask(tk1.Id).TaskParams, "hello100")
}

func TestDeleteTask(t *testing.T) {
	testBeforeSetUp()
	targetSeconds := 10
	tk1, _ := dq.Push(time.Duration(targetSeconds)*time.Second, "test1", "hello1")
	tk2, _ := dq.Push(time.Duration(targetSeconds)*time.Second, "test2", "hello2")
	tk3, _ := dq.Push(time.Duration(targetSeconds)*time.Second, "test3", "hello3")
	assert.Equal(t, 3, len(dq.TaskQueryTable))
	assert.Equal(t, 3, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
	err := dq.DeleteTask(tk2.Id)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(dq.TaskQueryTable))
	assert.Equal(t, 2, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))

	assert.Equal(t, dq.GetTask(tk1.Id).TaskType, "test1")
	assert.Equal(t, dq.GetTask(tk1.Id).TaskParams, "hello1")

	assert.Equal(t, dq.GetTask(tk3.Id).TaskType, "test3")
	assert.Equal(t, dq.GetTask(tk3.Id).TaskParams, "hello3")

	dq.DeleteTask(tk1.Id)
	dq.DeleteTask(tk3.Id)

	assert.Equal(t, 0, len(dq.TaskQueryTable))
	assert.Equal(t, 0, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
}

func TestConcurrentDeleteTasks(t *testing.T) {
	testBeforeSetUp()
	targetSeconds := 50
	taskCounts := 10000
	taskIds := []string{}
	var lock sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < taskCounts; i++ {
		wg.Add(1)
		go func() {
			tk, _ := dq.Push(time.Duration(targetSeconds)*time.Second, "test", "")
			lock.Lock()
			defer lock.Unlock()
			taskIds = append(taskIds, tk.Id)
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, taskCounts, len(dq.TaskQueryTable))
	assert.Equal(t, taskCounts, len(taskIds))
	assert.Equal(t, taskCounts, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))

	for i := 0; i < taskCounts; i++ {
		wg.Add(1)
		go func(index int) {
			dq.DeleteTask(taskIds[index])
			wg.Done()
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 0, len(dq.TaskQueryTable))
	assert.Equal(t, 0, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
}

func TestDelayQueueAndRedisIntegrate(t *testing.T) {
	testWithRedisBeforeSetUp()

	randomSlots := []int{60, 100, 560, 2450, 3500}
	eachSoltNodes := 100
	for _, seconds := range randomSlots {
		for i := 0; i < eachSoltNodes; i++ {
			dq.Push(time.Duration(seconds)*time.Second, "test", i)
		}
	}
	assert.Equal(t, eachSoltNodes*len(randomSlots), len(dq.TaskQueryTable))
	for _, seconds := range randomSlots {
		assert.Equal(t, eachSoltNodes, dq.WheelTaskQuantity(seconds%WHEEL_SIZE))
	}
	//remove nodes
	dq.TaskQueryTable = make(SlotRecorder)
	for i := 0; i < len(dq.TimeWheel); i++ {
		dq.TimeWheel[i].NotifyTasks = nil
	}
	// Do not use range to fetch wheel, it will use copy value
	// for _, wheel := range dq.TimeWheel {
	// 	wheel.NotifyTasks = nil
	// }
	assert.Equal(t, 0, len(dq.TaskQueryTable))
	for _, seconds := range randomSlots {
		assert.Equal(t, 0, dq.WheelTaskQuantity(seconds%WHEEL_SIZE))
	}
	// load from cache
	dq.loadTasksFromDb()
	assert.Equal(t, eachSoltNodes*len(randomSlots), len(dq.TaskQueryTable))
	for _, seconds := range randomSlots {
		assert.Equal(t, eachSoltNodes, dq.WheelTaskQuantity(seconds%WHEEL_SIZE))
	}
}

func BenchmarkPushTask(b *testing.B) {
	testBeforeSetUp()
	targetSeconds := 50

	for i := 0; i < b.N; i++ {
		dq.Push(time.Duration(targetSeconds)*time.Second, "test", i)
	}
}
