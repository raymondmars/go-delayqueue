package core

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/raymondmars/go-delayqueue/internal/app/notify"
	"github.com/stretchr/testify/assert"
)

type testNotify struct{}

// implement Executor interface
func (tn *testNotify) DoDelayTask(contents string) error {
	log.Println(fmt.Sprintf("Do task.....%s", contents))
	return nil
}

// a factory method to build executor
func testFactory(taskMode notify.NotifyMode) notify.Executor {
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

func (td *testDoNothingDb) GetWheelTimePointer() int {
	return 0
}

func (td *testDoNothingDb) SaveWheelTimePointer(index int) error {
	return nil
}

var dq *DelayQueue

func testBeforeSetUp() {
	presisDb := &testDoNothingDb{}
	dq = &DelayQueue{
		Persistence:  presisDb,
		TaskExecutor: testFactory,
	}
}

func testWithRedisBeforeSetUp() {
	dq = &DelayQueue{
		Persistence:  getRedisDb(),
		TaskExecutor: testFactory,
	}
	dq.RemoveAllTasks()
}

func TestBuildTaskId(t *testing.T) {
	testBeforeSetUp()
	for i := 0; i < WHEEL_SIZE; i++ {
		header := uuid.New().String()
		assert.Equal(t, fmt.Sprintf("%sx%x", header, i), dq.buildTaskId(header, i))
	}
}

func TestParseSlotIndex(t *testing.T) {
	testBeforeSetUp()
	for i := 0; i < WHEEL_SIZE; i++ {
		taskId := dq.buildTaskId(uuid.New().String(), i)
		index, err := dq.parseSlotIndex(taskId)
		assert.NoError(t, err)
		assert.Equal(t, i, int(index))
	}
}

func TestCanntPushTask(t *testing.T) {
	testBeforeSetUp()
	_, err := dq.Push(999*time.Millisecond, notify.HTTP, "")
	assert.Equal(t, err.Error(), "the delay time cannot be less than 1 second, current is: 999ms")
}

func TestPushTaskInCorrectPosition(t *testing.T) {
	testBeforeSetUp()
	var wg sync.WaitGroup
	for i := 1; i <= WHEEL_SIZE; i++ {
		wg.Add(1)
		go func(index int) {
			dq.Push(time.Duration(index)*time.Second, notify.HTTP, "")
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
			dq.Push(time.Duration(targetSeconds)*time.Second, notify.SubPub, "")
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
	tk, _ := dq.Push(time.Duration(targetSeconds)*time.Second, notify.HTTP, "hello,world")
	assert.NotNil(t, tk)

	// wait to task be executed
	time.Sleep(time.Duration(targetSeconds+2) * time.Second)
	assert.Equal(t, 0, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
	assert.Nil(t, dq.GetTask(tk.Id))
}

func TestGetTask(t *testing.T) {
	testBeforeSetUp()
	tk1, _ := dq.Push(10*time.Second, notify.HTTP, "hello1")
	tk2, _ := dq.Push(10*time.Second, notify.HTTP, "hello2")
	tk3, _ := dq.Push(20*time.Second, notify.SubPub, "hello3")

	assert.Equal(t, dq.GetTask(tk1.Id).TaskMode, notify.HTTP)
	assert.Equal(t, dq.GetTask(tk1.Id).TaskData, "hello1")

	assert.Equal(t, dq.GetTask(tk2.Id).TaskMode, notify.HTTP)
	assert.Equal(t, dq.GetTask(tk2.Id).TaskData, "hello2")

	assert.Equal(t, dq.GetTask(tk3.Id).TaskMode, notify.SubPub)
	assert.Equal(t, dq.GetTask(tk3.Id).TaskData, "hello3")
}

func TestConcurrentGetTask(t *testing.T) {
	testBeforeSetUp()
	targetSeconds := 50
	taskCounts := 10000
	var taskIds [10000]string
	var wg sync.WaitGroup
	total := 0
	for i := 0; i < taskCounts; i++ {
		tk, _ := dq.Push(time.Duration(targetSeconds)*time.Second, notify.HTTP, i)
		taskIds[i] = tk.Id
		total = total + i
	}

	innerTotal := 0
	for i := 0; i < taskCounts; i++ {
		wg.Add(1)
		go func(index int) {
			tk := dq.GetTask(taskIds[index])
			if tk != nil {
				p, _ := strconv.Atoi(tk.TaskData)
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
	tk1, _ := dq.Push(10*time.Second, notify.HTTP, "hello1")

	assert.Equal(t, dq.GetTask(tk1.Id).TaskMode, notify.HTTP)
	assert.Equal(t, dq.GetTask(tk1.Id).TaskData, "hello1")

	err := dq.UpdateTask(tk1.Id, notify.SubPub, "hello100")
	assert.Nil(t, err)

	assert.Equal(t, dq.GetTask(tk1.Id).TaskMode, notify.SubPub)
	assert.Equal(t, dq.GetTask(tk1.Id).TaskData, "hello100")
}

func TestDeleteTask(t *testing.T) {
	testBeforeSetUp()
	targetSeconds := 10
	tk1, _ := dq.Push(time.Duration(targetSeconds)*time.Second, notify.HTTP, "hello1")
	tk2, _ := dq.Push(time.Duration(targetSeconds)*time.Second, notify.SubPub, "hello2")
	tk3, _ := dq.Push(time.Duration(targetSeconds)*time.Second, notify.SubPub, "hello3")

	assert.Equal(t, 3, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
	err := dq.DeleteTask(tk2.Id)
	assert.Nil(t, err)

	assert.Equal(t, 2, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))

	assert.Equal(t, dq.GetTask(tk1.Id).TaskMode, notify.HTTP)
	assert.Equal(t, dq.GetTask(tk1.Id).TaskData, "hello1")

	assert.Equal(t, dq.GetTask(tk3.Id).TaskMode, notify.SubPub)
	assert.Equal(t, dq.GetTask(tk3.Id).TaskData, "hello3")

	dq.DeleteTask(tk1.Id)
	dq.DeleteTask(tk3.Id)

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
			tk, _ := dq.Push(time.Duration(targetSeconds)*time.Second, notify.HTTP, "")
			lock.Lock()
			defer lock.Unlock()
			taskIds = append(taskIds, tk.Id)
			wg.Done()
		}()
	}
	wg.Wait()

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

	assert.Equal(t, 0, dq.WheelTaskQuantity(targetSeconds%WHEEL_SIZE))
}

func TestDelayQueueAndRedisIntegrate(t *testing.T) {
	testWithRedisBeforeSetUp()

	randomSlots := []int{60, 100, 560, 2450, 3500}
	eachSoltNodes := 100
	for _, seconds := range randomSlots {
		for i := 0; i < eachSoltNodes; i++ {
			dq.Push(time.Duration(seconds)*time.Second, notify.HTTP, i)
		}
	}

	for _, seconds := range randomSlots {
		assert.Equal(t, eachSoltNodes, dq.WheelTaskQuantity(seconds%WHEEL_SIZE))
	}
	//remove nodes
	for i := 0; i < len(dq.TimeWheel); i++ {
		dq.TimeWheel[i].NotifyTasks = nil
	}
	// Do not use range to fetch wheel, it will use copy value
	// for _, wheel := range dq.TimeWheel {
	// 	wheel.NotifyTasks = nil
	// }
	for _, seconds := range randomSlots {
		assert.Equal(t, 0, dq.WheelTaskQuantity(seconds%WHEEL_SIZE))
	}
	// load from cache
	dq.loadTasksFromDb()
	for _, seconds := range randomSlots {
		assert.Equal(t, eachSoltNodes, dq.WheelTaskQuantity(seconds%WHEEL_SIZE))
	}
}

func BenchmarkPushTask(b *testing.B) {
	testBeforeSetUp()
	targetSeconds := 50

	for i := 0; i < b.N; i++ {
		dq.Push(time.Duration(targetSeconds)*time.Second, notify.HTTP, i)
	}
}
