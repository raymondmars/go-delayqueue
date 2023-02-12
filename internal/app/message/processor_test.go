package message

import (
	"fmt"
	"log"
	"testing"

	"github.com/raymondmars/go-delayqueue/internal/app/core"
	"github.com/raymondmars/go-delayqueue/internal/app/notify"
	"github.com/stretchr/testify/assert"
)

type testNotify struct{}

func (tn *testNotify) DoDelayTask(contents string) error {
	log.Println(fmt.Sprintf("test -> Do task.....%s", contents))
	return nil
}

func testFactory(taskMode notify.NotifyMode) notify.Executor {
	return &testNotify{}
}

type testDoNothingDb struct{}

func (td *testDoNothingDb) Save(task *core.Task) error {
	return nil
}

func (td *testDoNothingDb) GetList() []*core.Task {
	return []*core.Task{}
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

func testQueue() *core.DelayQueue {
	presisDb := &testDoNothingDb{}
	return &core.DelayQueue{
		Persistence:    presisDb,
		TaskExecutor:   testFactory,
		TaskQueryTable: make(core.SlotRecorder),
	}
}

func TestProcessor(t *testing.T) {
	dq := testQueue()
	processor := NewProcessor()

	// test not ready
	resp := processor.Receive(dq, []string{})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, NOT_READY, resp.ErrorCode)

	dq.Start()

	// test auth and ping
	resp = processor.Receive(dq, []string{})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_MESSAGE, resp.ErrorCode)

	resp = processor.Receive(dq, []string{"hello", "1"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, AUTH_FAILED, resp.ErrorCode)

	resp = processor.Receive(dq, []string{messageAuthCode, "1"})
	assert.Equal(t, Ok, resp.Status)
	assert.Equal(t, "pong", resp.Message)

	// test push task
	resp = processor.Receive(dq, []string{messageAuthCode, "2", "0", "1", "http://www.google.com", "test"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_DELAY_TIME, resp.ErrorCode)

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "100", "1", "http://www.google.com"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_PUSH_MESSAGE, resp.ErrorCode)

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "50", "1", "http://www.google.com", "test"})
	assert.Equal(t, Ok, resp.Status)
	assert.Equal(t, 1, dq.WheelTaskQuantity(50%core.WHEEL_SIZE))

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "100", "2", "queue_name"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_PUSH_MESSAGE, resp.ErrorCode)

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "100", "2", "queue_name", "test"})
	assert.Equal(t, Ok, resp.Status)
	assert.Equal(t, 1, dq.WheelTaskQuantity(100%core.WHEEL_SIZE))

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "100", "3", "queue_name", "test"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_PUSH_MESSAGE, resp.ErrorCode)
	assert.Equal(t, "Invalid notify way.", resp.Message)

	// test update task from client
	resp = processor.Receive(dq, []string{messageAuthCode, "2", "50", "1", "http://www.google.com", "test1"})
	assert.Equal(t, Ok, resp.Status)
	taskId := resp.Message
	taskInfo := dq.GetTask(taskId)
	assert.Equal(t, "http://www.google.com|test1", taskInfo.TaskData)
	assert.Equal(t, notify.HTTP, taskInfo.TaskMode)
	// send update message
	resp = processor.Receive(dq, []string{messageAuthCode, "3", taskId, "2", "queue_name", "test2"})
	assert.Equal(t, Ok, resp.Status)
	taskInfo = dq.GetTask(taskId)
	assert.Equal(t, "queue_name|test2", taskInfo.TaskData)
	assert.Equal(t, notify.SubPub, taskInfo.TaskMode)

	// test delete task from client
	delaySeconds := 30
	// push a task to queue
	resp = processor.Receive(dq, []string{messageAuthCode, "2", fmt.Sprintf("%d", delaySeconds), "1", "http://www.google.com", "test1"})
	assert.Equal(t, Ok, resp.Status)
	assert.Equal(t, 1, dq.WheelTaskQuantity(delaySeconds%core.WHEEL_SIZE))

	taskId = resp.Message

	// send delete message
	resp = processor.Receive(dq, []string{messageAuthCode, "4", taskId})
	assert.Equal(t, Ok, resp.Status)
	assert.Equal(t, 0, dq.WheelTaskQuantity(delaySeconds%core.WHEEL_SIZE))
}
