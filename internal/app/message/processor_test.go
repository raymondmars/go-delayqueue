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
	log.Println(fmt.Sprintf("Do task.....%s", contents))
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

var dq *core.DelayQueue

func testBeforeSetUp() {
	presisDb := &testDoNothingDb{}
	dq = &core.DelayQueue{
		Persistence:    presisDb,
		TaskExecutor:   testFactory,
		TaskQueryTable: make(core.SlotRecorder),
	}
}

func TestProcessorReceive(t *testing.T) {
	testBeforeSetUp()
	processor := NewProcessor()
	resp := processor.Receive(dq, []string{})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, NOT_READY, resp.ErrorCode)

	dq.Start()
	resp = processor.Receive(dq, []string{})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_MESSAGE, resp.ErrorCode)

	resp = processor.Receive(dq, []string{"hello", "1"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, AUTH_FAILED, resp.ErrorCode)

	resp = processor.Receive(dq, []string{messageAuthCode, "1"})
	assert.Equal(t, Ok, resp.Status)
	assert.Equal(t, "pong", resp.Message)

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "0", "1", "http://www.google.com", "test"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_DELAY_TIME, resp.ErrorCode)

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "100", "1", "http://www.google.com"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_PUSH_MESSAGE, resp.ErrorCode)

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "50", "1", "http://www.google.com", "test"})
	assert.Equal(t, Ok, resp.Status)
	assert.Equal(t, "push done", resp.Message)
	assert.Equal(t, 1, dq.WheelTaskQuantity(50%core.WHEEL_SIZE))

	resp = processor.Receive(dq, []string{messageAuthCode, "3", "50"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_PUSH_MESSAGE, resp.ErrorCode)
	assert.Equal(t, "Subscribe is not implemented.", resp.Message)

	resp = processor.Receive(dq, []string{messageAuthCode, "4"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_COMMAND, resp.ErrorCode)

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "100", "2", "test"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_PUSH_MESSAGE, resp.ErrorCode)
	assert.Equal(t, "PubSub is not implemented.", resp.Message)

	resp = processor.Receive(dq, []string{messageAuthCode, "2", "100", "3", "test"})
	assert.Equal(t, Fail, resp.Status)
	assert.Equal(t, INVALID_PUSH_MESSAGE, resp.ErrorCode)
	assert.Equal(t, "Invalid notify way.", resp.Message)
}
