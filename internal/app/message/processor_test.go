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
}
