package core

import (
	"fmt"

	"github.com/raymondmars/go-delayqueue/internal/app/notify"
)

type Task struct {
	Id string
	// the number of cycles of the task on the time wheel,
	// when it is equal to 0, the task is executed
	CycleCount int
	// the position of the task on the time wheel
	WheelPosition int
	// the task mode,
	// which is used by the factory method to determine which implementation object to use
	TaskMode notify.NotifyMode
	// task method parameters
	TaskData string

	Next *Task
}

func (t *Task) String() string {
	return fmt.Sprintf("%s %d %d %d %s", t.Id, t.CycleCount, t.WheelPosition, t.TaskMode, t.TaskData)
}
