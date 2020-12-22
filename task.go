package godelayqueue

import "errors"

//executor factory method
type BuildExecutor func(taskType string) Executor

type Task struct {
	CycleCount   int
	TaskType     string
	TaskParams   string
	TaskExecutor BuildExecutor
	Next         *Task
}

//execute task
func (t *Task) Execute() error {
	if t.TaskExecutor != nil {
		executor := t.TaskExecutor(t.TaskType)
		if executor != nil {
			return executor.DoDelayTask(t.TaskParams)
		} else {
			return errors.New("executor is nil")
		}
	} else {
		return errors.New("task build executor is nil")
	}

}
