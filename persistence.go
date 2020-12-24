package godelayqueue

type Persistence interface {
	Save(task *Task) error
	GetList() []*Task
	Delete(taskId string) error
}
