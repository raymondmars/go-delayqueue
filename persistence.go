package godelayqueue

type Persistence interface {
	Save(task *Task) error
	GetList() []*Task
	Delete(taskId string) error
	RemoveAll() error
	GetWheelTimePointer() int
	SaveWheelTimePointer(index int) error
}
