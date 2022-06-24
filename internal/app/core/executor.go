package core

//executor interface, the business instance need to implement it
type Executor interface {
	DoDelayTask(contents string) error
}
