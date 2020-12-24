package godelayqueue

import (
	"fmt"
	"testing"
)

type businessNotify struct {
}

//implement Executor interface
func (bn *businessNotify) DoDelayTask(contents string) error {
	fmt.Println(fmt.Sprintf("Do task.....%s", contents))
	return nil
}

//a factory method to build executor
func commonFactory(taskType string) Executor {
	//do filter business service by taskType ...
	//...
	fmt.Println(taskType)
	return &businessNotify{}
}
func TestTaskExecute(t *testing.T) {
	// task := &Task{
	// 	CycleCount: 1,
	// 	TaskType:   "RetryNotify",
	// 	TaskParams: `{name: "raymond"}`,
	// }
	// task.Execute()
}

func TestRunDelayQueue(t *testing.T) {
	c := make(chan struct{})

	// u := uuid.New()
	// fmt.Println(u.String())
	// redis := getRedisDb()
	// redis.RemoveAll()

	queue := GetDelayQueue(commonFactory)
	queue.Start()

	// go func() {
	// 	//Simulate pushing tasks to the time wheel
	// 	q := GetDelayQueue(commonFactory)
	// 	q.Push(5, "RetryNotify", `{name: "raymond"}`)
	// 	q.Push(15, "GoodComments", `{name: "raymond"}`)
	// 	q.Push(25, "TaskOne", `{name: "raymond"}`)
	// 	q.Push(35, "TaskThree", `{name: "raymond"}`)
	// 	q.Push(45, "TaskFour", `{name: "raymond"}`)

	// }()

	c <- struct{}{}
}

func TestQueuePersistence(t *testing.T) {

	// set := token.NewFileSet()
	// packs, err := parser.ParseDir(set, "./", nil, 0)
	// if err != nil {
	// 	fmt.Println("Failed to parse package:", err)
	// 	os.Exit(1)
	// }

	// funcs := []*ast.FuncDecl{}
	// for _, pack := range packs {
	// 	for _, f := range pack.Files {
	// 		for _, d := range f.Decls {
	// 			if fn, isFn := d.(*ast.FuncDecl); isFn {
	// 				funcs = append(funcs, fn)
	// 				fmt.Println(fn.Name)
	// 			}
	// 		}
	// 	}
	// }

	// fmt.Printf("all funcs: %+v\n", funcs)

}
