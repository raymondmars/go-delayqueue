package godelayqueue

//任务采用链表结构
type Task struct {
	//任务ID便于持久化
	Id string
	//任务在时间轮上的循环次数，等于0时，执行该任务
	CycleCount int
	//任务在时间轮上的位置
	WheelPosition int
	//任务类型，用于工厂方法判断该使用哪个实现对象
	TaskType string
	//任务方法参数
	TaskParams string

	Next *Task
}
