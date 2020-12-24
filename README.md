# go-delayqueue
A delay queue implemented in go language

* Support delayed execution of tasks, tasks are customized by users.   
* Support task persistence, redis is used to save tasks by default, and the task can be resumed from the cache after the program is down.   
