# go-delayqueue
![example workflow](https://github.com/0RaymondJiang0/go-delayqueue/actions/workflows/build.yml/badge.svg)

A delay queue implemented in go language

* The delay queue implemented by the time wheel algorithm supports fine-grained delayed task execution, user-defined task implementation, no context dependence, and high scalability.
* Delay tasks in the queue, support persistent storage, persistent objects can be implemented by themselves, redis persistent implementation is provided by default; support task recovery after program interruption, with higher reliability.
