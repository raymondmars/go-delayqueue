# go-delayqueue  
[中文](https://github.com/0RaymondJiang0/go-delayqueue/blob/main/README.md)  | [English](https://github.com/0RaymondJiang0/go-delayqueue/blob/main/README.en.md)   

A delay queue implemented in go language

* 时间轮算法实现的延迟队列，支持细粒度延迟任务执行，用户自定义任务实现，无上下文依赖，可扩展性高。       
* 延迟队列中的任务，支持持久化存储，持久对象可自行实现，默认提供redis持久实现；支持程序中断后的任务恢复，可靠性更高。      

详细说明参考如下文章：   

[使用时间轮算法实现延迟队列 (一)](https://raymondjiang.net/2020/12/10/implement-delay-queue-use-golang/)  

[使用时间轮算法实现延迟队列 (二)](https://raymondjiang.net/2020/12/20/implement-delay-queue-use-golang-2/)   
