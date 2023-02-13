## go-delayqueue
![example workflow](https://github.com/raymondmars/go-delayqueue/actions/workflows/build.yml/badge.svg)

A simple and easy-to-use delay queue for executing tasks at specific times. 
### Features
- Supports task execution at specific times.
- Supports task insertion, deletion, update.
- A variety of task execution methods are supported, such as executing an HTTP request at a specified time, or placing a task in some third-party queue and letting the client decide how to execute the task itself. 
- Tasks in the queue, support persistent storage, persistent objects can be implemented by themselves, redis persistent implementation is provided by default; support task recovery after program interruption, with higher reliability.
- Multi-thread safety. 
- The delay queue implemented by the time wheel algorithm, no context dependence, and high scalability.

### Use Cases      
This program can be used in the following scenarios:     
- Timed task execution, such as sending emails at a scheduled time. 
- Responsive at specific times, such as turning off a feature at a scheduled time.  
- Cache data expiration processing.  

### Introduction to the Time Wheel Algorithm   
The core algorithm of this queue is the time wheel algorithm, the core of which consists of three main components as follows:    
1. the ring array, which stores the task slot locations.   
2. a task chain table, storing information on delayed tasks.    
3. a second-based, timer, which moves one array element unit above the ring array every second.   

![image](https://user-images.githubusercontent.com/501182/218496661-32edaea4-f2e0-4099-b3ae-5f11fb24aea5.png)

For convenience, the delay queue sets the size of the ring array to 3600 by default, that is to say, it takes one hour for the timer to move around.
How do I determine where to insert a task? We use the following simple algorithm to determine and insert tasks into the task linked list:  
```go
// Get the number of rounds the hour hand needs to rotate
Cycle = (delay seconds) / 3600 
// The number of steps the current hour hand moves forward
Index = (delay seconds) % 3600
```

We only need to insert the task into the slot of the current hour hand position + Index. Each slot points to a task linked list to store tasks. When the Cycle of each task is 0, the task will be automatically executed and then deleted from the linked list.

### How to build  
```sh   
make build  
```   
### How to run   
You can reference this sample file: docker-compose-run-sample.yml in the project.  
To run delayqueue in the docker use follow command:  
```sh
docker-compose -f ./YOUR-DOCKER-COMPOSE-FILE.yml up -d go-delayqueue   
```   
You can build a client using any programming language to interact with the delay queue server or use the [go-delayqueue-client](https://github.com/raymondmars/go-delayqueue-client) to connect it and test it.  

### Contributing  
Anyone is welcome to submit pull requests and suggestions, issues.   

### License  
See [LICENSE](./LICENSE)
