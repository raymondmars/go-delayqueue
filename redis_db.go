package godelayqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/go-redis/redis/v8"
)

var lock sync.Once

const (
	// task key prefix
	TASK_KEY_PREFIX = "delaytk_"
)

var redisInstance *redisDb

type redisDb struct {
	Client *redis.Client
	// task list store task id
	TaskListKey string
	Context     context.Context
}

// singleton method
func getRedisDb() *redisDb {

	lock.Do(func() {
		dbNumber, _ := strconv.Atoi(GetEvnWithDefaultVal("REDIS_DB", "0"))
		redisInstance = &redisDb{
			Client: redis.NewClient(&redis.Options{
				Addr:     GetEvnWithDefaultVal("REDIS_ADDR", "localhost:6379"),
				Password: GetEvnWithDefaultVal("REDIS_PWD", ""),
				DB:       dbNumber,
			}),
			TaskListKey: GetEvnWithDefaultVal("DELAY_QUEUE_LIST_KEY", "__delay_queue_list__"),
			Context:     context.Background(),
		}
	})

	return redisInstance
}

// save task to redis
func (rd *redisDb) Save(task *Task) error {
	task.Next = nil
	tk, err := json.Marshal(task)
	if err != nil {
		log.Println(err)
		return err
	}

	if string(tk) != "" {
		key := fmt.Sprintf("%s%s", TASK_KEY_PREFIX, task.Id)
		if val, _ := rd.Client.Get(rd.Context, key).Result(); val == "" {
			rd.Client.LPush(rd.Context, rd.TaskListKey, task.Id)
		}
		result := rd.Client.Set(rd.Context, key, string(tk), 0)
		return result.Err()

	} else {
		return errors.New("task is emtpy")
	}

}

// get list from redis
func (rd *redisDb) GetList() []*Task {
	listResult := rd.Client.LRange(rd.Context, rd.TaskListKey, 0, -1)
	listArray, _ := listResult.Result()
	tasks := []*Task{}
	if listArray != nil && len(listArray) > 0 {
		for _, item := range listArray {
			key := fmt.Sprintf("%s%s", TASK_KEY_PREFIX, item)
			taskCmd := rd.Client.Get(rd.Context, key)
			if val, err := taskCmd.Result(); err == nil {
				entity := Task{}
				err := json.Unmarshal([]byte(val), &entity)
				if err == nil {
					tasks = append(tasks, &entity)
				}
			}
		}
	}
	return tasks
}

// remove task from redis
func (rd *redisDb) Delete(taskId string) error {
	rd.Client.LRem(rd.Context, rd.TaskListKey, 0, taskId)
	rd.Client.Del(rd.Context, fmt.Sprintf("%s%s", TASK_KEY_PREFIX, taskId))

	return nil
}

// remove all tasks from redis
func (rd *redisDb) RemoveAll() error {
	listResult := rd.Client.LRange(rd.Context, rd.TaskListKey, 0, -1)
	listArray, _ := listResult.Result()
	if listArray != nil && len(listArray) > 0 {
		for _, tkId := range listArray {
			rd.Client.Del(rd.Context, fmt.Sprintf("%s%s", TASK_KEY_PREFIX, tkId))
		}
	}
	rd.Client.Del(rd.Context, rd.TaskListKey)
	return nil
}
