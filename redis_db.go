package godelayqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	"gopkg.in/redis.v3"
)

var lock sync.Once

const (
	//每个task存储到redis当中的key前缀，便于与其他数据的区分度
	TASK_KEY_PREFIX = "delaytk_"
)

var redisInstance *redisDb

type redisDb struct {
	Client *redis.Client
	//task 列表key，该redis列表用于存储task的任务ID
	TaskListKey string
}

//单列方法
func getRedisDb() *redisDb {

	lock.Do(func() {
		dbNumber, _ := strconv.Atoi(GetEvnWithDefaultVal("REDIS_DB", "0"))
		redisInstance = &redisDb{
			Client: redis.NewClient(&redis.Options{
				Addr:     GetEvnWithDefaultVal("REDIS_ADDR", "localhost:6379"),
				Password: GetEvnWithDefaultVal("REDIS_PWD", ""), // no password set
				DB:       int64(dbNumber),                       // use  DB
			}),
			TaskListKey: GetEvnWithDefaultVal("DELAY_QUEUE_LIST_KEY", "__delay_queue_list__"),
		}
	})

	return redisInstance
}

//将task保存到redis, 将task id 存入 list, task 整体内容放入其 id 对应数据槽
func (rd *redisDb) Save(task *Task) error {
	tk, err := json.Marshal(task)
	if err != nil {
		log.Println(err)
		return err
	}
	if string(tk) != "" {
		key := fmt.Sprintf("%s%s", TASK_KEY_PREFIX, task.Id)
		//如果key不存在
		if val, _ := rd.Client.Get(key).Result(); val == "" {
			rd.Client.LPush(rd.TaskListKey, task.Id)
		}
		result := rd.Client.Set(key, string(tk), 0)
		return result.Err()

	} else {
		return errors.New("task is emtpy")
	}

}

//从 redis 中恢复持久化的 task 列表
func (rd *redisDb) GetList() []*Task {
	listResult := rd.Client.LRange(rd.TaskListKey, 0, -1)
	listArray, _ := listResult.Result()
	tasks := []*Task{}
	if listArray != nil && len(listArray) > 0 {
		for _, item := range listArray {
			key := fmt.Sprintf("%s%s", TASK_KEY_PREFIX, item)
			taskCmd := rd.Client.Get(key)
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

//从 redis 从删除某个 task
func (rd *redisDb) Delete(taskId string) error {
	rd.Client.LRem(rd.TaskListKey, 0, taskId)
	rd.Client.Del(fmt.Sprintf("%s%s", TASK_KEY_PREFIX, taskId))

	return nil
}

//从 redis 中清空所有 task
func (rd *redisDb) RemoveAll() error {
	listResult := rd.Client.LRange(rd.TaskListKey, 0, -1)
	listArray, _ := listResult.Result()
	if listArray != nil && len(listArray) > 0 {
		for _, tkId := range listArray {
			rd.Client.Del(fmt.Sprintf("%s%s", TASK_KEY_PREFIX, tkId))
		}
	}
	rd.Client.Del(rd.TaskListKey)
	return nil
}
