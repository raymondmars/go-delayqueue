package godelayqueue

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var testRedisDb = getRedisDb()

func testBeforeClearDb() {
	testRedisDb.RemoveAll()
}
func TestSaveTaskIntoDb(t *testing.T) {
	testBeforeClearDb()
	task := &Task{
		Id:            "123",
		CycleCount:    5,
		WheelPosition: 10,
		TaskType:      "test",
		TaskParams:    "hello,world",
	}
	testRedisDb.Save(task)
	list := testRedisDb.GetList()
	assert.Equal(t, 1, len(list))
	assert.Equal(t, "123 5 10 test hello,world", list[0].String())
}

func TestRemoveTaskFromDb(t *testing.T) {
	testBeforeClearDb()
	task := &Task{
		Id:            "123",
		CycleCount:    5,
		WheelPosition: 10,
		TaskType:      "test",
		TaskParams:    "hello,world",
	}
	testRedisDb.Save(task)
	list := testRedisDb.GetList()
	assert.Equal(t, 1, len(list))
	testRedisDb.Delete(task.Id)
	assert.Equal(t, 0, len(testRedisDb.GetList()))
}

func TestRemoveAllTasksFromDb(t *testing.T) {
	testBeforeClearDb()
	assert.Equal(t, 0, len(testRedisDb.GetList()))
	counts := 1000
	for i := 0; i < counts; i++ {
		task := &Task{
			Id:            fmt.Sprintf("1%d", i),
			CycleCount:    5 * i,
			WheelPosition: 10,
			TaskType:      "test",
			TaskParams:    "hello,world",
		}
		testRedisDb.Save(task)
	}
	assert.Equal(t, counts, len(testRedisDb.GetList()))
	testRedisDb.RemoveAll()
	assert.Equal(t, 0, len(testRedisDb.GetList()))
}

func BenchmarkSaveToDb(b *testing.B) {
	testBeforeClearDb()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		u := uuid.New()
		task := &Task{
			Id:            u.String(),
			CycleCount:    5 * i,
			WheelPosition: 10,
			TaskType:      "test",
			TaskParams:    "hello,world",
		}
		testRedisDb.Save(task)
	}
	b.StopTimer()
	testRedisDb.RemoveAll()
}
