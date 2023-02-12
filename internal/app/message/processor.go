package message

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/raymondmars/go-delayqueue/internal/app/core"
	"github.com/raymondmars/go-delayqueue/internal/app/notify"
	"github.com/raymondmars/go-delayqueue/internal/pkg/common"
)

type ResponseStatusCode uint
type ResponseErrCode uint

const (
	Ok ResponseStatusCode = iota + 1
	Fail
)

const (
	NOT_READY            ResponseErrCode = 1000
	INVALID_MESSAGE      ResponseErrCode = 1010
	AUTH_FAILED          ResponseErrCode = 1012
	INVALID_DELAY_TIME   ResponseErrCode = 1014
	INVALID_COMMAND      ResponseErrCode = 1016
	INVALID_PUSH_MESSAGE ResponseErrCode = 1018
	UPDATE_FAILED        ResponseErrCode = 1020
	DELETE_FAILED        ResponseErrCode = 1022
)

type Response struct {
	Status    ResponseStatusCode
	ErrorCode ResponseErrCode
	Message   string
}

func (r Response) String() string {
	if r.Status == Ok {
		return fmt.Sprintf("%d|%s", r.Status, r.Message)
	} else {
		return fmt.Sprintf("%d|%d|%s", r.Status, r.ErrorCode, r.Message)
	}
}

type Processor interface {
	Receive(queue *core.DelayQueue, contents []string) *Response
}

var messageAuthCode = common.GetEvnWithDefaultVal("MESSAGE_AUTH_CODE", "0_ONMARS_1")

type processor struct {
}

func NewProcessor() Processor {
	return &processor{}
}

// receive message from client
func (p *processor) Receive(queue *core.DelayQueue, contents []string) *Response {
	// defer conn.Close()
	if queue == nil || !queue.IsReady {
		return &Response{
			Status:    Fail,
			ErrorCode: NOT_READY,
		}
	}
	log.Info("Receive message:" + strings.Join(contents, " | "))
	// message format is:
	// first line is auth code; 0 ----------|
	// second line is cmd name; 1 ----------|
	// third line is delay seconds or task id(for update, delete); 2 ----------|
	// fourth line is notify way 3 ----------|
	// fifth line http url if task mode is HTTP, or is queueName if task mode is PubSub; 4 ----------|
	// sixth line is message contents; 5 ----------|
	if len(contents) < 2 || len(contents) > 6 {
		return &Response{
			Status:    Fail,
			ErrorCode: INVALID_MESSAGE,
		}
	}

	if contents[0] != messageAuthCode {
		return &Response{
			Status:    Fail,
			ErrorCode: AUTH_FAILED,
		}
	}

	code, _ := strconv.Atoi(contents[1])
	cmd := Command(code)
	if cmd == Test {
		return &Response{
			Status:  Ok,
			Message: "pong",
		}
	}

	switch cmd {
	case Push:
		if len(contents) != 6 {
			return &Response{
				Status:    Fail,
				ErrorCode: INVALID_PUSH_MESSAGE,
			}
		}
		delaySeconds, _ := strconv.Atoi(contents[2])
		if delaySeconds <= 0 {
			return &Response{
				Status:    Fail,
				ErrorCode: INVALID_DELAY_TIME,
			}
		}
		wayCode, _ := strconv.Atoi(contents[3])
		taskTarget := contents[4]
		taskData := contents[5]
		switch notify.NotifyMode(wayCode) {
		case notify.HTTP:
			return p.executePush(queue, taskTarget, taskData, delaySeconds, notify.HTTP)
		case notify.SubPub:
			return p.executePush(queue, taskTarget, taskData, delaySeconds, notify.SubPub)
		default:
			return &Response{
				Status:    Fail,
				ErrorCode: INVALID_PUSH_MESSAGE,
				Message:   "Invalid notify way.",
			}
		}

	case Update:
		if len(contents) != 6 {
			return &Response{
				Status:    Fail,
				ErrorCode: INVALID_PUSH_MESSAGE,
			}
		}
		taskId := strings.TrimSpace(contents[2])
		wayCode, _ := strconv.Atoi(contents[3])
		taskTarget := contents[4]
		taskData := contents[5]
		err := queue.UpdateTask(taskId, notify.NotifyMode(wayCode), fmt.Sprintf("%s|%s", taskTarget, taskData))
		if err != nil {
			return &Response{
				Status:    Fail,
				ErrorCode: UPDATE_FAILED,
				Message:   err.Error(),
			}
		} else {
			return &Response{
				Status:  Ok,
				Message: taskId,
			}
		}
	case Delete:
		if len(contents) != 3 {
			return &Response{
				Status:    Fail,
				ErrorCode: INVALID_PUSH_MESSAGE,
			}
		}
		taskId := strings.TrimSpace(contents[2])
		err := queue.DeleteTask(taskId)
		if err != nil {
			return &Response{
				Status:    Fail,
				ErrorCode: DELETE_FAILED,
				Message:   err.Error(),
			}
		} else {
			return &Response{
				Status:  Ok,
				Message: taskId,
			}
		}
	default:
		return &Response{
			Status:    Fail,
			ErrorCode: INVALID_COMMAND,
			Message:   "Invalid command.",
		}
	}
}

func (p *processor) executePush(queue *core.DelayQueue, target, data string, delaySeconds int, mode notify.NotifyMode) *Response {
	task, err := queue.Push(time.Duration(delaySeconds)*time.Second, mode, fmt.Sprintf("%s|%s", target, data))
	if err != nil {
		return &Response{
			Status:    Fail,
			ErrorCode: INVALID_PUSH_MESSAGE,
			Message:   err.Error(),
		}
	}
	return &Response{
		Status:  Ok,
		Message: task.Id,
	}
}
