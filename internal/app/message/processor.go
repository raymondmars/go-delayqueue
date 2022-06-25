package message

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/raymondmars/go-delayqueue/internal/app/core"
	"github.com/raymondmars/go-delayqueue/internal/app/notify"
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
	AUTH_FAILED          ResponseErrCode = 1011
	INVALID_COMMAND      ResponseErrCode = 1012
	INVALID_PUSH_MESSAGE ResponseErrCode = 1013
)

type Response struct {
	Status    ResponseStatusCode
	ErrorCode ResponseErrCode
	Message   string
}

func (r Response) String() string {
	if r.Status == Ok {
		return fmt.Sprintf("%d,%s", r.Status, r.Message)
	} else {
		return fmt.Sprintf("%d,%d,%s", r.Status, r.ErrorCode, r.Message)
	}
}

type Processor interface {
	Receive(queue *core.DelayQueue, contents []string) *Response
}

var messageAuthCode = "0_ONMARS_1"

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
	// message format is:
	// first line is auth code; 0 ----------|
	// second line is cmd name; 1 ----------|
	// third line is delay seconds; 2 ----------|
	// fourth line is task mode;    3 ----------|
	// fifth line is http url if task mode is HTTP
	// or is message contents if task mode is PubSub; 4 ----------|
	// sixth line is message contents if task mode is HTTP; 5 ----------|
	if len(contents) == 0 || len(contents) > 6 {
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
	delaySeconds, _ := strconv.Atoi(contents[2])
	if delaySeconds == 0 {
		return &Response{
			Status:    Fail,
			ErrorCode: INVALID_MESSAGE,
		}
	}
	cmd := Command(code)

	switch cmd {
	case Test:
		return &Response{
			Status:  Ok,
			Message: "pong",
		}
	case Push:
		wayCode, _ := strconv.Atoi(contents[3])
		switch notify.NotifyMode(wayCode) {
		case notify.HTTP:
			if len(contents) != 6 {
				return &Response{
					Status:    Fail,
					ErrorCode: INVALID_PUSH_MESSAGE,
				}
			}
			targetUrl := contents[4]
			taskData := contents[5]
			_, err := queue.Push(time.Duration(delaySeconds)*time.Second, notify.HTTP, fmt.Sprintf("%s|%s", targetUrl, taskData))
			if err != nil {
				return &Response{
					Status:    Fail,
					ErrorCode: INVALID_PUSH_MESSAGE,
					Message:   err.Error(),
				}
			}
			return &Response{
				Status:  Ok,
				Message: "push done",
			}
		case notify.SubPub:
			if len(contents) != 5 {
				return &Response{
					Status:    Fail,
					ErrorCode: INVALID_PUSH_MESSAGE,
				}
			}
			log.Warnln("PubSub is not implemented.")
			return &Response{
				Status:    Fail,
				ErrorCode: INVALID_PUSH_MESSAGE,
				Message:   "PubSub is not implemented.",
			}
		default:
			return &Response{
				Status:    Fail,
				ErrorCode: INVALID_PUSH_MESSAGE,
			}
		}

	case Subscribe:
		return &Response{
			Status:  Ok,
			Message: "subscribe done",
		}
	default:
		return &Response{
			Status:    Fail,
			ErrorCode: INVALID_COMMAND,
		}
	}
}
