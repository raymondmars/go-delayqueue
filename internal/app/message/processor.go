package message

import (
	"fmt"
	"net"
	"strconv"
)

type Processor interface {
	Receive(conn net.Conn, contents []string)
}
type ResponseMsgCode uint

const (
	OK              ResponseMsgCode = 1000
	INVALID_MESSAGE ResponseMsgCode = 1010
	AUTH_FAILED     ResponseMsgCode = 1011
	INVALID_COMMAND ResponseMsgCode = 1012
)

var messageAuthCode = "0_ONMARS_1"

type processor struct {
}

func NewProcessor() Processor {
	return &processor{}
}

// receive message from client
func (p *processor) Receive(conn net.Conn, contents []string) {
	defer conn.Close()
	// message format is: first line is auth code;
	// second line is cmd name;
	// third line is message contents;
	if len(contents) == 0 || len(contents) > 3 {
		conn.Write([]byte(fmt.Sprintf("%d", INVALID_MESSAGE)))
		return
	}
	if contents[0] != messageAuthCode {
		conn.Write([]byte(fmt.Sprintf("%d", AUTH_FAILED)))
		return
	}
	code, _ := strconv.Atoi(contents[1])
	cmd := Command(code)

	switch cmd {
	case Test:
		conn.Write([]byte("pong"))
		return
	case Push:
		if len(contents) != 3 {
			conn.Write([]byte(fmt.Sprintf("%d", INVALID_MESSAGE)))
			return
		}
		conn.Write([]byte("push done"))
		return
	case Subscribe:
		conn.Write([]byte("subscribe done"))
		return
	default:
		conn.Write([]byte(fmt.Sprintf("%d", INVALID_COMMAND)))
		return
	}
}
