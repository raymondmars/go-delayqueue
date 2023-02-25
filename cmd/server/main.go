package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/raymondmars/go-delayqueue/internal/app/core"
	"github.com/raymondmars/go-delayqueue/internal/app/message"
	"github.com/raymondmars/go-delayqueue/internal/app/notify"
	"github.com/raymondmars/go-delayqueue/internal/pkg/common"

	log "github.com/sirupsen/logrus"
)

const (
	DEFAULT_HOST      = "0.0.0.0"
	DEFAULT_PORT      = "3450"
	DEFAULT_CONN_TYPE = "tcp"
)

var delayQueue *core.DelayQueue

// client send command echo -n "ping" | nc localhost 3450
// telnet localhost 3450
// quit: ctrl+] and input quit
//telnet x.x.x.x xxxx <<EOF
func main() {
	welcome()
	// init logger
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	if common.IsDevEnv() {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	host := common.GetEvnWithDefaultVal("CONN_HOST", DEFAULT_HOST)
	port := common.GetEvnWithDefaultVal("CONN_PORT", DEFAULT_PORT)
	conType := common.GetEvnWithDefaultVal("CONN_TYPE", DEFAULT_CONN_TYPE)

	delayQueue = core.GetDelayQueue(notify.BuildExecutor)
	go delayQueue.Start()

	l, err := net.Listen(conType, fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		log.Error("Listen error: ", err)
	}
	defer l.Close()
	println("-> ready for accepting any connection on " + host + ":" + port)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	reader := bufio.NewReader(conn)
	contents := []string{}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Error(err)
			}
			break
		}
		if strings.TrimSpace(line) == "" {
			break
		}
		line = strings.Trim(line, "\n")
		line = strings.Trim(line, "\r")

		contents = append(contents, line)
	}
	processor := message.NewProcessor()
	resp := processor.Receive(delayQueue, contents)
	conn.Write([]byte(resp.String()))
	conn.Close()
}
