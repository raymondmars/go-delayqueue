package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/0RaymondJiang0/go-delayqueue/internal/app/message"
	"github.com/0RaymondJiang0/go-delayqueue/internal/pkg/common"

	log "github.com/sirupsen/logrus"
)

const (
	DEFAULT_HOST      = "127.0.0.1"
	DEFAULT_PORT      = "3450"
	DEFAULT_CONN_TYPE = "tcp"
)

func main() {
	// init logger
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)

	host := common.GetEvnWithDefaultVal("CONN_HOST", DEFAULT_HOST)
	port := common.GetEvnWithDefaultVal("CONN_PORT", DEFAULT_PORT)
	conType := common.GetEvnWithDefaultVal("CONN_TYPE", DEFAULT_CONN_TYPE)

	l, err := net.Listen(conType, fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		log.Error("Listen error: ", err)
	}
	defer l.Close()
	log.Infoln("Listening on " + host + ":" + port)
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
	// client send command echo -n "ping" | nc localhost 3450
	//telnet localhost 3450
	// quit: ctrl+], 然后，输入 quit
	//telnet x.x.x.x xxxx <<EOF
}

// Handles incoming requests.
// func handleRequest2(conn net.Conn) {
// 	buf := make([]byte, 0, 1024) // big buffer
// 	tmp := make([]byte, 16)      // each chunck package
// 	k := 0
// 	for {
// 		n, err := conn.Read(tmp)
// 		if err != nil {
// 			if err != io.EOF {
// 				fmt.Println("read error:", err)
// 			}
// 			break
// 		}
// 		// if k == 0 {
// 		// 	cmd := string(tmp)

// 		// }
// 		fmt.Println(string(tmp[0]))
// 		buf = append(buf, tmp[:n]...)
// 		k++
// 	}

// 	log.Infoln("received:", string(buf))

// 	if string(buf) == "ping" {
// 		conn.Write([]byte("pong"))
// 	} else {
// 		// Send a response back to person contacting us.
// 		conn.Write([]byte("Message received."))
// 	}
// 	// Close the connection when you're done with it.
// 	conn.Close()
// }

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
	fmt.Println(contents)
	processor := message.NewProcessor()
	processor.Receive(conn, contents)
}
