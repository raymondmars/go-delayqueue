package queue_supplier

import (
	"errors"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/raymondmars/go-delayqueue/internal/pkg/common"
	log "github.com/sirupsen/logrus"
)

var amqpURI = common.GetEvnWithDefaultVal("AMQP_URI", "amqp://guest:guest@localhost:5672/")

const (
	exchangeName = ""
)

type rabbitmq struct {
}

func (r *rabbitmq) Push(contents string) error {
	splitValue := strings.Split(contents, "|")
	if len(splitValue) > 1 {
		queueName := splitValue[0]
		pushContents := splitValue[1]
		return r.pushToQueue(queueName, []byte(pushContents))

	} else {
		log.Warnln(fmt.Sprintf("invalid rabbitmq queue notify contents: %s", contents))
		return errors.New("invalid notify contents")
	}
}

func (r *rabbitmq) pushToQueue(queueName string, body []byte) error {
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Printf("connect mq failed: %v", err)
		return err
	}
	defer connection.Close()

	//创建一个Channel
	channel, err := connection.Channel()
	if err != nil {
		log.Printf("create channel failed: %v", err)
		return err
	}
	defer channel.Close()

	//创建一个queue
	q, err := channel.QueueDeclare(
		string(queueName), // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Printf("create queue failed: %v", err)
		return err
	}

	err = channel.Publish(
		exchangeName, // exchange
		q.Name,       // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			DeliveryMode:    amqp.Persistent,
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            body,
		})

	if err != nil {
		log.Printf("error: %v", err)
	}

	return err
}
