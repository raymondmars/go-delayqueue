package queue_supplier

import (
	"strings"
)

type QueueSupplier uint

const (
	RABBIT_QUEUE QueueSupplier = iota + 1
	KAFAK_QUEUE
)

type PubService interface {
	Push(contents string) error
}

func NewPubService(supplier QueueSupplier) PubService {
	switch supplier {
	case RABBIT_QUEUE:
		return &rabbitmq{}
	case KAFAK_QUEUE:
		return &kafka{}
	default:
		return nil
	}
}

func StringToQueue(q string) QueueSupplier {
	switch strings.ToLower(q) {
	case "rabbitmq":
		return RABBIT_QUEUE
	case "kafka":
		return KAFAK_QUEUE
	default:
		return QueueSupplier(0)
	}
}
