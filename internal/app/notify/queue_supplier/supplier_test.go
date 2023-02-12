package queue_supplier

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringToQueue(t *testing.T) {
	assert.Equal(t, QueueSupplier(0), StringToQueue(""))
	assert.Equal(t, RABBIT_QUEUE, StringToQueue("rabbitmq"))
	assert.Equal(t, KAFAK_QUEUE, StringToQueue("kafka"))
	assert.Equal(t, QueueSupplier(0), StringToQueue("3"))
}

func TestNewPubService(t *testing.T) {
	service := NewPubService(RABBIT_QUEUE)
	assert.IsType(t, &rabbitmq{}, service)

	service = NewPubService(KAFAK_QUEUE)
	assert.IsType(t, &kafka{}, service)

	service = NewPubService(KAFAK_QUEUE + 1)
	assert.Equal(t, nil, service)
}
