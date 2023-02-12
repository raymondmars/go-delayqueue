package notify

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockPubService struct{}

func (c *mockPubService) Push(contents string) error {
	return nil
}

type mockErrPubService struct{}

func (c *mockErrPubService) Push(contents string) error {
	return errors.New("push failed with errors")
}

func TestPubSubNofityDoDelayTask(t *testing.T) {
	normalMockPubService := &mockPubService{}
	notify := &pubNotify{Service: normalMockPubService}
	assert.Error(t, errors.New("QUEUE_SUPPLIER is emtpy"), notify.DoDelayTask("test"))

	os.Setenv("QUEUE_SUPPLIER", "rabbitmq-err")
	defer os.Unsetenv("QUEUE_SUPPLIER")

	notify2 := &pubNotify{}
	assert.Error(t, errors.New("PubService is nil, please check QUEUE_SUPPLIER"), notify2.DoDelayTask("test"))

	normalMockPubService2 := &mockPubService{}
	okNotify := &pubNotify{Service: normalMockPubService2}
	os.Setenv("QUEUE_SUPPLIER", "rabbitmq")
	assert.Equal(t, nil, okNotify.DoDelayTask("test"))

	errMockPubService := &mockErrPubService{}
	errNotify := &pubNotify{Service: errMockPubService}
	assert.Error(t, errors.New("push failed with errors"), errNotify.DoDelayTask("test"))
}
