package notify

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

var mockBody = ioutil.NopCloser(bytes.NewBufferString("Hello World"))

type mockHttpOkClient struct{}

func (c *mockHttpOkClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       mockBody,
	}, nil
}

type mockHttpErrClient struct{}

func (c *mockHttpErrClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{}, errors.New("invalid request data")
}

type mockHttpNoneOkClient struct{}

func (c *mockHttpNoneOkClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       mockBody,
	}, nil
}

func TestHttpNofityDoDelayTask(t *testing.T) {
	notifyOk := &httpNotify{
		Client: &mockHttpOkClient{},
	}
	notifyErr := &httpNotify{
		Client: &mockHttpErrClient{},
	}
	notifyNoneOk := &httpNotify{
		Client: &mockHttpNoneOkClient{},
	}

	assert.Error(t, errors.New("invalid notify contents"), notifyOk.DoDelayTask("test"))
	assert.Equal(t, nil, notifyOk.DoDelayTask("https://google.com|test"))
	assert.Error(t, errors.New(fmt.Sprintf("http notify error: %s", "invalid request data")), notifyErr.DoDelayTask("https://google.com|test"))
	assert.Error(t, errors.New("http request response is not 200"), notifyNoneOk.DoDelayTask("https://google.com|test"))
}
