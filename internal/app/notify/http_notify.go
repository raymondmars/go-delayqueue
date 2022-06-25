package notify

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}
type httpNotify struct {
	Client HttpClient
}

func NewHttpNotify() *httpNotify {
	return &httpNotify{
		Client: &http.Client{
			Timeout: time.Second * 10,
		},
	}
}

func (nt *httpNotify) DoDelayTask(contents string) error {
	log.Info(fmt.Sprintf("Do task.....%s", contents))
	splitValue := strings.Split(contents, "|")
	if len(splitValue) > 1 {
		req, _ := http.NewRequest(http.MethodPost, splitValue[0], bytes.NewBuffer([]byte(splitValue[1])))

		resp, err := nt.Client.Do(req)
		if err != nil {
			log.Warnln(fmt.Sprintf("http notify error: %v", err))
			return errors.New(fmt.Sprintf("http notify error: %s", err.Error()))
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Warnln(fmt.Sprintf("http request response is %d", resp.StatusCode))
			return errors.New("http request response is not 200")
		}
		return nil
	} else {
		log.Warnln(fmt.Sprintf("invalid http notify contents: %s", contents))
		return errors.New("invalid notify contents")
	}

}
