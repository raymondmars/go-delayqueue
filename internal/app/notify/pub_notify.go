package notify

import (
	"errors"
	"fmt"
	"log"

	"github.com/raymondmars/go-delayqueue/internal/app/notify/queue_supplier"
	"github.com/raymondmars/go-delayqueue/internal/pkg/common"
)

type pubNotify struct {
	Service queue_supplier.PubService
}

func (nt *pubNotify) DoDelayTask(contents string) error {
	log.Println(fmt.Sprintf("Do task.....%s", contents))
	configSupplier := common.GetEvnWithDefaultVal("QUEUE_SUPPLIER", "")
	if configSupplier != "" {
		if nt.Service == nil {
			nt.Service = queue_supplier.NewPubService(queue_supplier.StringToQueue(configSupplier))
		}
		if nt.Service != nil {
			return nt.Service.Push(contents)
		} else {
			return errors.New("PubService is nil, please check QUEUE_SUPPLIER")
		}
	}

	return errors.New("QUEUE_SUPPLIER is emtpy")
}
