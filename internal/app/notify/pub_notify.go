package notify

import (
	"fmt"
	"log"
)

type pubNotify struct {
}

func (nt *pubNotify) DoDelayTask(contents string) error {
	log.Println(fmt.Sprintf("Do task.....%s", contents))
	return nil
}
