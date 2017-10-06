package gmgo

import (
	"fmt"
	"testing"
)

type mongoEventHandler struct {
}

func (me mongoEventHandler) HandleInsertEvent(event TailEvent) {
	fmt.Printf("%+v", event)
}

func (me mongoEventHandler) HandleUpdateEvent(event TailEvent) {
	fmt.Printf("%+v", event)
}

func (me mongoEventHandler) HandleDeleteEvent(event TailEvent) {
	fmt.Printf("%+v", event)
}
func (me mongoEventHandler) HandleDropEvent(event TailEvent) {
	fmt.Printf("%+v", event)
}
func (me mongoEventHandler) HandleError(err error) {
	fmt.Printf("%s", err)
}

func xxTestTailing(t *testing.T) {
	session := testDBSession()

	mt := new(MongoTail)
	mt.EventHandler = mongoEventHandler{}

	mt.Start(session)
}
