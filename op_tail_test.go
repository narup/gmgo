package gmgo

import "testing"

type mongoEventHandler struct {
}

func (me mongoEventHandler) HandleInsertEvent(event TailEvent) {

}

func (me mongoEventHandler) HandleUpdateEvent(event TailEvent) {

}
func (me mongoEventHandler) HandleDeleteEvent(event TailEvent) {

}
func (me mongoEventHandler) HandleDropEvent(event TailEvent) {

}
func (me mongoEventHandler) HandleError(err error) {

}

type mongoSanitizer struct {
}

func (ms mongoSanitizer) RequireSanitizing(collection string) bool {
	return true
}
func (ms mongoSanitizer) Sanitize(collection string, data map[string]interface{}) map[string]interface{} {
	return data
}

func TestTailing(t *testing.T) {
	session := testDBSession()

	mt := new(MongoTail)
	mt.Sanitizer = mongoSanitizer{}
	mt.EventHandler = mongoEventHandler{}

	mt.Start(session)
}
