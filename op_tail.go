package gmgo

import (
	"fmt"
	"log"

	"github.com/rwynn/gtm"
)

//TailEvent defines oplog tail event
type TailEvent struct {
	ID         interface{}
	Collection string
	Data       map[string]interface{}
}

func newTailEvent(op *gtm.Op) TailEvent {
	te := TailEvent{}
	te.ID = op.Id
	te.Data = op.Data
	te.Collection = op.GetCollection()

	return te
}

//TailEventHandler interface for handling tail event
type TailEventHandler interface {
	HandleInsertEvent(event TailEvent)
	HandleUpdateEvent(event TailEvent)
	HandleDeleteEvent(event TailEvent)
	HandleDropEvent(event TailEvent)
	HandleError(err error)
}

//Sanitizer sanitizes data before saving it to destination
type Sanitizer interface {
	RequireSanitizing(collection string) bool
	Sanitize(collection string, data map[string]interface{}) map[string]interface{}
}

//OutputConnector connector for output db
type OutputConnector struct {
	ConnectionURL string
}

//MongoTail handles MongoDB tailing event coordination and also
//update destination dbs
type MongoTail struct {
	Sanitizer        Sanitizer
	EventHandler     TailEventHandler
	ReImport         bool
	MappingFilePath  string
	OutputConnectors []OutputConnector
}

//Start - starts tailing mongodb oplog.
func (mt MongoTail) Start(dbSession *DbSession) {
	// nil options get initialized to gtm.DefaultOptions()
	options := gtm.DefaultOptions()
	ctx := gtm.Start(dbSession.Session, options)
	// ctx.OpC is a channel to read ops from
	// ctx.ErrC is a channel to read errors from
	// ctx.Stop() stops all go routines started by gtm.Start
	go func() {
		ctx.DirectReadWg.Wait()
		fmt.Println("Imported all the collections")
	}()

	mt.listen(ctx)
}

func (mt MongoTail) listen(ctx *gtm.OpCtx) {
	log.Printf("Listening for MongoDB oplog events")
	for {
		// loop forever receiving events
		select {
		case err := <-ctx.ErrC:
			mt.EventHandler.HandleError(err)
		case op := <-ctx.OpC:
			msg := fmt.Sprintf(`Got op <%v> for object <%v> 
			in database <%v>
			and collection <%v>
			and timestamp <%v>`,
				op.Operation, op.Id, op.GetDatabase(),
				op.GetCollection(), op.Timestamp)
			fmt.Println(msg)

			mt.dispatchEvents(op)

			//sanitize if needed
			if mt.Sanitizer.RequireSanitizing(op.GetCollection()) {
				sanitizedData := mt.Sanitizer.Sanitize(op.GetCollection(), op.Data)
				mt.writeOutput(sanitizedData)
			} else {
				mt.writeOutput(op.Data)
			}
		}
	}
}

func (mt MongoTail) dispatchEvents(op *gtm.Op) {
	if op.IsInsert() {
		mt.EventHandler.HandleInsertEvent(newTailEvent(op))
	}
	if op.IsUpdate() {
		mt.EventHandler.HandleUpdateEvent(newTailEvent(op))
	}
	if op.IsDelete() {
		mt.EventHandler.HandleDeleteEvent(newTailEvent(op))
	}
	if op.IsDrop() {
		mt.EventHandler.HandleDeleteEvent(newTailEvent(op))
	}
}

func (mt MongoTail) writeOutput(data map[string]interface{}) {

}
