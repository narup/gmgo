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

//StartTailing - starts tailing mongodb oplog.
func StartTailing(dbSession *DbSession, evntHandler TailEventHandler) {
	// nil options get initialized to gtm.DefaultOptions()
	options := gtm.DefaultOptions()
	options.DirectReadNs = []string{"phildb.appConfig"}
	ctx := gtm.Start(dbSession.Session, options)
	// ctx.OpC is a channel to read ops from
	// ctx.ErrC is a channel to read errors from
	// ctx.Stop() stops all go routines started by gtm.Start
	go func() {
		ctx.DirectReadWg.Wait()
		fmt.Println("Imported all the collections")
	}()

	listen(ctx, evntHandler)
}

func listen(ctx *gtm.OpCtx, evntHandler TailEventHandler) {
	log.Printf("Listening for MongoDB oplog events")
	for {
		// loop forever receiving events
		select {
		case err := <-ctx.ErrC:
			evntHandler.HandleError(err)
		case op := <-ctx.OpC:
			msg := fmt.Sprintf(`Got op <%v> for object <%v> 
			in database <%v>
			and collection <%v>
			and timestamp <%v>`,
				op.Operation, op.Id, op.GetDatabase(),
				op.GetCollection(), op.Timestamp)
			fmt.Println(msg)

			if op.IsInsert() {
				evntHandler.HandleInsertEvent(newTailEvent(op))
			}
			if op.IsUpdate() {
				evntHandler.HandleUpdateEvent(newTailEvent(op))
			}
			if op.IsDelete() {
				evntHandler.HandleDeleteEvent(newTailEvent(op))
			}
			if op.IsDrop() {
				evntHandler.HandleDeleteEvent(newTailEvent(op))
			}
		}
	}
}
