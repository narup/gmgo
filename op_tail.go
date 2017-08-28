package gmgo

import (
	"fmt"
	"log"
	"time"

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

//MongoTail handles MongoDB tailing event coordination and also
//update destination dbs
type MongoTail struct {
	EventHandler TailEventHandler
	ReImport     bool
}

//Start - starts tailing mongodb oplog.
func (mt MongoTail) Start(dbSession *DbSession) {
	// nil options get initialized to gtm.DefaultOptions()
	bd := time.Duration(750) * time.Millisecond
	options := &gtm.Options{
		After:               nil,        // if nil defaults to LastOpTimestamp
		Filter:              nil,        // only receive inserts in this collection
		OpLogDatabaseName:   nil,        // defaults to "local"
		OpLogCollectionName: nil,        // defaults to a collection prefixed "oplog."
		CursorTimeout:       nil,        // defaults to 100s
		ChannelSize:         20,         // defaults to 20
		BufferSize:          20,         // defaults to 50. used to batch fetch documents on bursts of activity
		BufferDuration:      bd,         // defaults to 750 ms. after this timeout the batch is force fetched
		WorkerCount:         5,          // defaults to 1. number of go routines batch fetching concurrently
		Ordering:            gtm.Oplog,  // defaults to gtm.Oplog. ordering guarantee of events on the output channel
		UpdateDataAsDelta:   false,      // set to true to only receive delta information in the Data field on updates (info straight from oplog)
		DirectReadNs:        []string{}, // set to a slice of namespaces to read data directly from bypassing the oplog
		DirectReadLimit:     5000,       // defaults to 100. the maximum number of documents to return in each direct read query
		DirectReadersPerCol: 10,
		DirectReadFilter:    nil,
		DirectReadBatchSize: 500,
	}
	ctx := gtm.Start(dbSession.Session, options)
	// ctx.OpC is a channel to read ops from
	// ctx.ErrC is a channel to read errors from
	// ctx.Stop() stops all go routines started by gtm.Start
	go func() {
		ctx.DirectReadWg.Wait()
		fmt.Println("[GMGO] imported all the collections")
	}()

	mt.listen(ctx)
}

func (mt MongoTail) listen(ctx *gtm.OpCtx) {
	log.Printf("[GMGO] listening for MongoDB oplog events")
	for {
		// loop forever receiving events
		select {
		case err := <-ctx.ErrC:
			mt.EventHandler.HandleError(err)
		case op := <-ctx.OpC:
			mt.dispatchEvents(op)
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
