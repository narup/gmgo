package gmgo

import (
	"fmt"

	"github.com/rwynn/gtm"
)

//StartTailing - starts tailing mongodb oplog.
func StartTailing(dbSession *DbSession) {
	// nil options get initialized to gtm.DefaultOptions()
	ctx := gtm.Start(dbSession.Session, nil)
	// ctx.OpC is a channel to read ops from
	// ctx.ErrC is a channel to read errors from
	// ctx.Stop() stops all go routines started by gtm.Start
	for {
		// loop forever receiving events
		select {
		case err := <-ctx.ErrC:
			// handle errors
			fmt.Println(err)
		case op := <-ctx.OpC:
			// op will be an insert, delete, update, or drop to mongo
			// you can check which by calling
			// op.IsInsert(), op.IsDelete(), op.IsUpdate(), or op.IsDrop()
			// op.Data will get you the full document for inserts and updates
			msg := fmt.Sprintf(`Got op <%v> for object <%v> 
			in database <%v>
			and collection <%v>
			and data <%v>
			and timestamp <%v>`,
				op.Operation, op.Id, op.GetDatabase(),
				op.GetCollection(), op.Data, op.Timestamp)
			fmt.Println(msg) // or do something more interesting
		}
	}
}
