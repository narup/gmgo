package main

import (
	"fmt"

	"github.com/narup/gmgo"
)

type mongoEventHandler struct {
}

func (me mongoEventHandler) HandleInsertEvent(event gmgo.TailEvent) {
	fmt.Printf("%+v", event)
}

func (me mongoEventHandler) HandleUpdateEvent(event gmgo.TailEvent) {
	fmt.Printf("%+v", event)
}

func (me mongoEventHandler) HandleDeleteEvent(event gmgo.TailEvent) {
	fmt.Printf("%+v", event)
}
func (me mongoEventHandler) HandleDropEvent(event gmgo.TailEvent) {
	fmt.Printf("%+v", event)
}
func (me mongoEventHandler) HandleError(err error) {
	fmt.Printf("%s", err)
}

func main() {
	session := testDBSession()

	mt := new(gmgo.MongoTail)
	mt.EventHandler = mongoEventHandler{}

	mt.Start(session)
}

func testDBSession() *gmgo.DbSession {
	dbConfig := gmgo.DbConfig{HostURL: "mongodb://localhost:27017/phildb", DBName: "phildb", UserName: "", Password: "", Mode: 1}
	err := gmgo.Setup(dbConfig)
	if err != nil {
		fmt.Printf("Connection failed %s", err)
		return nil
	}

	philDB, err := gmgo.Get("phildb")
	if err != nil {
		fmt.Printf("Get db failed %s", err)
		return nil
	}

	fmt.Println(philDB.Config.DBName)

	return philDB.Session()
}
