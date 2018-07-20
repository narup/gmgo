package gmgo

import (
	"fmt"
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"
)

func testDBSession() *DbSession {
	dbConfig := DbConfig{HostURL: "mongodb://localhost:27017/phildb-prod", DBName: "phildb-prod", UserName: "", Password: "", Mode: 1}
	err := Setup(dbConfig)
	if err != nil {
		fmt.Printf("Connection failed %s", err)
		return nil
	}

	philDB, err := Get("phildb-prod")
	if err != nil {
		fmt.Printf("Get db failed %s", err)
		return nil
	}

	fmt.Println(philDB.Config.DBName)

	return philDB.Session()
}

//test user object
type user struct {
	ID          bson.ObjectId `json:"id" bson:"_id,omitempty"`
	CreatedDate *time.Time    `json:"createdDate" bson:"createdDate,omitempty"`
	UpdatedDate *time.Time    `json:"updatedDate" bson:"updatedDate,omitempty"`
	FullName    string        `json:"fullName" bson:"fullName" binding:"required"`
	Email       string        `json:"email" bson:"email" binding:"required"`
	PhoneNumber string        `json:"phoneNumber" bson:"phoneNumber,omitempty"`
	FromNumber  string        `json:"fromNumber" bson:"fromNumber,omitempty"`
	ZipCode     string        `json:"zipCode" bson:"zipCode" binding:"required"`
	City        string        `json:"city" bson:"city,omitempty"`
	State       string        `json:"state" bson:"state,omitempty"`
}

func (u *user) CollectionName() string {
	return "rexUser"
}

var TotalUserCount = -1

func xxTestPagedQuery(t *testing.T) {
	session := testDBSession()
	defer session.Close()

	count := 0
	pd := session.DocumentIterator(Q{"state": "CA"}, "rexUser")
	//The Snashopt ($snapshot) operator prevents the cursor from returning a document more than
	//once because an intervening write operation results in a move of the document.
	pd.Load(IteratorConfig{PageSize: 50, Snapshot: true})
	for pd.HasMore() {
		usr := new(user)
		err := pd.Next(usr)
		if err != nil {
			println(err.Error())
			return
		}
		count = count + 1
	}

	if TotalUserCount == -1 {
		println("Test failed. Set the value of TotalUserCount")
	} else if count == TotalUserCount {
		println("Test passed")
	} else {
		println("Test failed")
	}
}

func xxTestSorting(t *testing.T) {
	session := testDBSession()
	defer session.Close()

	itr := session.DocumentIterator(Q{"state": "CA"}, "rexUser")
	itr.Load(IteratorConfig{Limit: 20, SortBy: []string{"-_id"}})

	result, err := itr.All(new(user))
	if err != nil {
		println(err)
		return
	}
	users := result.([]*user)
	for _, usr := range users {
		println(usr.ID.Hex() + " -- " + usr.CreatedDate.String())
	}
}

func xxTestBatchAll(t *testing.T) {
	session := testDBSession()
	defer session.Close()

	itr := session.DocumentIterator(Q{"state": "CA"}, "rexUser")
	itr.Load(IteratorConfig{})
	result, err := itr.All(new(user))
	if err != nil {
		println(err)
		return
	}
	users := result.([]*user)
	println(len(users))
}

func xxTestReadGridFSFile(t *testing.T) {
	session := testDBSession()

	file := new(File)
	file.ByteLength = 1024
	err := session.ReadFile("5713f1b0e4b067fc28d6fbaa", "rex_files", file)
	if err != nil {
		t.Errorf("File read failed %s", err)
		return
	}
	fmt.Printf("File name:%s, Content Type: %s\n", file.Name, file.ContentType)
}
