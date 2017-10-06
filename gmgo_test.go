package gmgo

import (
	"fmt"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
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

func xxTestPagedQuery(t *testing.T) {
	session := testDBSession()
	defer session.Close()

	count := 0
	dedupMap := make(map[string]string)

	pd := session.DocumentIterator(Q{}, new(user))
	//The Snashopt ($snapshot) operator prevents the cursor from returning a document more than
	//once because an intervening write operation results in a move of the document.
	pd.Load(IteratorConfig{PageSize: 200, Snapshot: true})
	for pd.HasMore() {
		count = count + 1
		result, err := pd.Next()
		if err != nil {
			println(err.Error())
			return
		}

		u := result.(*user)
		if dedupMap[u.ID.Hex()] == "" {
			session.UpdateFieldValue(Q{"_id": u.ID}, u.CollectionName(), "phoneNumber", "")
			dedupMap[u.ID.Hex()] = "1"
		} else {
			//test if there are infact documents returning more than once.
			println(u.ID.Hex())
			t.Error("Test failed")
		}
	}
	println(count)
}

func xxTestSorting(t *testing.T) {
	session := testDBSession()
	defer session.Close()

	itr := session.DocumentIterator(Q{"state": "CA"}, new(user))
	itr.Load(IteratorConfig{Limit: 20, SortBy: []string{"fullName"}})

	result, err := itr.All(new(user))
	if err != nil {
		println(err)
		return
	}
	users := result.([]*user)
	for _, usr := range users {
		println(usr.FullName)
	}
}

func xxTestBatchAll(t *testing.T) {
	session := testDBSession()
	defer session.Close()

	itr := session.DocumentIterator(Q{"state": "CA"}, new(user))
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
