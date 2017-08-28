package gmgo

import (
	"fmt"
	"testing"
)

func testDBSession() *DbSession {
	dbConfig := DbConfig{HostURL: "mongodb://localhost:27017/phildb", DBName: "phildb", UserName: "", Password: "", Mode: 1}
	err := Setup(dbConfig)
	if err != nil {
		fmt.Printf("Connection failed %s", err)
		return nil
	}

	philDB, err := Get("phildb")
	if err != nil {
		fmt.Printf("Get db failed %s", err)
		return nil
	}

	fmt.Println(philDB.Config.DBName)

	return philDB.Session()
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
