package gmgo

import (
	"fmt"
	"testing"
)

func TestGMGO(t *testing.T) {
	dbConfig := DbConfig{HostURL: "mongodb://localhost:27017/phildb", DBName: "phildb", UserName: "", Password: "", Mode: 1}
	err := Setup(dbConfig)
	if err != nil {
		t.Errorf("Connection failed %s", err)
	}

	philDB, err := Get("phildb")
	if err != nil {
		t.Errorf("Get db failed %s", err)
	}

	fmt.Println(philDB.Config.DBName)

	file := new(File)
	file.ByteLength = 1024
	err = philDB.Session().ReadFile("5713f1b0e4b067fc28d6fbaa", file)
	if err != nil {
		t.Errorf("File read failed %s", err)
		return
	}
	fmt.Printf("File name:%s, Content Type: %s\n", file.Name, file.ContentType)
}
