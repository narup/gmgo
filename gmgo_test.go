package gmgo

import (
	"fmt"
	"testing"
)

func TestGMGO(t *testing.T) {
	dbConfig := DbConfig{Host: "localhost:27017", DBName: "phildb", UserName: "", Password: "", Mode: 1}
	err := Setup(dbConfig)
	if err != nil {
		t.Errorf("Connection failed %s", err)
	}

	philDB, err := Get("phildb")
	if err != nil {
		t.Errorf("Get db failed %s", err)
	}

	fmt.Println(philDB.Config.DBName)
}
