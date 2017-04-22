# gomongo
Convenient wrapper around Go's MongoDB driver Mgo

## Usage:

```go
package main

import (
	"fmt"
	"github.com/phil-inc/gmgo"
	"log"
)

var userDB gmgo.Db

####################
type User struct {
    Name string `json:"name" bson:"name"`
    Email string `json:"email" bson:"email"`
}

// Each of your data model that needs to be persisted should implment DBObject interface
func (user User) CollectionName() string {
    return "user"
}

####################

func saveNewUser() {
   user := &User{Name:'Puran', Email:'puran@xyz.com'}
   user.Id = bson.NewObjectId()
   userId, err := userDB.Save(user)
   if err != nil {
	log.Fatalf("Error saving user : %s.\n", err)
   }

   fmt.Printf("User id %s", userId)
}

func findUser(userId string) *User {
    user := new(User)
    if err := userDB.FindById(userId, user); err != nil {
        return nil
    }
    return user
}

func setupUserDB() {
    if userDbErr := gmgo.Setup(gmgo.DbConfig{"localhost:27017", "userdb", "", ""}); userDbErr != nil {
    		log.Fatalf("Database connection error : %s.\n", userDbErr)
    		return
    }

    newDb, userDbErr := gmgo.New("userdb")
    if userDbErr != nil {
        log.Fatalf("Db connection error : %s.\n", err)
    }
    userDB = newDb
}

func main() {
    //setup Mongo database connection. You can setup multiple db connections
    setupUserDB()
    user := findUser("56596608e4b07ceddcfad96e")
    if user != nil {
    	fmt.Printf("User name:%s\n", user.Name)
    } else {
	fmt.Printf("Couldn't find user")
    }
	
    //Find all users
    users, err := db.FindAll(gmgo.Q{}, new(User)) //Note user pointer is passed to identify the collection type etc.
    if err != nil {
    	fmt.Printf("Error fetching users %s", err)
    } else {
	for _, user := range users {
	   fmt.Println(user.Name)
        }
    }
}

```
