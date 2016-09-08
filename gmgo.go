package gmgo

import (
	"errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"reflect"
	"time"
)

//query representation to hide bson.M type to single file
type Q map[string]interface{}

type queryFunc func(q *mgo.Query, result interface{}) error

// connectionMap holds all the db connection per database name
var connectionMap = make(map[string]Db)

// Document interface implemented by structs that needs to be persisted. It should provide collection name,
// as in the database. Also, a way to create new object id before saving.
type Document interface {
	CollectionName() string
}

// DbConfig represents the configuration params needed for MongoDB connection
type DbConfig struct {
	Host, DBName, UserName, Password string
}

// Db represents database connection which holds reference to global session and configuration for that database.
type Db struct {
	Config  DbConfig
	Session *mgo.Session
}

// collection returns a mgo.Collection representation for given collection name and session
func (db Db) collection(collectionName string, session *mgo.Session) *mgo.Collection {
	return session.DB(db.Config.DBName).C(collectionName)
}

// slice returns the interface representation of actual collection type for returning list data
func (db Db) slice(d Document) interface{} {
	documentType := reflect.TypeOf(d)
	documentSlice := reflect.MakeSlice(reflect.SliceOf(documentType), 0, 0)

	// Create a pointer to a slice value and set it to the slice
	return reflect.New(documentSlice.Type()).Interface()
}

func (db Db) findQuery(d Document, s *mgo.Session, q interface{}) *mgo.Query {
	//collection pointer for the given document
	return db.collection(d.CollectionName(), s).Find(q)
}

func (db Db) executeFindAll(query Q, document Document, qf queryFunc) (interface{}, error) {
	session := db.Session.Copy()
	defer session.Close()

	//collection pointer for the given document
	documents := db.slice(document)
	q := db.findQuery(document, session, query)

	if err := qf(q, documents); err != nil {
		log.Printf("Error fetching %s list. Error: %s\n", document.CollectionName(), err)
		return nil, err
	}
	return results(documents)
}

// Save inserts the given document that represents the collection to the database.
func (db Db) Save(document Document) error {
	session := db.Session.Copy()
	defer session.Close()

	coll := db.collection(document.CollectionName(), session)
	if err := coll.Insert(document); err != nil {
		return err
	}

	log.Println("Document inserted successfully!")
	return nil
}

// Find the object by id. Returns error if it's not able to find the document. If document is found
// it's copied to the passed in result object.
func (db Db) FindById(id string, result Document) error {
	session := db.Session.Copy()
	defer session.Close()

	coll := db.collection(result.CollectionName(), session)
	if err := coll.FindId(bson.ObjectIdHex(id)).One(result); err != nil {
		log.Printf("Error fetching %s with id %s. Error: %s\n", result.CollectionName(), id, err)
		return err
	} else {
		log.Printf("Found data for id %s\n", id)
	}
	return nil
}

func (db Db) Find(query map[string]interface{}, document Document) error {
	session := db.Session.Copy()
	defer session.Close()

	q := db.findQuery(document, session, query)
	if err := q.One(document); err != nil {
		log.Printf("Error fetching %s with query %s. Error: %s\n", document.CollectionName(), query, err)
		return err
	} else {
		log.Printf("Found data for query %s\n", query)
	}

	return nil
}

func (db Db) FindByRef(ref *mgo.DBRef, document Document) error {
	session := db.Session.Copy()
	defer session.Close()

	q := session.DB(db.Config.DBName).FindRef(ref)
	if err := q.One(document); err != nil {
		log.Printf("Error fetching %s. Error: %s\n", document.CollectionName(), err)
		return err
	}
	return nil
}

func (db Db) FindAll(query Q, document Document) (interface{}, error) {
	fn := func(q *mgo.Query, result interface{}) error {
		return q.All(result)
	}
	return db.executeFindAll(query, document, fn)
}

func (db Db) FindWithLimit(limit int, query Q, document Document) (interface{}, error) {
	fn := func(q *mgo.Query, result interface{}) error {
		return q.Limit(limit).All(result)
	}
	return db.executeFindAll(query, document, fn)
}

func New(dbName string) (Db, error) {
	if db, ok := connectionMap[dbName]; ok {
		return db, nil
	}
	return Db{}, errors.New("Database connection not available. Perform 'Setup' first")
}

// Setup the MongoDB connection based on passed in config. It can be called multiple times to setup connection to
// multiple MongoDB instances.
func Setup(dbConfig DbConfig) error {
	log.Println("Connecting to MongoDB...")

	mongoDBDialInfo := &mgo.DialInfo{
		Addrs:    []string{dbConfig.Host},
		Timeout:  5 * time.Second,
		Database: dbConfig.DBName,
		Username: dbConfig.UserName,
		Password: dbConfig.Password,
	}

	dbSession, err := mgo.DialWithInfo(mongoDBDialInfo)
	if err != nil {
		log.Printf("MongoDB connection failed : %s. Exiting the program.\n", err)
		return err
	}

	log.Println("Connected to MongoDB successfully")

	/* Initialized database object with global session*/
	connectionMap[dbConfig.DBName] = Db{Session: dbSession, Config: dbConfig}

	return nil
}

func results(documents interface{}) (interface{}, error) {
	return reflect.ValueOf(documents).Elem().Interface(), nil
}
