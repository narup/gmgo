package gmgo

import (
	"errors"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"log"
	"reflect"
	"time"
)

// Q query representation to hide bson.M type to single file
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
	Mode                             int
}

// Db represents database connection which holds reference to global session and configuration for that database.
type Db struct {
	Config      DbConfig
	mainSession *mgo.Session
}

// DbSession mgo session wrapper
type DbSession struct {
	db      Db
	Session *mgo.Session
}

// Session creates the copy of the main session
func (db Db) Session() *DbSession {
	return &DbSession{db: db, Session: db.mainSession.Copy()}
}

// Clone returns the clone of current DB session. Cloned session
// uses the same socket connection
func (s *DbSession) Clone() *DbSession {
	return &DbSession{db: s.db, Session: s.Session.Clone()}
}

// Close closes the underlying mgo session
func (s *DbSession) Close() {
	s.Session.Close()
}

// collection returns a mgo.Collection representation for given collection name and session
func (s *DbSession) collection(collectionName string) *mgo.Collection {
	return s.Session.DB(s.db.Config.DBName).C(collectionName)
}

// findQuery constrcuts the find query based on given query params
func (s *DbSession) findQuery(d Document, q Q) *mgo.Query {
	//collection pointer for the given document
	return s.collection(d.CollectionName()).Find(q)
}

// executeFindAll executes find all query
func (s *DbSession) executeFindAll(query Q, document Document, qf queryFunc) (interface{}, error) {
	documents := slice(document)
	q := s.findQuery(document, query)

	if err := qf(q, documents); err != nil {
		if err.Error() != "not found" {
			log.Printf("Error fetching %s list. Error: %s\n", document.CollectionName(), err)
		}
		return nil, err
	}
	return results(documents)
}

// Save inserts the given document that represents the collection to the database.
func (s *DbSession) Save(document Document) error {
	coll := s.collection(document.CollectionName())
	if err := coll.Insert(document); err != nil {
		return err
	}
	return nil
}

// Update updates the given document based on given selector
func (s *DbSession) Update(selector Q, document Document) error {
	coll := s.collection(document.CollectionName())
	return coll.Update(selector, document)
}

// FindByID find the object by id. Returns error if it's not able to find the document. If document is found
// it's copied to the passed in result object.
func (s *DbSession) FindByID(id string, result Document) error {
	coll := s.collection(result.CollectionName())
	if err := coll.FindId(bson.ObjectIdHex(id)).One(result); err != nil {
		if err.Error() != "not found" {
			log.Printf("Error fetching %s with id %s. Error: %s\n", result.CollectionName(), id, err)
		}
		return err
	}
	return nil
}

// Find the data based on given query
func (s *DbSession) Find(query Q, document Document) error {
	q := s.findQuery(document, query)
	if err := q.One(document); err != nil {
		if err.Error() != "not found" {
			log.Printf("Error fetching %s with query %s. Error: %s\n", document.CollectionName(), query, err)
		}
		return err
	}
	return nil
}

// FindByRef finds the document based on given db reference.
func (s *DbSession) FindByRef(ref *mgo.DBRef, document Document) error {
	q := s.Session.DB(s.db.Config.DBName).FindRef(ref)
	if err := q.One(document); err != nil {
		if err.Error() != "not found" {
			log.Printf("Error fetching %s. Error: %s\n", document.CollectionName(), err)
		}

		return err
	}
	return nil
}

// FindAll returns all the documents based on given query
func (s *DbSession) FindAll(query Q, document Document) (interface{}, error) {
	fn := func(q *mgo.Query, result interface{}) error {
		return q.All(result)
	}
	return s.executeFindAll(query, document, fn)
}

// FindWithLimit find the doucments for given query with limit
func (s *DbSession) FindWithLimit(limit int, query Q, document Document) (interface{}, error) {
	fn := func(q *mgo.Query, result interface{}) error {
		return q.Limit(limit).All(result)
	}
	return s.executeFindAll(query, document, fn)
}

// Get creates new database connection
func Get(dbName string) (Db, error) {
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

	session, err := mgo.DialWithInfo(mongoDBDialInfo)
	if err != nil {
		log.Printf("MongoDB connection failed : %s. Exiting the program.\n", err)
		return err
	}

	log.Println("Connected to MongoDB successfully")
	/* Initialized database object with global session*/
	connectionMap[dbConfig.DBName] = Db{mainSession: session, Config: dbConfig}

	return nil
}

func results(documents interface{}) (interface{}, error) {
	return reflect.ValueOf(documents).Elem().Interface(), nil
}

// slice returns the interface representation of actual collection type for returning list data
func slice(d Document) interface{} {
	documentType := reflect.TypeOf(d)
	documentSlice := reflect.MakeSlice(reflect.SliceOf(documentType), 0, 0)

	// Create a pointer to a slice value and set it to the slice
	return reflect.New(documentSlice.Type()).Interface()
}
