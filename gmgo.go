package gmgo

import (
	"errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"reflect"
	"time"
)

/* DB Connection map */
var connectionMap = make(map[string]Db)

/**
 * Interface implemented by structs that needs to be persisted. It should provide collection name, as in the database.
 * Also, a way to create new object id before saving.
 */
type Document interface {
	CollectionName() string
}

/** Database config for the connection */
type DbConfig struct {
	Host, DBName, UserName, Password string
}

/**
 * Mongo database connection which holds reference to global session and config it was used.
 * Both of these are immutable once initialized and not exposed to clients.
 */
type Db struct {
	Config  DbConfig
	Session *mgo.Session
}

// Public Db functions
// private method to construct collection object based on the name and session
func (db Db) collection(collectionName string, session *mgo.Session) *mgo.Collection {
	return db.Session.DB(db.Config.DBName).C(collectionName)
}

// Insert the given db object representing the document to the database
func (db Db) Save(object Document) error {
	session := db.Session.Copy()
	defer session.Close()

	//objectId := object.CreateNewObjectId()
	coll := db.collection(object.CollectionName(), session)
	if err := coll.Insert(object); err != nil {
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

func (db Db) Find(query map[string]interface{}, result Document) error {
	session := db.Session.Copy()
	defer session.Close()

	coll := db.collection(result.CollectionName(), session)
	if err := coll.Find(query).One(result); err != nil {
		log.Printf("Error fetching %s with query %s. Error: %s\n", result.CollectionName(), query, err)
		return err
	} else {
		log.Printf("Found data for query %s\n", query)
	}

	return nil
}

func (db Db) FindAll(query map[string]interface{}, document Document) (interface{}, error) {
	session := db.Session.Copy()
	defer session.Close()

	//collection pointer for the given document
	coll := db.collection(document.CollectionName(), session)

	documentType := reflect.TypeOf(document)
	documentSlice := reflect.MakeSlice(reflect.SliceOf(documentType), 0, 0)

	// Create a pointer to a slice value and set it to the slice
	documents := reflect.New(documentSlice.Type())

	if err := coll.Find(query).All(documents.Interface()); err != nil {
		log.Printf("Error fetching %s with id %s. Error: %s\n", document.CollectionName(), err)
		return nil, err
	}

	return documents.Elem().Interface(), nil
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
