package gmgo

import (
	"errors"
	"strings"

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

// Db represents database connection which holds reference to global session and configuration for that database.
type Db struct {
	Config      DbConfig
	mainSession *mgo.Session
}

// DbConfig represents the configuration params needed for MongoDB connection
type DbConfig struct {
	HostURL, DBName, UserName, Password string
	Hosts                               []string
	Mode                                int
}

// DbSession mgo session wrapper
type DbSession struct {
	db      Db
	Session *mgo.Session
}

// Document interface implemented by structs that needs to be persisted. It should provide collection name,
// as in the database. Also, a way to create new object id before saving.
type Document interface {
	CollectionName() string
}

//DocumentIterator is used to iterate over results and also provides a way to configure query using IteractorConfig
//For example:
//
//	session := db.Session()
//	defer session.Close()
//
//	pd := session.DocumentIterator(Q{"state":"CA"}, new(user))
//	pd.Load(IteratorConfig{PageSize: 200, Snapshot: true})
//	for pd.HasMore() {
//		result, err := pd.Next()
//		if err != nil {
//			println(err.Error())
//			return
//		}
//  	u := result.(*user)
//	}
type DocumentIterator struct {
	iterator *mgo.Iter
	query    *mgo.Query
	pageSize int
	document Document
	loaded   bool
}

//IteratorConfig defines different iterator config to load the document interator
type IteratorConfig struct {
	//PageSize is used as a batch size. See mgo.Iter.Batch() for more details.
	//Default value used by MongoDB is 100. So, any value less than 100 is ignored
	PageSize int
	//Limit used limit the number of documents
	Limit int
	//Snashopt ($snapshot) operator prevents the cursor from returning a document more than
	//once because an intervening write operation results in a move of the document.
	Snapshot bool
	//SortBy list of field names to sort the result
	SortBy []string
}

// File file representation
type File struct {
	ID          string
	Name        string
	ContentType string
	ByteLength  int
	Data        []byte
}

func (pd *DocumentIterator) loadInternal() {
	if pd.loaded {
		return
	}

	ic := IteratorConfig{Snapshot: false, PageSize: 100, Limit: -1}
	pd.Load(ic)
}

//Load loads the document iterator using IteratorConfig
//For example:
// Limit and sort by user full name
// 	itr := session.DocumentIterator(Q{"state": "CA"}, new(user))
// 	itr.Load(IteratorConfig{Limit: 20, SortBy: []string{"fullName"}})
//
// fetch with page size
// 	pd.Load(IteratorConfig{PageSize: 200})
func (pd *DocumentIterator) Load(cfg IteratorConfig) {
	if cfg.PageSize >= 100 {
		pd.query = pd.query.Batch(cfg.PageSize)
	}
	if cfg.Limit > 0 {
		pd.query = pd.query.Limit(cfg.Limit)
	}
	if cfg.Snapshot {
		pd.query = pd.query.Snapshot()
	}
	if cfg.SortBy != nil && len(cfg.SortBy) > 0 {
		pd.query = pd.query.Sort(strings.Join(cfg.SortBy, ","))
	}

	pd.iterator = pd.query.Iter()
	pd.loaded = true
}

//HasMore returns true if paged document has still more documents to fetch.
func (pd *DocumentIterator) HasMore() bool {
	pd.loadInternal()
	return !pd.iterator.Done()
}

//Close closes the document iterator
func (pd *DocumentIterator) Close() error {
	pd.loadInternal()
	return pd.iterator.Close()
}

//All returns all the documents in the iterator.
func (pd *DocumentIterator) All(document Document) (interface{}, error) {
	pd.loadInternal()

	documents := slice(document)
	err := pd.iterator.All(documents)
	if err != nil {
		return nil, err
	}

	return results(documents)
}

//Next returns the next result object in the paged document. If there's no element it will check for error
//and return the error if there's error.
func (pd *DocumentIterator) Next() (interface{}, error) {
	pd.loadInternal()

	hasNext := pd.iterator.Next(pd.document)
	if hasNext {
		return pd.document, nil
	}
	return nil, pd.iterator.Err()
}

// Session creates the copy of the gmgo session
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

//gridFS returns grid fs for session
func (s *DbSession) gridFS(prefix string) *mgo.GridFS {
	return s.Session.DB(s.db.Config.DBName).GridFS(prefix)
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
		if err.Error() != mgo.ErrNotFound.Error() {
			log.Printf("Error fetching %s list. Error: %s\n", document.CollectionName(), err)
		}
		return nil, err
	}
	return results(documents)
}

// Collection returns a mgo.Collection representation for given document
func (s *DbSession) Collection(d Document) *mgo.Collection {
	return s.Session.DB(s.db.Config.DBName).C(d.CollectionName())
}

// Save inserts the given document that represents the collection to the database.
func (s *DbSession) Save(document Document) error {
	coll := s.collection(document.CollectionName())
	err := coll.Insert(document)
	if err != nil {
		return err
	}
	return nil
}

// Update updates the given document based on given selector
func (s *DbSession) Update(selector Q, document Document) error {
	coll := s.collection(document.CollectionName())
	return coll.Update(selector, document)
}

//UpdateFieldValue updates the single field with a given value for a collection name based query
func (s *DbSession) UpdateFieldValue(query Q, collectionName, field string, value interface{}) error {
	return s.collection(collectionName).Update(query, bson.M{"$set": bson.M{field: value}})
}

// FindByID find the object by id. Returns error if it's not able to find the document. If document is found
// it's copied to the passed in result object.
func (s *DbSession) FindByID(id string, result Document) error {
	coll := s.collection(result.CollectionName())
	if err := coll.FindId(bson.ObjectIdHex(id)).One(result); err != nil {
		if err.Error() != mgo.ErrNotFound.Error() {
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
		if err.Error() != mgo.ErrNotFound.Error() {
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
		if err.Error() != mgo.ErrNotFound.Error() {
			log.Printf("Error fetching %s. Error: %s\n", document.CollectionName(), err)
		}

		return err
	}
	return nil
}

// FindAllWithFields returns all the documents with given fields based on a given query
func (s *DbSession) FindAllWithFields(query Q, fields []string, document Document) (interface{}, error) {
	fn := func(q *mgo.Query, result interface{}) error {
		return q.Select(sel(fields...)).All(result)
	}
	return s.executeFindAll(query, document, fn)
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

//DocumentIterator returns the document iterator which could be used to fetch documents
//as batch with batch size and other config params
func (s *DbSession) DocumentIterator(query Q, document Document) *DocumentIterator {
	q := s.findQuery(document, query)

	iter := new(DocumentIterator)
	iter.document = document
	iter.query = q

	return iter
}

// Exists check if the document exists for given query
func (s *DbSession) Exists(query Q, document Document) (bool, error) {
	q := s.findQuery(document, query)
	if err := q.Select(bson.M{"_id": 1}).Limit(1).One(document); err != nil {
		if err.Error() == mgo.ErrNotFound.Error() {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

//Remove removes the given document type based on the query
func (s *DbSession) Remove(query Q, document Document) error {
	return s.collection(document.CollectionName()).Remove(query)
}

//RemoveAll removes all the document matching given selector query
func (s *DbSession) RemoveAll(query Q, document Document) error {
	_, err := s.collection(document.CollectionName()).RemoveAll(query)
	return err
}

// Pipe returns the pipe for a given query and document
func (s *DbSession) Pipe(pipeline interface{}, document Document) *mgo.Pipe {
	return s.collection(document.CollectionName()).Pipe(pipeline)
}

//SaveFile saves the given file in a gridfs
func (s *DbSession) SaveFile(file File, prefix string) (string, error) {
	f, err := s.gridFS(prefix).Create(file.Name)
	if err != nil {
		return "", err
	}

	f.SetContentType(file.ContentType)
	_, err = f.Write(file.Data)
	if err != nil {
		return "", err
	}
	defer f.Close()

	fileID := f.Id().(bson.ObjectId)

	return fileID.Hex(), nil
}

//ReadFile read file based on given id
func (s *DbSession) ReadFile(id, prefix string, file *File) error {
	f, err := s.gridFS(prefix).OpenId(bson.ObjectIdHex(id))
	if err != nil {
		return err
	}
	n := f.Size()
	if n == 0 {
		n = 8192
	}
	b := make([]byte, n)
	_, err = f.Read(b)

	err = f.Close()
	if err != nil {
		return err
	}

	file.ID = id
	file.Data = b
	file.Name = f.Name()
	file.ContentType = f.ContentType()

	return nil
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
	if dbConfig.Hosts == nil && dbConfig.HostURL == "" && dbConfig.DBName == "" {
		return errors.New("Invalid connection info. Missing host and db info")
	}

	var session *mgo.Session
	var err error
	if dbConfig.Hosts != nil && dbConfig.DBName != "" {
		mongoDBDialInfo := &mgo.DialInfo{
			Addrs:    dbConfig.Hosts,
			Timeout:  10 * time.Second,
			Database: dbConfig.DBName,
			Username: dbConfig.UserName,
			Password: dbConfig.Password,
		}
		session, err = mgo.DialWithInfo(mongoDBDialInfo)
	} else {
		session, err = mgo.DialWithTimeout(dbConfig.HostURL, 10*time.Second)
	}

	if err != nil {
		log.Printf("MongoDB connection failed : %s. Exiting the program.\n", err)
		return err
	}

	//starting with primary preferred, but individual query can change mode per copied session
	session.SetMode(mgo.Strong, true)
	log.Println("Connected to MongoDB successfully")

	/* Initialized database object with global session*/
	connectionMap[dbConfig.DBName] = Db{mainSession: session, Config: dbConfig}

	return nil
}

func sel(q ...string) (r bson.M) {
	r = make(bson.M, len(q))
	for _, s := range q {
		r[s] = 1
	}
	return
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
