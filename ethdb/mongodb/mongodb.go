package ethdb

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Database struct {
	fn string // filename for reporting
	db *mongo.Database

	getTimer        metrics.Timer // Timer for measuring the database get request counts and latencies
	putTimer        metrics.Timer // Timer for measuring the database put request counts and latencies
	delTimer        metrics.Timer // Timer for measuring the database delete request counts and latencies
	missMeter       metrics.Meter // Meter for measuring the missed database get requests
	readMeter       metrics.Meter // Meter for measuring the database get request data usage
	writeMeter      metrics.Meter // Meter for measuring the database put request data usage
	batchPutTimer   metrics.Timer
	batchWriteTimer metrics.Timer
	batchWriteMeter metrics.Meter

	quitLock sync.Mutex // Mutex protecting the quit channel access

	log log.Logger // Contextual logger tracking the database path
}

type KV struct {
	key   []byte
	value []byte
}

// NewBadgerDatabase returns a BadgerDB wrapped object.
func New(file string) (*Database, error) {
	logger := log.New("database", file)

	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().
		ApplyURI("mongodb+srv://main_user:Passw0rd@cluster0.us76j.mongodb.net/?retryWrites=true&w=majority").
		SetServerAPIOptions(serverAPIOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)

	// (Re)check for errors and abort if opening of the db failed
	if err != nil {
		return nil, err
	}
	mdb := &Database{
		fn:  file,
		db:  client.Database("ethdb"),
		log: logger,
	}

	return mdb, nil
}

// Close deallocates the internal map and ensures any consecutive data access op
// failes with an error.
func (db *Database) Close() error {
	db.db.Client().Disconnect(context.Background())

	db.db = nil
	return nil
}

func (db *Database) Has(key []byte) (bool, error) {
	var result KV
	err := db.db.Collection("ethdb").FindOne(context.TODO(), bson.D{{"key", key}}).Decode(&result)
	hasKey := false
	if err == nil {
		hasKey = true
	}
	return hasKey, err
}

// Get returns the given key if it's present.
func (db *Database) Get(key []byte) ([]byte, error) {
	// Measure the database get latency, if requested
	if db.getTimer != nil {
		defer db.getTimer.UpdateSince(time.Now())
	}

	var result KV

	err := db.db.Collection("ethdb").FindOne(context.TODO(), bson.D{{"key", key}}).Decode(&result)

	if err != nil {
		return nil, err
	}

	if db.readMeter != nil {
		db.readMeter.Mark(int64(len(result.value)))
	}
	if err != nil {
		if db.missMeter != nil {
			db.missMeter.Mark(1)
		}
		return nil, err
	}

	return result.value, nil
}

// Put puts the given key / value to the queue
func (db *Database) Put(key []byte, value []byte) error {

	if db.putTimer != nil {
		defer db.putTimer.UpdateSince(time.Now())
	}

	if db.writeMeter != nil {
		db.writeMeter.Mark(int64(len(value)))
	}

	hasKey, err := db.Has(key)

	if err != nil {
		return err
	}
	if hasKey {
		filter := bson.D{{"key", key}}
		update := bson.D{{"$set", bson.D{{"key", key}, {"value", value}}}}
		_, err := db.db.Collection("ethdb").UpdateOne(context.TODO(), filter, update)
		return err
	} else {
		doc := bson.D{{"key", key}, {"value", value}}
		_, err := db.db.Collection("ethdb").InsertOne(context.TODO(), doc)
		return err
	}
}

// Delete deletes the key from the queue and database
func (db *Database) Delete(key []byte) error {
	// Measure the database delete latency, if requested
	if db.delTimer != nil {
		defer db.delTimer.UpdateSince(time.Now())
	}

	filter := bson.D{{"key", key}}
	result, err := db.db.Collection("ethdb").DeleteOne(context.TODO(), filter)

	if err == nil {
		if result.DeletedCount == 0 {
			err = nil
		}
		return err
	} else {
		return err
	}
}

func (db *Database) NewBatch() ethdb.Batch {
	return &batch{db: db.db}
}

type batch struct {
	db     *mongo.Database
	models []mongo.WriteModel
	size   int
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	b.models = append(b.models, mongo.NewInsertOneModel().SetDocument(bson.D{{"key", key}, {"value", value}}))
	b.size += 1
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	b.models = append(b.models, mongo.NewDeleteOneModel().SetFilter(bson.D{{"key", key}}))
	b.size += 1
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	opts := options.BulkWrite().SetOrdered(true)
	_, err := b.db.Collection("ethdb").BulkWrite(context.TODO(), b.models, opts)

	if err != nil {
		return err
	} else {
		return nil
	}
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.models = b.models[:0]
	b.size = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	opts := options.BulkWrite().SetOrdered(false)
	_, err := b.db.Collection("ethdb").BulkWrite(context.TODO(), b.models, opts)

	if err != nil {
		return err
	} else {
		return nil
	}
}

type iterator struct {
	db *mongo.Database
	// cursor *mongo.Cursor
	skip  int
	key   []byte
	value []byte
	err   error
}

func (db *Database) NewIterator() ethdb.Iterator {
	return &iterator{
		db:    db.db,
		key:   make([]byte, 0),
		value: make([]byte, 0),
		skip:  0,
	}
}

func (i *iterator) Error() error {
	return i.err
}

func (i *iterator) Key() []byte {
	return i.key
}

func (i *iterator) Value() []byte {
	return i.value
}

func (i *iterator) Next() bool {
	var key []byte
	var value []byte
	var result KV

	opts := options.FindOne().SetSort(bson.D{{"key", 1}}).SetSkip(int64(i.skip + 1))
	err := i.db.Collection("ethdb").FindOne(context.TODO(), bson.D{{"key", key}}, opts).Decode(&result)

	if err != nil {
		i.err = err
		return false
	}

	i.key = key
	i.value = value
	i.skip += 1
	return true
}

func (i *iterator) Prev() bool {
	var key []byte
	var value []byte
	var result KV

	opts := options.FindOne().SetSort(bson.D{{"key", 1}}).SetSkip(int64(i.skip - 1))
	err := i.db.Collection("ethdb").FindOne(context.TODO(), bson.D{{"key", key}}, opts).Decode(&result)

	if err != nil {
		i.err = err
		return false
	}

	i.key = key
	i.value = value
	i.skip -= 1
	return true
}

func (i *iterator) Release() {
	i.key = nil
	i.value = nil
	i.skip = 0
}
