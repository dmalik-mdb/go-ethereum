package mongodb

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Database struct {
	fn string // filename for reporting
	db *mongo.Database

	log log.Logger // Contextual logger tracking the database path
}

type KV struct {
	Key   string `bson:"key"`
	Value string `bson:"value"`
}

func New(file string) (*Database, error) {
	logger := log.New("database", file)

	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().
		ApplyURI("mongodb+srv://[USERNAME]:[PASSWORD]@[CLUSTER]/?retryWrites=true&w=majority").
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

func (db *Database) Close() error {
	db.db.Client().Disconnect(context.Background())
	db.db = nil
	return nil
}

func (db *Database) Has(key []byte) (bool, error) {
	var result KV
	hasKey := false

	opts := options.FindOneOptions{}
	opts.SetProjection(bson.D{{"_id", -1}, {"key", 1}, {"value", -1}})
	err := db.db.Collection("ethdb").FindOne(context.TODO(), bson.D{{"key", hex.EncodeToString(key)}}, &opts).Decode(&result)
	if err != nil {
		return hasKey, err
	} else {
		hasKey = true
		return hasKey, err
	}
}

// Get returns the given key if it's present.
func (db *Database) Get(key []byte) ([]byte, error) {
	var result KV

	opts := options.FindOneOptions{}
	opts.SetProjection(bson.D{{"_id", -1}, {"key", 1}, {"value", 1}})
	err := db.db.Collection("ethdb").FindOne(context.TODO(), bson.D{{"key", hex.EncodeToString(key)}}, &opts).Decode(&result)
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(result.Value)
}

// Put puts the given key / value to the queue
func (db *Database) Put(key []byte, value []byte) error {
	hasKey, err := db.Has(key)

	if err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	if hasKey {
		filter := bson.D{{"key", hex.EncodeToString(key)}}
		update := bson.D{{"$set", bson.D{{"key", hex.EncodeToString(key)}, {"value", hex.EncodeToString(value)}}}}
		_, err := db.db.Collection("ethdb").UpdateOne(context.TODO(), filter, update)
		return err
	} else {
		doc := bson.D{{"key", hex.EncodeToString(key)}, {"value", hex.EncodeToString(value)}}
		_, err := db.db.Collection("ethdb").InsertOne(context.TODO(), doc)
		return err
	}
}

// Delete deletes the key from the queue and database
func (db *Database) Delete(key []byte) error {

	filter := bson.D{{"key", hex.EncodeToString(key)}}
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

func (db *Database) NewBatchWithSize(size int) ethdb.Batch {
	return &batch{db: db.db}
}

type batch struct {
	db     *mongo.Database
	models []mongo.WriteModel
	size   int
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	b.models = append(b.models, mongo.NewInsertOneModel().SetDocument(bson.D{{"key", hex.EncodeToString(key)}, {"value", hex.EncodeToString(value)}}))
	b.size += 1
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	b.models = append(b.models, mongo.NewDeleteOneModel().SetFilter(bson.D{{"key", hex.EncodeToString(key)}}))
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
	opts := options.BulkWrite().SetOrdered(true)
	_, err := b.db.Collection("ethdb").BulkWrite(context.TODO(), b.models, opts)
	if err != nil {
		return err
	} else {
		return nil
	}
}

type iterator struct {
	db     *mongo.Database
	cursor *mongo.Cursor
	key    []byte
	value  []byte
	err    error
}

func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	var pr = hex.EncodeToString(prefix)
	var st = hex.EncodeToString(append(prefix, start...))

	opts := options.FindOptions{}
	opts.SetProjection(bson.D{{"_id", -1}, {"key", 1}, {"value", 1}})
	opts.SetSort(bson.D{{"key", 1}})
	filter := bson.D{{"$text", bson.D{{"$search", pr}}}, {"key", bson.D{{"$lte", st}}}}

	cursor, err := db.db.Collection("ethdb").Find(context.TODO(), filter, &opts)

	return &iterator{
		db:     db.db,
		cursor: cursor,
		key:    make([]byte, 0),
		value:  make([]byte, 0),
		err:    err,
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
	return i.cursor.Next(context.TODO())
}

func (i *iterator) Release() {
	defer i.cursor.Close(context.TODO())
	i.key, i.value = nil, nil
}

func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}

func (db *Database) NewSnapshot() (ethdb.Snapshot, error) {
	return nil, nil
}

func (db *Database) Stat(property string) (string, error) {
	return "", errors.New("unknown property")
}
