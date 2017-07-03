package turtleDB

import (
	"bytes"
)

type action struct {
	// put state, false assumes a delete action
	put bool
	// value of action, only looked at during put state
	value Value
}

// Txn is a basic transaction interface
type Txn interface {
	clear()

	// Get bucket by key
	Get(key string) (Bucket, error)
	// Create bucket by key
	Create(key string) (Bucket, error)
	// Delete bucket by key
	Delete(key string) error
	// ForEach bucket
	ForEach(fn ForEachBucketFn)
}

// Bucket represents a db bucket
type Bucket interface {
	// Get value by key
	Get(key string) (Value, error)
	// Put value by key
	Put(key string, value Value) error
	// Delete key
	Delete(key string) error
	// Exists returns whether or not a value exists for a given key
	Exists(key string) bool
	// ForEach key/value pair
	ForEach(fn ForEachFn)
}

// ForEachBucketFn is used for iterate through each bucket
type ForEachBucketFn func(key string, bkt Bucket) (end bool)

// ForEachFn is used for iterate through each value
type ForEachFn func(key string, value Value) (end bool)

// TxnFn is used for transactions
type TxnFn func(txn Txn) error

// MarshalFn is for marshaling
type MarshalFn func(Value) ([]byte, error)

// UnmarshalFn is for unmarshaling
type UnmarshalFn func([]byte) (Value, error)

func getKeys(key []byte) (bktKey, refKey string, err error) {
	spl := bytes.SplitN(key, []byte{':'}, 2)
	if len(spl) < 2 {
		// TODO: Error handling here
		return
	}

	bktKey = string(spl[0])
	refKey = string(spl[1])
	return
}

func mergeKeys(bktKey, refKey string) (key []byte) {
	return []byte(bktKey + ":" + refKey)
}
