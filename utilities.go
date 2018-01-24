package turtleDB

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/itsmontoya/mrT"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrInvalidKey is returned when an invalid key is encountered
	// A key must have a bucket AND reference to be valid
	ErrInvalidKey = errors.Error("key does not have a bucket and reference")
	// ErrInvalidMarshalFunc is returned when a nil marshal function is provided
	ErrInvalidMarshalFunc = errors.Error("invalid marshal function")
	// ErrInvalidUnmarshalFunc is returned when a nil unmarshal function is provided
	ErrInvalidUnmarshalFunc = errors.Error("invalid unmarshal function")
	// ErrNoFuncsMatch is returned when a Funcs lookup for a bucket key yields no results
	ErrNoFuncsMatch = errors.Error("no functions match for this key")
)

type action struct {
	// put state, false assumes a delete action
	put bool
	// value of action, only looked at during put state
	value Value
}

// DB is a basic database type
type DB interface {
	Name() string
	Read(TxnFn) error
	Update(TxnFn) error
	Import(r io.Reader) (txnID string, err error)
	Export(txnID string, w io.Writer) error
	ForEachTxn(txnID string, archive bool, fn mrT.ForEachFn) (err error)
	SetVerbosity(Verbosity)
	SetAoC(aoc bool)
	Close() error
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
	ForEach(fn ForEachBucketFn) error
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
	ForEach(fn ForEachFn) error
}

// ForEachBucketFn is used for iterate through each bucket
type ForEachBucketFn func(key string, bkt Bucket) (err error)

// ForEachFn is used for iterate through each value
type ForEachFn func(key string, value Value) (err error)

// TxnFn is used for transactions
type TxnFn func(txn Txn) error

// MarshalFn is for marshaling
type MarshalFn func(Value) ([]byte, error)

// UnmarshalFn is for unmarshaling
type UnmarshalFn func([]byte) (Value, error)

func getKeys(key []byte) (bktKey, refKey string, err error) {
	spl := bytes.SplitN(key, []byte{':'}, 2)
	if len(spl) < 2 {
		err = ErrInvalidKey
		return
	}

	bktKey = string(spl[0])
	refKey = string(spl[1])
	return
}

func mergeKeys(bktKey, refKey string) (key []byte) {
	return []byte(bktKey + ":" + refKey)
}

// NewFuncsMap will create a FuncsMap and set it's default entry as the provided funcs
func NewFuncsMap(mfn MarshalFn, ufn UnmarshalFn) (fm FuncsMap) {
	fm = make(FuncsMap)
	fm.Put("default", mfn, ufn)
	return fm
}

// FuncsMap is a map of functions for marshaling and unmarshaling
type FuncsMap map[string]*Funcs

// Get will get a matching Funcs for a given key
func (fm FuncsMap) Get(key string) (fns *Funcs, err error) {
	var ok bool
	if fns, ok = fm[key]; ok {
		return
	}

	if fns, ok = fm["default"]; ok {
		return
	}

	err = ErrNoFuncsMatch
	return
}

// Put will set a marshal and unmarshal func for a given key
func (fm FuncsMap) Put(key string, mfn MarshalFn, ufn UnmarshalFn) (err error) {
	if key == "" {
		return ErrEmptyKey
	}

	if mfn == nil {
		return ErrInvalidMarshalFunc
	}

	if ufn == nil {
		return ErrInvalidUnmarshalFunc
	}

	fm[key] = &Funcs{mfn, ufn}
	return
}

// Funcs is a set of functions
type Funcs struct {
	Marshal   MarshalFn
	Unmarshal UnmarshalFn
}

// MarshalJSON is a basic JSON marshaler helper func
func MarshalJSON(val Value) (b []byte, err error) {
	return json.Marshal(val)
}

// UnmarshalJSON is a basic JSON unmarshaler helper func
func UnmarshalJSON(b []byte) (val Value, err error) {
	err = json.Unmarshal(b, &val)
	return
}

var jsonFM = NewFuncsMap(MarshalJSON, UnmarshalJSON)
