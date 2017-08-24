// This file was automatically generated by genx.
// Any changes will be lost if this file is regenerated.
// see https://github.com/OneOfOne/genx
// cmd: genx -pkg github.com/Path94/turtleDB -t Value=[]byte -o ./bytes/turtle.go
// +build !genx

package turtleDB

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Path94/atoms"
	"github.com/itsmontoya/middleware"
	"github.com/itsmontoya/mrT"
	"github.com/missionMeteora/journaler"
	"github.com/missionMeteora/toolkit/errors"
)

func newBucket() *bucket {
	var b bucket
	b.m = make(map[string][]byte)
	return &b
}

// bucket is thread-safe a basic data store
type bucket struct {
	mux sync.RWMutex

	m map[string][]byte
}

// get will retrieve a value for a provided key
func (b *bucket) get(key string) (value []byte, err error) {
	var ok bool
	b.mux.RLock()
	if value, ok = b.m[key]; !ok {
		// Value does not exist for this key
		err = ErrKeyDoesNotExist
	}
	b.mux.RUnlock()
	return
}

// put will set a value for a provided key
func (b *bucket) put(key string, value []byte) {
	b.mux.Lock()
	b.m[key] = value
	b.mux.Unlock()
}

// delete will remove a provided key
func (b *bucket) delete(key string) (ok bool) {
	b.mux.Lock()
	delete(b.m, key)
	b.mux.Unlock()
	return
}

// exists will return a boolean representing if a value exists for a provided key
func (b *bucket) exists(key string) (ok bool) {
	b.mux.RLock()
	_, ok = b.m[key]
	b.mux.RUnlock()
	return
}

func (b *bucket) Get(key string) (value []byte, err error) {
	return b.get(key)
}

func (b *bucket) Put(key string, value []byte) error {
	// bucket's are read-only to the user. This is an exposed Put function to
	// satisfy the Bucket interface. Since client's cannot put directly to a bucket,
	// we can automatically return ErrNotWriteTxn without doing any lookups
	// Note: Internal functions use bucket.put for modifications to the bucket
	return ErrNotWriteTxn
}

func (b *bucket) Delete(key string) error {
	return ErrNotWriteTxn
}

func (b *bucket) Exists(key string) (ok bool) {
	return b.exists(key)
}

func (b *bucket) ForEach(fn ForEachFn) (err error) {
	b.mux.RLock()
	defer b.mux.RUnlock()

	for key, val := range b.m {
		if err = fn(key, val); err != nil {
			return
		}
	}

	return
}

var zero_byte []byte

func newBuckets() *buckets {
	var b buckets
	b.m = make(map[string]*bucket)
	return &b
}

type buckets struct {
	mux sync.RWMutex
	m   map[string]*bucket
}

// Get will get a bucket
func (b *buckets) get(key string) (bkt *bucket, err error) {
	var ok bool
	b.mux.RLock()
	bkt, ok = b.m[key]
	b.mux.RUnlock()

	if !ok {
		// No match was found, return error
		err = ErrKeyDoesNotExist
	}

	return
}

// create will create a bucket at a given key. This is intended for internal use only
func (b *buckets) create(key string) (bkt *bucket) {
	var ok bool
	b.mux.Lock()
	if bkt, ok = b.m[key]; !ok {
		// Bucket does not exist, create new bucket
		bkt = newBucket()
		// Assign new bucket to the bucket map
		b.m[key] = bkt
	}
	b.mux.Unlock()
	return
}

// delete will delete a bucket at a given key. This is intended for internal use only
func (b *buckets) delete(key string) (bkt *bucket) {
	b.mux.Lock()
	delete(b.m, key)
	b.mux.Unlock()
	return
}

// Get will get a bucket
func (b *buckets) Get(key string) (bkt Bucket, err error) {
	return b.get(key)
}

// Create will create and return a bucket
// Note: This will always error due to being a read-only interface
func (b *buckets) Create(key string) (Bucket, error) {
	return nil, ErrNotWriteTxn
}

// Delete will delete a bucket
// Note: This will always error due to being a read-only interface
func (b *buckets) Delete(key string) (err error) {
	return ErrNotWriteTxn
}

// ForEach will iterate through all the child buckets
func (b *buckets) ForEach(fn ForEachBucketFn) (err error) {
	b.mux.RLock()
	defer b.mux.RUnlock()

	for key, bucket := range b.m {
		if err = fn(key, bucket); err != nil {
			return
		}
	}

	return
}

// rTxn is a read transaction
type rTxn struct {
	// Original buckets
	*buckets
}

func (r *rTxn) clear() {
	r.buckets = nil
}

const (
	// ErrEmptyImporter is reutrned when the importer is nil when calling NewSlave
	ErrEmptyImporter = errors.Error("importer cannot be nil")
)

// NewSlave will return a new slave
// Note: importInterval is interval time in seconds
func NewSlave(name, path string, fm FuncsMap, imp Importer, importInterval int) (sp *Slave, err error) {
	var s Slave
	if imp == nil {
		// Import function cannot be nil, return
		err = ErrEmptyImporter
		return
	}

	if s.db, err = New(name, path, fm); err != nil {
		// Error initializing db, return
		return
	}

	// Set journaler for Stdout logging
	s.out = journaler.New("TurtleDB", name)
	// Set import func
	s.fn = imp.Import

	// Start update loop in a new goroutine
	go s.loop(importInterval)

	sp = &s
	return
}

// Slave is a read-only db
type Slave struct {
	db  *Turtle
	out *journaler.Journaler
	// Function used for importing on loop calls
	fn ImportFn
	// Last transaction id
	lastTxn atoms.String
	// Closed state
	closed atoms.Bool
}

func (s *Slave) loop(importInterval int) {
	var (
		r   io.Reader
		err error
	)

	for !s.closed.Get() {
		if r, err = s.fn(s.lastTxn.Load()); err != nil {
			// We encountered an error while importing, log the error and continue on
			s.out.Error("Error importing: %v", err)
		} else {
			// We successfully received the reader, import reader
			s.importReader(r)
		}

		time.Sleep(time.Second * time.Duration(importInterval))
	}
}

func (s *Slave) importReader(r io.Reader) {
	var (
		ltxn string
		err  error
	)

	if ltxn, err = s.db.Import(r); err != nil {
		// We encountered an error while importing, log the error and continue on
		s.out.Error("Error importing: %v", err)
		return
	}

	if ltxn == "" {
		// No import occurred, do not update txn
		return
	}

	// Update the last txn value
	s.lastTxn.Store(ltxn)
	return
}

// Read opens a read transaction
func (s *Slave) Read(fn TxnFn) (err error) {
	return s.db.Read(fn)
}

// Update opens an update transaction
func (s *Slave) Update(fn TxnFn) (err error) {
	return ErrSlaveUpdate
}

// TxnID will return the current transaction id
func (s *Slave) TxnID() (txnID string) {
	return s.lastTxn.Load()
}

// Close will close the slave
func (s *Slave) Close() (err error) {
	if !s.closed.Set(true) {
		return errors.ErrIsClosed
	}

	return s.db.Close()
}

// ImportFn is called when an import is requested
type ImportFn func(txnID string) (io.Reader, error)

// Importer is the interface needed to call import for Slave DBs
type Importer interface {
	Import(txnID string) (r io.Reader, err error)
}

const (
	// ErrNotWriteTxn is returned when PUT or DELETE are called during a read txn
	ErrNotWriteTxn = errors.Error("cannot perform write actions during a read transaction")
	// ErrKeyDoesNotExist is returned when a key does not exist
	ErrKeyDoesNotExist = errors.Error("key does not exist")
	// ErrEmptyKey is returned when an empty key is provided
	ErrEmptyKey = errors.Error("empty keys are invalid")
	// ErrSlaveUpdate is returned when an update transaction is called from a slave database
	ErrSlaveUpdate = errors.Error("cannot call an update transaction from a slave db")
	// ErrInvalidType is a helper error for importing services to utilize. This is not used internally
	ErrInvalidType = errors.Error("invalid type")
)

// Value is the value type

// New will return a new instance of Turtle
func New(name, path string, fm FuncsMap, mws ...middleware.Middleware) (tp *Turtle, err error) {
	var t Turtle
	mws = append([]middleware.Middleware{middleware.Base64MW{}}, mws...)
	if t.mrT, err = mrT.New(path, name, mws...); err != nil {
		return
	}

	if fm == nil {
		t.fm = jsonFM
	} else {
		t.fm = fm
	}

	t.b = newBuckets()

	if err = t.load(); err != nil {
		return
	}

	tp = &t
	return
}

// Turtle is a DB, he's not a slow fella - I promise!
type Turtle struct {
	// Read/Write mutex
	mux sync.RWMutex
	// Back-end persistence
	mrT *mrT.MrT

	b  *buckets
	fm FuncsMap

	// Closed state
	closed uint32
}

// isClosed will atomically check the closed state of the database
func (t *Turtle) isClosed() bool {
	return atomic.LoadUint32(&t.closed) == 1
}

// load is called on DB initialization and will populate the in-memory store from our file back-end
func (t *Turtle) load() (err error) {
	// If an error is encountered during ForEach, generally a disk or middleware related issue
	// Any error which may be encountered SHOULD occur before any iteration occurs
	// TODO: Do some heavy combing through the codebase to confirm this statement
	return t.mrT.ForEach("", false, t.loadLine)
}

func (t *Turtle) loadLine(lineType byte, key, val []byte) (err error) {
	switch lineType {
	case mrT.PutLine:
		return t.loadPutLine(key, val)
	case mrT.DeleteLine:
		return t.loadDelLine(key)
	case mrT.TransactionLine, mrT.NilLine, mrT.CommentLine:
	}

	return
}

func (t *Turtle) loadPutLine(key, val []byte) (err error) {
	var bktKey, refKey string
	if bktKey, refKey, err = getKeys(key); err != nil {
		return
	}

	var fns *Funcs
	if fns, err = t.fm.Get(bktKey); err != nil {
		return
	}

	var v []byte
	if v, err = fns.Unmarshal(val); err != nil {
		// Error encountered while unmarshaling, return and end the loop early
		return
	}

	bkt := t.b.create(bktKey)
	// Set the key as our parsed value within the database store
	bkt.put(refKey, v)
	return
}

func (t *Turtle) loadDelLine(key []byte) (err error) {
	var bktKey, refKey string
	if bktKey, refKey, err = getKeys(key); err != nil {
		return
	}

	if refKey == "" {
		// Empty reference key represents the bucket
		t.b.delete(bktKey)
		return
	}

	var bkt *bucket
	if bkt, err = t.b.get(bktKey); err != nil {
		return
	}

	// Remove the value from the bucket
	bkt.delete(refKey)
	return
}

func (t *Turtle) snapshot() (errs *errors.ErrorList) {
	// Initialize errorlist
	errs = &errors.ErrorList{}

	// Acquire read-lock
	t.mux.RLock()
	// Defer release of read-lock
	defer t.mux.RUnlock()

	errs.Push(t.mrT.Archive(func(txn *mrT.Txn) error {
		return t.forEachMemory(func(bktKey, refKey string, val []byte) (err error) {
			// Put the updated bytes to the back-end
			// The only possible errors we would encounter are:
			// 	1. Disk issues
			// 	2. Middleware issues
			return txn.Put(mergeKeys(bktKey, refKey), val)
		})
	}))

	return
}

// forEachMemory will go through all items in memory
// Note: This is NOT thread-safe, please handle locking within calling func
func (t *Turtle) forEachMemory(fn func(bkt, key string, val []byte) error) (err error) {
	// Iterate through all items
	return t.b.ForEach(func(bktKey string, bkt Bucket) (err error) {
		var fns *Funcs
		if fns, err = t.fm.Get(bktKey); err != nil {
			return
		}

		return bkt.ForEach(func(refKey string, val []byte) (err error) {
			// Marshal the value as bytes
			var b []byte
			if b, err = fns.Marshal(val); err != nil {
				return
			}

			if err = fn(bktKey, refKey, b); err != nil {
				return
			}

			return
		})
	})
}

// Export will stream an export
func (t *Turtle) Export(txnID string, w io.Writer) (err error) {
	// Acquire read-lock
	t.mux.RLock()
	// Defer release of read-lock
	defer t.mux.RUnlock()

	return t.mrT.Export(txnID, w)
}

// Import will process an export
func (t *Turtle) Import(r io.Reader) (lastTxn string, err error) {
	t.mux.Lock()
	defer t.mux.Unlock()

	return t.mrT.Import(r, t.loadLine)
}

// Read opens a read transaction
func (t *Turtle) Read(fn TxnFn) (err error) {
	var txn rTxn
	// Acquire read-lock
	t.mux.RLock()
	// Defer release of read-lock
	defer t.mux.RUnlock()

	if t.isClosed() {
		// DB is closed and we cannot perform any actions, return with error
		return errors.ErrIsClosed
	}

	// Assign buckets to txn's buckets field
	txn.buckets = t.b

	// Defer txn clear
	defer txn.clear()

	// Call provided func
	return fn(&txn)
}

// Update opens an update transaction
func (t *Turtle) Update(fn TxnFn) (err error) {
	var txn wTxn
	// Acquire write-lock
	t.mux.Lock()
	// Defer release of write-lock
	defer t.mux.Unlock()

	if t.isClosed() {
		// DB is closed and we cannot perform any actions, return with error
		return errors.ErrIsClosed
	}

	// Assign bucket to transactions bucket field
	txn.b = t.b
	// Create new txnStore
	txn.tb = newTxnBuckets(t.b)
	// Set marshal func
	txn.fm = t.fm
	// Defer txn clear
	defer txn.clear()

	// Call provided func
	if err = fn(&txn); err != nil {
		return
	}
	// Commit changes
	if err = t.mrT.Txn(txn.commit); err != nil {
		return
	}
	// Merge changes
	txn.merge()
	return
}

// Close will close Turtle
func (t *Turtle) Close() (err error) {
	if !atomic.CompareAndSwapUint32(&t.closed, 0, 1) {
		// DB is already closed, return with error
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	// Attempt to snapshot
	errs.Push(t.snapshot())
	// Close file back-end
	errs.Push(t.mrT.Close())
	return errs.Err()
}

func newTxnBucket(b *bucket) *txnBucket {
	var tb txnBucket
	tb.b = b
	tb.m = make(map[string]*action)
	return &tb
}

// txnBucket is a specialized data store handling transaction actions
type txnBucket struct {
	mux sync.RWMutex

	b *bucket
	m map[string]*action

	deleted bool
}

// get will retrieve a value for a provided key
func (t *txnBucket) get(key string) (value []byte, err error) {
	var (
		a  *action
		ok bool
	)

	if a, ok = t.m[key]; !ok {
		// No actions were taken for this key during the transaction
		if t.b == nil {
			err = ErrKeyDoesNotExist
			return
		}

		return t.b.get(key)
	}

	if !a.put {
		// Key was deleted during this transaction, return early with error
		err = ErrKeyDoesNotExist
		return
	}

	// Key was updated during the transaction, set value
	value = a.value
	return
}

// put will set a value for a provided key
func (t *txnBucket) put(key string, value []byte) {
	t.m[key] = &action{
		put:   true,
		value: value,
	}
}

func (t *txnBucket) delete(key string) (ok bool) {
	if t.b != nil && t.b.exists(key) {
		// Empty action is a delete action
		t.m[key] = &action{}
		return true
	}

	var a *action
	if a, ok = t.m[key]; ok {
		if a.put {
			// We only need to create a delete action if the item has survived a transaction commit
			delete(t.m, key)
			return true
		}
	}

	return false
}

func (t *txnBucket) deleteAll() {
	t.mux.Lock()
	defer t.mux.Unlock()

	t.forEach(func(key string, _ []byte) (err error) {
		t.delete(key)
		return
	})
}

// exists will return a boolean representing if an action was taken for a provided key
func (t *txnBucket) exists(key string) (ok bool) {
	var a *action
	if a, ok = t.m[key]; ok {
		// If a.put is false, that means the key was deleted
		return a.put == true
	}

	if t.b != nil {
		return t.b.exists(key)
	}

	return false
}

func (t *txnBucket) forEach(fn ForEachFn) (err error) {
	for key, a := range t.m {
		if !a.put {
			continue
		}

		if err = fn(key, a.value); err != nil {
			return
		}
	}

	if t.b == nil {
		return
	}

	return t.b.ForEach(func(key string, val []byte) (err error) {
		if _, ok := t.m[key]; ok {
			return
		}

		return fn(key, val)
	})
}

func (t *txnBucket) Get(key string) (value []byte, err error) {
	t.mux.RLock()
	value, err = t.get(key)
	t.mux.RUnlock()
	return
}

func (t *txnBucket) Put(key string, value []byte) (err error) {
	t.mux.Lock()
	t.put(key, value)
	t.mux.Unlock()
	return
}

func (t *txnBucket) Delete(key string) (err error) {
	t.mux.Lock()
	ok := t.delete(key)
	t.mux.Unlock()

	if !ok {
		err = ErrKeyDoesNotExist
	}
	return
}

func (t *txnBucket) Exists(key string) (ok bool) {
	t.mux.RLock()
	ok = t.exists(key)
	t.mux.RUnlock()
	return
}

func (t *txnBucket) ForEach(fn ForEachFn) (err error) {
	t.mux.RLock()
	err = t.forEach(fn)
	t.mux.RUnlock()
	return
}

func newTxnBuckets(b *buckets) *txnBuckets {
	var tb txnBuckets
	tb.b = b
	tb.m = make(map[string]*txnBucket)
	return &tb
}

type txnBuckets struct {
	mux sync.RWMutex

	// Main branch
	b *buckets
	// Transaction buckets
	m map[string]*txnBucket
}

// create will create a bucket at a given key
func (tb *txnBuckets) create(key string, rb *bucket) (bkt *txnBucket, created bool) {
	var ok bool
	if bkt, ok = tb.m[key]; !ok {
		// Get reference bucket
		if rb == nil {
			rb, _ = tb.b.get(key)
		}

		bkt = newTxnBucket(rb)
		tb.m[key] = bkt
		created = true
	}

	bkt.deleted = false
	return
}

// Get will get a bucket
func (tb *txnBuckets) Get(key string) (b Bucket, err error) {
	var (
		tbkt *txnBucket
		ok   bool
	)

	tb.mux.RLock()
	if tbkt, ok = tb.m[key]; ok {
		if tbkt.deleted {
			// This bucket was deleted during the txn
			err = ErrKeyDoesNotExist
		} else {
			b = tbkt
		}
	}
	tb.mux.RUnlock()

	if ok {
		return
	}

	tb.mux.Lock()
	// Check again to ensure the txnBucket wasn't created inbetween locks
	if b, ok = tb.m[key]; !ok {
		if bb, err := tb.b.get(key); err == nil {
			tbkt := newTxnBucket(bb)
			tb.m[key] = tbkt
			b = tbkt
			ok = true
		}
	}
	tb.mux.Unlock()

	if !ok {
		// No match was found, return error
		err = ErrKeyDoesNotExist
	}

	return
}

// Create will create and return a bucket
func (tb *txnBuckets) Create(key string) (bkt Bucket, err error) {
	tb.mux.Lock()
	defer tb.mux.Unlock()
	bkt, _ = tb.create(key, nil)
	return
}

// Delete will delete a bucket
func (tb *txnBuckets) Delete(key string) (err error) {
	var (
		bkt *txnBucket
		ok  bool
	)

	// Lock before doing anything
	tb.mux.Lock()
	defer tb.mux.Unlock()

	if bkt, ok = tb.m[key]; !ok {
		// No bucket exists at this key, no need to delete it
		return ErrKeyDoesNotExist
	}

	// Delete the contents of the bucket
	bkt.deleteAll()
	bkt.deleted = true
	return
}

// ForEach will iterate through all the child txnBuckets
func (tb *txnBuckets) ForEach(fn ForEachBucketFn) (err error) {
	// We are write locking because we may need to modify the buckets for the txn
	// It may be possible to adjust this to a read lock once everything is working properly
	tb.mux.Lock()
	defer tb.mux.Unlock()

	for key, bucket := range tb.m {
		if err = fn(key, bucket); err != nil {
			return
		}
	}

	return tb.b.ForEach(func(key string, bkt Bucket) (err error) {
		bb := bkt.(*bucket)
		tbkt, created := tb.create(key, bb)
		if !created {
			return
		}

		return fn(key, tbkt)
	})
}

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
	value []byte
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
	Get(key string) ([]byte, error)
	// Put value by key
	Put(key string, value []byte) error
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
type ForEachFn func(key string, value []byte) (err error)

// TxnFn is used for transactions
type TxnFn func(txn Txn) error

// MarshalFn is for marshaling
type MarshalFn func([]byte) ([]byte, error)

// UnmarshalFn is for unmarshaling
type UnmarshalFn func([]byte) ([]byte, error)

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
func MarshalJSON(val []byte) (b []byte, err error) {
	return json.Marshal(val)
}

// UnmarshalJSON is a basic JSON unmarshaler helper func
func UnmarshalJSON(b []byte) (val []byte, err error) {
	err = json.Unmarshal(b, &val)
	return
}

var jsonFM = NewFuncsMap(MarshalJSON, UnmarshalJSON)

// wTxn represents a write transactgion
type wTxn struct {
	b  *buckets
	tb *txnBuckets
	fm FuncsMap
}

func (w *wTxn) clear() {
	// Set store reference to nil
	w.b = nil
	// Set transaction store reference to nil
	w.tb = nil
	// Set the funcs map to nil
	w.fm = nil
}

// put is a QoL func to log a put action
func (w *wTxn) put(txn *mrT.Txn, bktKey, refKey string, value []byte) (err error) {
	var fns *Funcs
	if fns, err = w.fm.Get(bktKey); err != nil {
		return
	}

	var b []byte
	// Attempt to marshal value as bytes
	if b, err = fns.Marshal(value); err != nil {
		// Marshal error encountered, return
		return
	}

	// Get merged key
	key := mergeKeys(bktKey, refKey)

	// Log action to disk
	if err = txn.Put(key, b); err != nil {
		return
	}

	return
}

// delete is a QoL func to log a delete action
func (w *wTxn) delete(txn *mrT.Txn, bktKey, refKey string) error {
	// Get merged key
	key := mergeKeys(bktKey, refKey)

	// Log action to disk
	return txn.Delete(key)
}

// commit will log all actions to disk
func (w *wTxn) commit(txn *mrT.Txn) (err error) {
	for bktKey, bkt := range w.tb.m {
		for refKey, a := range bkt.m {
			// If action.put is true, put action
			// Else, delete action
			if a.put {
				if err = w.put(txn, string(bktKey), string(refKey), a.value); err != nil {
					// Error encountered while logging put, return
					return
				}
			} else {
				if err = w.delete(txn, string(bktKey), string(refKey)); err != nil {
					// Error encountered while logging delete, return
					return
				}
			}
		}

		if bkt.deleted {
			if err = w.delete(txn, string(bktKey), ""); err != nil {
				// Error encountered while logging delete, return
				return
			}
		}
	}

	return
}

// merge will merge the transaction store values with the store values
func (w *wTxn) merge() {
	for bktKey, bkt := range w.tb.m {
		if bkt.deleted {
			// If the bucket was deleted, we don't have to do anything fancy. Just delete and move on
			w.b.delete(bktKey)
			continue
		}

		bb := w.b.create(bktKey)

		for refKey, a := range bkt.m {
			if a.put {
				bb.put(refKey, a.value)
				continue
			}

			bb.delete(refKey)
		}
	}
}

// Get will get a bucket
func (w *wTxn) Get(key string) (b Bucket, err error) {
	return w.tb.Get(key)
}

// Create will create a bucket
func (w *wTxn) Create(key string) (b Bucket, err error) {
	return w.tb.Create(key)
}

func (w *wTxn) Delete(key string) (err error) {
	return w.tb.Delete(key)
}

// ForEach will iterate through all buckets
func (w *wTxn) ForEach(fn ForEachBucketFn) error {
	return w.tb.ForEach(fn)
}
