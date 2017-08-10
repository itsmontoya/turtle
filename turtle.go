package turtleDB

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/cheekybits/genny/generic"
	"github.com/itsmontoya/middleware"
	"github.com/itsmontoya/mrT"
	"github.com/missionMeteora/toolkit/errors"
)

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
type Value generic.Type

// New will return a new instance of Turtle
func New(name, path string, fm FuncsMap) (tp *Turtle, err error) {
	var t Turtle
	if t.mrT, err = mrT.New(path, name, middleware.Base64MW{}); err != nil {
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

	var v Value
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

		return bkt.ForEach(func(refKey string, val Value) (err error) {
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
