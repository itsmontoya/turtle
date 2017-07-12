package turtleDB

import (
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
	// Inner error, this is intended so that the error returned by ForEach
	// does not overwrite a true error we encounter during iteration.
	// To explain further - if ForEach returns a nil error, yet we encountered
	// an unmarshal error during the loop. The error would be returned as nil.
	var ierr error
	if err = t.mrT.ForEach(func(lineType byte, key, value []byte) (end bool) {
		switch lineType {
		case mrT.PutLine:
			bktKey, refKey, err := getKeys(key)
			if err != nil {
				return
			}

			var fns *Funcs
			if fns, ierr = t.fm.Get(bktKey); ierr != nil {
				return
			}

			var v Value
			if v, ierr = fns.Unmarshal(value); ierr != nil {
				// Error encountered while unmarshaling, return and end the loop early
				return true
			}

			bkt := t.b.create(bktKey)
			// Set the key as our parsed value within the database store
			bkt.put(refKey, v)

		case mrT.DeleteLine:
			bktKey, refKey, err := getKeys(key)
			if err != nil {
				return
			}

			if refKey == "" {
				// Empty reference key represents the bucket
				t.b.delete(bktKey)
				return
			}

			bkt, err := t.b.Get(bktKey)
			if err != nil {
				return
			}

			// Remove the value from the bucket
			bkt.Delete(refKey)
		case mrT.TransactionLine, mrT.NilLine, mrT.CommentLine:
		}

		return
	}); err != nil {
		// Error encountered during ForEach, generally a disk or middleware related issue
		// Any error which may be encountered SHOULD occur before any iteration occurs
		// TODO: Do some heavy combing through the codebase to confirm this statement
		return
	}

	// Return any inner errors encountered
	return ierr
}

func (t *Turtle) snapshot() (errs errors.ErrorList) {
	// Acquire read-lock
	t.mux.RLock()
	// Defer release of read-lock
	defer t.mux.RUnlock()

	errs.Push(t.mrT.Archive(func(txn *mrT.Txn) (err error) {
		// Iterate through all items
		t.b.ForEach(func(bktKey string, bkt Bucket) bool {
			var fns *Funcs
			if fns, err = t.fm.Get(bktKey); err != nil {
				return true
			}

			bkt.ForEach(func(refKey string, val Value) (end bool) {
				// Marshal the value as bytes
				b, err := fns.Marshal(val)
				if err != nil {
					errs.Push(err)
					err = nil
					// We don't necessarily need to stop the world for marshal errors,
					// add to errors list and move on
					return
				}

				// Put the updated bytes to the back-end
				if err = txn.Put(mergeKeys(bktKey, refKey), b); err != nil {
					// Errors on put are something we need to immediately yield for.
					// The only possible errors we would encounter are:
					// 	1. Disk issues
					// 	2. Middleware issues
					// Both of which would occur for every subsequent item
					return
				}
				return
			})

			return false
		})

		return
	}))

	return errs
}

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

// Update will create an update transaction
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
	tErrs := t.snapshot()
	errs.Push(&tErrs)

	// Close file back-end
	errs.Push(t.mrT.Close())
	return errs.Err()
}
