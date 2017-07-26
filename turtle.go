package turtle

import (
	"sync"
	"sync/atomic"

	"github.com/cheekybits/genny/generic"
	"github.com/itsmontoya/mrT"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrNotWriteTxn is returned when PUT or DELETE are called during a read txn
	ErrNotWriteTxn = errors.Error("cannot perform write actions during a read transaction")
	// ErrKeyDoesNotExist is returned when a key does not exist
	ErrKeyDoesNotExist = errors.Error("key does not exist")
)

// Value is the value type
type Value generic.Type

// New will return a new instance of Turtle
func New(name, path string, mfn MarshalFn, ufn UnmarshalFn) (tp *Turtle, err error) {
	var t Turtle
	if t.mrT, err = mrT.New(path, name); err != nil {
		return
	}

	t.s = make(store)
	t.mfn = mfn
	t.ufn = ufn

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
	// Internal store
	s store

	mfn MarshalFn
	ufn UnmarshalFn

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
		if lineType == mrT.DeleteLine {
			// We encountered a delete line, remove the key from the map and return early
			delete(t.s, string(key))
			return
		}

		var v Value
		if v, ierr = t.ufn(value); err != nil {
			// Error encountered while unmarshaling, return and end the loop early
			return true
		}

		// Set the key as our parsed value within the database store
		t.s[string(key)] = v
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

func (t *Turtle) snapshot() (errs *errors.ErrorList) {
	// Acquire read-lock
	t.mux.RLock()
	// Defer release of read-lock
	defer t.mux.RUnlock()
	// Initialize errorlist before using
	errs = &errors.ErrorList{}

	errs.Push(t.mrT.Archive(func(txn *mrT.Txn) (err error) {
		// Iterate through all items
		for key, value := range t.s {
			var b []byte
			// Marshal the value as bytes
			if b, err = t.mfn(value); err != nil {
				errs.Push(err)
				err = nil
				// We don't necessarily need to stop the world for marshal errors,
				// add to errors list and move on
				continue
			}

			// Put the updated bytes to the back-end
			if err = txn.Put([]byte(key), b); err != nil {
				// Errors on put are something we need to immediately yield for.
				// The only possible errors we would encounter are:
				// 	1. Disk issues
				// 	2. Middleware issues
				// Both of which would occur for every subsequent item
				return
			}
		}

		return
	}))

	return
}

func (t *Turtle) Read(fn TxnFn) (err error) {
	var txn RTxn
	// Acquire read-lock
	t.mux.RLock()
	// Defer release of read-lock
	defer t.mux.RUnlock()

	if t.isClosed() {
		// DB is closed and we cannot perform any actions, return with error
		return errors.ErrIsClosed
	}

	// Assign store to txn's store field
	txn.s = t.s
	// Defer txn clear
	defer txn.clear()

	// Call provided func
	return fn(&txn)
}

// Update will create an update transaction
func (t *Turtle) Update(fn TxnFn) (err error) {
	var txn WTxn
	// Acquire write-lock
	t.mux.Lock()
	// Defer release of write-lock
	defer t.mux.Unlock()

	if t.isClosed() {
		// DB is closed and we cannot perform any actions, return with error
		return errors.ErrIsClosed
	}

	// Assign store to txn's store field
	txn.s = t.s
	// Create new txnStore
	txn.ts = make(txnStore)
	// Set marshal func
	txn.mfn = t.mfn
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
