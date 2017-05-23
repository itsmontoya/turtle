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

func (t *Turtle) isClosed() bool {
	return atomic.LoadUint32(&t.closed) == 1
}

func (t *Turtle) load() (err error) {
	var ierr error
	if err = t.mrT.ForEach(func(lineType byte, key, value []byte) (end bool) {
		if lineType == mrT.DeleteLine {
			delete(t.s, string(key))
			return
		}

		var v Value
		if v, ierr = t.ufn(value); err != nil {
			return true
		}

		t.s[string(key)] = v
		return
	}); err != nil {
		return
	}

	return ierr
}

func (t *Turtle) snapshot() (errs *errors.ErrorList) {
	t.mux.RLock()
	defer t.mux.RUnlock()

	errs.Push(t.mrT.Archive(func(txn *mrT.Txn) (err error) {
		for key, value := range t.s {
			var b []byte
			if b, err = t.mfn(value); err != nil {
				errs.Push(err)
				err = nil
				// We don't necessarily need to stop the world for marshal errors, add to errors list and move on
				continue
			}

			if err = txn.Put([]byte(key), b); err != nil {
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
	// Defer read-lock release
	defer t.mux.RUnlock()
	if t.isClosed() {
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
	// Defer write-lock release
	defer t.mux.Unlock()
	if t.isClosed() {
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

// Close will close turtle
func (t *Turtle) Close() (err error) {
	if !atomic.CompareAndSwapUint32(&t.closed, 0, 1) {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	errs.Push(t.snapshot())
	errs.Push(t.mrT.Close())
	return errs.Err()
}
