// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package bytes

import (
	"sync"
	"sync/atomic"

	"github.com/itsmontoya/mrT"
	"github.com/missionMeteora/toolkit/errors"
)

type RTxn struct {
	s store
}

func (r *RTxn) clear() {
	r.s = nil
}

func (r *RTxn) Get(key string) ([]byte, error) {
	return r.s.get(key)
}

func (r *RTxn) Put(key string, value []byte) error {

	return ErrNotWriteTxn
}

func (r *RTxn) Delete(key string) error {

	return ErrNotWriteTxn
}

func (r *RTxn) ForEach(fn ForEachFn) (err error) {
	for key, value := range r.s {
		if fn(key, value) {

			return
		}
	}

	return
}

const (
	ErrNotWriteTxn = errors.Error("cannot perform write actions during a read transaction")

	ErrKeyDoesNotExist = errors.Error("key does not exist")
)

func newTurtle(name, path string, mfn MarshalFn, ufn UnmarshalFn) (tp *turtle, err error) {
	var t turtle
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

type turtle struct {
	mux sync.RWMutex

	mrT *mrT.MrT

	s store

	mfn MarshalFn
	ufn UnmarshalFn

	closed uint32
}

func (t *turtle) isClosed() bool {
	return atomic.LoadUint32(&t.closed) == 1
}

func (t *turtle) load() (err error) {

	var ierr error
	if err = t.mrT.ForEach(func(lineType byte, key, value []byte) (end bool) {
		if lineType == mrT.DeleteLine {

			delete(t.s, string(key))
			return
		}

		var v []byte
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

func (t *turtle) snapshot() (errs *errors.ErrorList) {

	t.mux.RLock()

	defer t.mux.RUnlock()

	errs.Push(t.mrT.Archive(func(txn *mrT.Txn) (err error) {

		for key, value := range t.s {
			var b []byte

			if b, err = t.mfn(value); err != nil {
				errs.Push(err)
				err = nil

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

func (t *turtle) Read(fn TxnFn) (err error) {
	var txn RTxn

	t.mux.RLock()

	defer t.mux.RUnlock()

	if t.isClosed() {

		return errors.ErrIsClosed
	}

	txn.s = t.s

	defer txn.clear()

	return fn(&txn)
}

func (t *turtle) Update(fn TxnFn) (err error) {
	var txn WTxn

	t.mux.Lock()

	defer t.mux.Unlock()

	if t.isClosed() {

		return errors.ErrIsClosed
	}

	txn.s = t.s

	txn.ts = make(txnStore)

	txn.mfn = t.mfn

	defer txn.clear()

	if err = fn(&txn); err != nil {
		return
	}

	if err = t.mrT.Txn(txn.commit); err != nil {
		return
	}

	txn.merge()
	return
}

func (t *turtle) Close() (err error) {
	if !atomic.CompareAndSwapUint32(&t.closed, 0, 1) {

		return errors.ErrIsClosed
	}

	var errs errors.ErrorList

	errs.Push(t.snapshot())

	errs.Push(t.mrT.Close())
	return errs.Err()
}

type store map[string][]byte

func (s store) get(key string) (value []byte, err error) {
	var ok bool
	if value, ok = s[key]; !ok {

		err = ErrKeyDoesNotExist
	}

	return
}

func (s store) exists(key string) (ok bool) {
	_, ok = s[key]
	return
}

type txnStore map[string]*action

func (t txnStore) get(key string) (value []byte, ok bool, err error) {
	var a *action
	if a, ok = t[key]; !ok {

		return
	}

	if !a.put {

		err = ErrKeyDoesNotExist
		return
	}

	value = a.value
	return
}

func (t txnStore) exists(key string) (ok bool) {
	_, ok = t[key]
	return
}

type action struct {
	put bool

	value []byte
}

type Txn interface {
	clear()

	Get(key string) ([]byte, error)

	Put(key string, value []byte) error

	Delete(key string) error

	ForEach(fn ForEachFn) error
}

type ForEachFn func(key string, value []byte) (end bool)

type TxnFn func(txn Txn) error

type MarshalFn func([]byte) ([]byte, error)

type UnmarshalFn func([]byte) ([]byte, error)

type WTxn struct {
	s store

	ts txnStore

	mfn MarshalFn
}

func (w *WTxn) clear() {

	w.s = nil

	w.ts = nil
}

func (w *WTxn) put(txn *mrT.Txn, key string, value []byte) (err error) {
	var b []byte

	if b, err = w.mfn(value); err != nil {

		return
	}

	if err = txn.Put([]byte(key), b); err != nil {
		return
	}

	return
}

func (w *WTxn) delete(txn *mrT.Txn, key string) error {

	return txn.Delete([]byte(key))
}

func (w *WTxn) commit(txn *mrT.Txn) (err error) {
	for key, action := range w.ts {

		if action.put {
			if err = w.put(txn, key, action.value); err != nil {

				return
			}
		} else {
			if err = w.delete(txn, key); err != nil {

				return
			}
		}
	}

	return
}

func (w *WTxn) merge() {

	for key, action := range w.ts {
		if action.put {

			w.s[key] = action.value
		} else {

			delete(w.s, key)
		}
	}
}

func (w *WTxn) Get(key string) (value []byte, err error) {
	var ok bool

	if value, ok, err = w.ts.get(key); ok || err != nil {

		return
	}

	return w.s.get(key)
}

func (w *WTxn) Put(key string, value []byte) (err error) {
	w.ts[key] = &action{
		put:   true,
		value: value,
	}

	return
}

func (w *WTxn) Delete(key string) (err error) {
	if !w.s.exists(key) && !w.ts.exists(key) {

		return
	}

	w.ts[key] = &action{
		put: false,
	}
	return
}

func (w *WTxn) ForEach(fn ForEachFn) (err error) {
	var ok bool
	for key, action := range w.ts {
		if !action.put {

			continue
		}

		if fn(key, action.value) {

			return
		}
	}

	for key, value := range w.s {
		if _, ok = w.ts[key]; ok {

			continue
		}

		if fn(key, value) {

			return
		}
	}

	return
}
