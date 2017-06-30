package turtle

import (
	"sync"
)

type buckets map[string]*bucket

func newBucket() *bucket {
	var b bucket
	b.m = make(map[string]Value)
	return &b
}

// bucket is thread-safe a basic data store
type bucket struct {
	mux sync.RWMutex

	m map[string]Value
}

// get will retrieve a value for a provided key
func (b *bucket) get(key string) (value Value, err error) {
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
func (b *bucket) put(key string, value Value) {
	b.mux.Lock()
	b.m[key] = value
	b.mux.Unlock()
}

// exists will return a boolean representing if a value exists for a provided key
func (b *bucket) exists(key string) (ok bool) {
	b.mux.RLock()
	_, ok = b[key]
	b.mux.RUnlock()
	return
}

func newTxnBucket(mfn MarshalFn) *txnBucket {
	var tb txnBucket
	tb.m = make(map[string]*action)
	tb.mfn = mfn
	return &tb
}

// txnBucket is a specialized data store handling transaction actions
type txnBucket struct {
	mux sync.RWMutex
	mfn MarshalFn

	m map[string]*action
}

// get will retrieve a value for a provided key
func (t *txnBucket) get(key string) (value Value, ok bool, err error) {
	var a *action
	if a, ok = t.m[key]; !ok {
		// No actions were taken for this key during the transaction
		return
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
func (t *txnBucket) put(key string, value Value) {
	t.mux.Lock()
	t.m[key] = value
	t.mux.Unlock()
}

// exists will return a boolean representing if an action was taken for a provided key
func (t *txnBucket) exists(key string) (ok bool) {
	_, ok = t.m[key]
	return
}
