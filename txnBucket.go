package turtleDB

import "sync"

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
}

// get will retrieve a value for a provided key
func (t *txnBucket) get(key string) (value Value, err error) {
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
func (t *txnBucket) put(key string, value Value) {
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

	t.forEach(func(key string, _ Value) bool {
		t.delete(key)
		return false
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

func (t *txnBucket) forEach(fn ForEachFn) {
	for key, a := range t.m {
		if !a.put {
			continue
		}

		if fn(key, a.value) {
			return
		}
	}

	if t.b == nil {
		return
	}

	t.b.ForEach(func(key string, val Value) (end bool) {
		if _, ok := t.m[key]; ok {
			return
		}

		fn(key, val)
		return
	})
}

func (t *txnBucket) Get(key string) (value Value, err error) {
	t.mux.RLock()
	value, err = t.get(key)
	t.mux.RUnlock()
	return
}

func (t *txnBucket) Put(key string, value Value) (err error) {
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

func (t *txnBucket) ForEach(fn ForEachFn) {
	t.mux.RLock()
	t.forEach(fn)
	t.mux.RUnlock()
}
