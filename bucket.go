package turtleDB

import "sync"

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

// delete will remove a provided key
func (b *bucket) delete(key string) (ok bool) {
	b.mux.Lock()
	if _, ok = b.m[key]; ok {
		delete(b.m, key)
	}
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

func (b *bucket) Get(key string) (value Value, err error) {
	return b.get(key)
}

func (b *bucket) Put(key string, value Value) error {
	return ErrNotWriteTxn
}

func (b *bucket) Delete(key string) error {
	return ErrNotWriteTxn
}

func (b *bucket) Exists(key string) (ok bool) {
	return b.exists(key)
}

func (b *bucket) ForEach(fn ForEachFn) {
	b.mux.RLock()
	defer b.mux.RUnlock()

	for key, val := range b.m {
		if fn(key, val) {
			return
		}
	}
}
