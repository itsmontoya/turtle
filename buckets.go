package turtleDB

import "sync"

func newBuckets() *buckets {
	var b buckets
	b.m = make(map[string]*bucket)
	return &b
}

type buckets struct {
	mux sync.RWMutex
	m   map[string]*bucket
}

// create will create a bucket at a given key. This is intended for internal use only
func (b *buckets) create(key string) (bkt *bucket) {
	var ok bool
	b.mux.Lock()
	if bkt, ok = b.m[key]; !ok {
		bkt = newBucket()
		b.m[key] = bkt
	}
	b.mux.Unlock()
	return
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
func (b *buckets) ForEach(fn ForEachBucketFn) {
	b.mux.RLock()
	defer b.mux.RUnlock()

	for key, bucket := range b.m {
		if fn(key, bucket) {
			return
		}
	}
}
