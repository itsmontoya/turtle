package turtleDB

import "sync"

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
	tbkt, ok = tb.m[key]
	tb.mux.RUnlock()

	if ok {
		if tbkt.deleted {
			// This bucket was deleted during the txn
			err = ErrKeyDoesNotExist
			return
		}

		b = tbkt
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
