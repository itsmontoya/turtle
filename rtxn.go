package turtle

// RTxn is a read transaction
type RTxn struct {
	// Original buckets
	b *buckets
}

func (r *RTxn) clear() {
	r.b = nil
}

// Get will get a value for a provided key
func (r *RTxn) Get(key string) (Bucket, error) {
	var ok bool
	if Value, ok = r.b[key]; !ok {
		return ErrKeyDoesNotExist
	}

	return
}

// Create will create a bucket for a provided key
func (r *RTxn) Create(key string, bucket Bucket) error {
	// Cannot perform PUT actions during a read transaction
	return ErrNotWriteTxn
}

// Delete will delete a key
func (r *RTxn) Delete(key string) error {
	// Cannot perform PUT actions during a read transaction
	return ErrNotWriteTxn
}

// ForEach will iterate through all current items
func (r *RTxn) ForEach(fn ForEachBucketFn) (err error) {
	for key, bucket := range r.b {
		if fn(key, bucket) {
			// End was called, return early
			return
		}
	}

	return
}
