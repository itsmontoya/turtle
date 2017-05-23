package turtle

// RTxn is a read transaction
type RTxn struct {
	// Original store
	s store
}

func (r *RTxn) clear() {
	r.s = nil
}

// Get will get a value for a provided key
func (r *RTxn) Get(key string) (Value, error) {
	return r.s.get(key)
}

// Put will put a value for a provided key
func (r *RTxn) Put(key string, value Value) error {
	// Cannot perform PUT actions during a read transaction
	return ErrNotWriteTxn
}

// Delete will delete a key
func (r *RTxn) Delete(key string) error {
	// Cannot perform PUT actions during a read transaction
	return ErrNotWriteTxn
}

// ForEach will iterate through all current items
func (r *RTxn) ForEach(fn ForEachFn) (err error) {
	for key, value := range r.s {
		if fn(key, value) {
			// End was called, return early
			return
		}
	}

	return
}
