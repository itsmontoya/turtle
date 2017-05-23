package turtle

import "github.com/itsmontoya/mrT"

// WTxn is a write transaction
type WTxn struct {
	// Original store
	s store
	// Transaction store
	ts txnStore
	// Marshal func
	mfn MarshalFn
}

func (w *WTxn) clear() {
	w.s = nil
	w.ts = nil
}

func (w *WTxn) commit(txn *mrT.Txn) (err error) {
	for key, action := range w.ts {
		if !action.put {
			if err = txn.Delete([]byte(key)); err != nil {
				return
			}
		} else {
			var b []byte
			if b, err = w.mfn(action.value); err != nil {
				return
			}

			if err = txn.Put([]byte(key), b); err != nil {
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

// Get will get a value for a provided key
func (w *WTxn) Get(key string) (value Value, err error) {
	var ok bool
	if value, ok, err = w.ts.get(key); err != nil || ok {
		return
	}

	return w.s.get(key)
}

// Put will put a value for a provided key
func (w *WTxn) Put(key string, value Value) (err error) {
	w.ts[key] = &action{
		put:   true,
		value: value,
	}

	return
}

// Delete will delete a key
func (w *WTxn) Delete(key string) (err error) {
	if !w.s.exists(key) && !w.ts.exists(key) {
		return
	}

	w.ts[key] = &action{
		put: false,
	}
	return
}

// ForEach will iterate through all current items
func (w *WTxn) ForEach(fn ForEachFn) (err error) {
	var ok bool
	for key, action := range w.ts {
		if !action.put {
			// Action was not a PUT action, which means it was a delete action
			continue
		}

		if fn(key, action.value) {
			// End was called, return early
			return
		}
	}

	for key, value := range w.s {
		if _, ok = w.ts[key]; ok {
			// This key already exists within our transaction map, we can continue on
			continue
		}

		if fn(key, value) {
			// End was called, return early
			return
		}
	}

	return
}
