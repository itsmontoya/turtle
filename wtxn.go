package turtle

import "github.com/itsmontoya/mrT"

// WTxn is a write transaction
type WTxn struct {
	// Original bucket
	b *bucket
	// Transaction bucket
	tb *txnBucket
	// Marshal func
	mfn MarshalFn
}

func (w *WTxn) clear() {
	// Set store reference to nil
	w.b = nil
	// Set transaction store reference to nil
	w.tb = nil
}

// put is a QoL func to log a put action
func (w *WTxn) put(txn *mrT.Txn, key string, value Value) (err error) {
	var b []byte
	// Attempt to marshal value as bytes
	if b, err = w.mfn(value); err != nil {
		// Marshal error encountered, return
		return
	}

	// Log action to disk
	if err = txn.Put([]byte(key), b); err != nil {
		return
	}

	return
}

// delete is a QoL func to log a delete action
func (w *WTxn) delete(txn *mrT.Txn, key string) error {
	// Log action to disk
	return txn.Delete([]byte(key))
}

// commit will log all actions to disk
func (w *WTxn) commit(txn *mrT.Txn) (err error) {
	for key, action := range w.ts {
		// If action.put is true, put action
		// Else, delete action
		if action.put {
			if err = w.put(txn, key, action.value); err != nil {
				// Error encountered while logging put, return
				return
			}
		} else {
			if err = w.delete(txn, key); err != nil {
				// Error encountered while logging delete, return
				return
			}
		}
	}

	return
}

// merge will merge the transaction store values with the store values
func (w *WTxn) merge() {
	// Iterate through all transaction store actions
	for key, action := range w.ts {
		if action.put {
			// Put action, update value for key
			w.s[key] = action.value
		} else {
			// Delete action, remove key
			delete(w.s, key)
		}
	}
}

// Get will get a value for a provided key
func (w *WTxn) Get(key string) (value Value, err error) {
	var ok bool
	// Attempt to get from transaction store first
	if value, ok, err = w.ts.get(key); ok || err != nil {
		// We've encountered two situations:
		//	1. We've found the value (ok is true)
		//	2. The value has been deleted during this transaction (err == ErrKeyDoesNotExist)
		return
	}

	// Return results from get called directly on store
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
		// This key does not exist within the store nor the transaction
		// TODO: Add a better deletion use-case for transaction-only finds
		return
	}

	// No value is needed as this is a delete action
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
