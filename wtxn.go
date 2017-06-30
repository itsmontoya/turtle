package turtleDB

import "github.com/itsmontoya/mrT"

// wTxn represents a write transactgion
type wTxn struct {
	b  *buckets
	tb *txnBuckets

	// Marshal func
	mfn MarshalFn
}

func (w *wTxn) clear() {
	// Set store reference to nil
	w.b = nil
	// Set transaction store reference to nil
	w.tb = nil
	// Set the marshal func to nil
	w.mfn = nil
}

// put is a QoL func to log a put action
func (w *wTxn) put(txn *mrT.Txn, key []byte, value Value) (err error) {
	var b []byte
	// Attempt to marshal value as bytes
	if b, err = w.mfn(value); err != nil {
		// Marshal error encountered, return
		return
	}

	// Log action to disk
	if err = txn.Put(key, b); err != nil {
		return
	}

	return
}

// delete is a QoL func to log a delete action
func (w *wTxn) delete(txn *mrT.Txn, key []byte) error {
	// Log action to disk
	return txn.Delete(key)
}

// commit will log all actions to disk
func (w *wTxn) commit(txn *mrT.Txn) (err error) {
	for bktKey, bkt := range w.tb.m {
		for refKey, a := range bkt.m {
			// Get merged key
			key := mergeKeys(string(bktKey), string(refKey))

			// If action.put is true, put action
			// Else, delete action
			if a.put {
				if err = w.put(txn, key, a.value); err != nil {
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
	}

	return
}

// merge will merge the transaction store values with the store values
func (w *wTxn) merge() {
	for bktKey, bkt := range w.tb.m {
		for refKey, a := range bkt.m {
			if a.put {
				w.b.create(bktKey).put(refKey, a.value)
			} else {
				bb, err := w.b.get(bktKey)
				if err != nil {
					continue
				}

				bb.delete(refKey)
			}
		}
	}
}

// Get will get a bucket
func (w *wTxn) Get(key string) (b Bucket, err error) {
	return w.tb.Get(key)
}

// Create will create a bucket
func (w *wTxn) Create(key string) (b Bucket, err error) {
	return w.tb.Create(key)
}

func (w *wTxn) Delete(key string) (err error) {
	return w.tb.Delete(key)
}

// ForEach will iterate through all buckets
func (w *wTxn) ForEach(fn ForEachBucketFn) {
	w.tb.ForEach(fn)
}
