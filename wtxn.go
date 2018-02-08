package turtleDB

import "github.com/itsmontoya/mrT"

// wTxn represents a write transactgion
type wTxn struct {
	b  *buckets
	tb *txnBuckets
	fm FuncsMap
}

func (w *wTxn) clear() {
	// Set store reference to nil
	w.b = nil
	// Set transaction store reference to nil
	w.tb = nil
	// Set the funcs map to nil
	w.fm = nil
}

// put is a QoL func to log a put action
func (w *wTxn) put(txn *mrT.Txn, bktKey, refKey string, value Value) (err error) {
	var fns *Funcs
	if fns, err = w.fm.Get(bktKey); err != nil {
		return
	}

	var b []byte
	// Attempt to marshal value as bytes
	if b, err = fns.Marshal(value); err != nil {
		// Marshal error encountered, return
		return
	}

	// Get merged key
	key := mergeKeys(bktKey, refKey)

	// Log action to disk
	if err = txn.Put(key, b); err != nil {
		return
	}

	return
}

// delete is a QoL func to log a delete action
func (w *wTxn) delete(txn *mrT.Txn, bktKey, refKey string) error {
	// Get merged key
	key := mergeKeys(bktKey, refKey)

	// Log action to disk
	return txn.Delete(key)
}

// commit will log all actions to disk
func (w *wTxn) commit(txn *mrT.Txn) (err error) {
	for bktKey, bkt := range w.tb.m {
		for refKey, a := range bkt.m {
			// If action.put is true, put action
			// Else, delete action
			if a.put {
				if err = w.put(txn, string(bktKey), string(refKey), a.value); err != nil {
					// Error encountered while logging put, return
					return
				}
			} else {
				if err = w.delete(txn, string(bktKey), string(refKey)); err != nil {
					// Error encountered while logging delete, return
					return
				}
			}
		}

		if bkt.deleted {
			if err = w.delete(txn, string(bktKey), ""); err != nil {
				// Error encountered while logging delete, return
				return
			}
		}
	}

	return
}

// merge will merge the transaction store values with the store values
func (w *wTxn) merge() {
	for bktKey, bkt := range w.tb.m {
		if bkt.deleted {
			// If the bucket was deleted, we don't have to do anything fancy. Just delete and move on
			w.b.delete(bktKey)
			continue
		}

		bb := w.b.create(bktKey)

		for refKey, a := range bkt.m {
			if a.put {
				bb.put(refKey, a.value)
				continue
			}

			bb.delete(refKey)
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
func (w *wTxn) ForEach(fn ForEachBucketFn) error {
	return w.tb.ForEach(fn)
}
