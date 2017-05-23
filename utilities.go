package turtle

// store is a basic data store
type store map[string]Value

// get will retrieve a value for a provided key
func (s store) get(key string) (value Value, err error) {
	var ok bool
	if value, ok = s[key]; !ok {
		// Value does not exist for this key
		err = ErrKeyDoesNotExist
	}

	return
}

// exists will return a boolean representing if a value exists for a provided key
func (s store) exists(key string) (ok bool) {
	_, ok = s[key]
	return
}

// txnStore is a specialized data store handling transaction actions
type txnStore map[string]*action

// get will retrieve a value for a provided key
func (t txnStore) get(key string) (value Value, ok bool, err error) {
	var a *action
	if a, ok = t[key]; !ok {
		// No actions were taken for this key during the transaction
		return
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

// exists will return a boolean representing if an action was taken for a provided key
func (t txnStore) exists(key string) (ok bool) {
	_, ok = t[key]
	return
}

type action struct {
	// put state, false assumes a delete action
	put bool
	// value of action, only looked at during put state
	value Value
}

// Txn is a basic transaction interface
type Txn interface {
	clear()

	// Get value by key
	Get(key string) (Value, error)
	// Put value by key
	Put(key string, value Value) error
	// Delete key
	Delete(key string) error
	// ForEach key/value pair
	ForEach(fn ForEachFn) error
}

// ForEachFn is used for ForEach requests
type ForEachFn func(key string, value Value) (end bool)

// TxnFn is used for transactions
type TxnFn func(txn Txn) error

// MarshalFn is for marshaling
type MarshalFn func(Value) ([]byte, error)

// UnmarshalFn is for unmarshaling
type UnmarshalFn func([]byte) (Value, error)
