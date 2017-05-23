package turtle

type store map[string]Value

func (s store) get(key string) (value Value, err error) {
	var ok bool
	if value, ok = s[key]; !ok {
		err = ErrKeyDoesNotExist
	}

	return
}

func (s store) exists(key string) (ok bool) {
	_, ok = s[key]
	return
}

type txnStore map[string]*action

func (t txnStore) get(key string) (value Value, ok bool, err error) {
	var a *action
	if a, ok = t[key]; !ok {
		return
	}

	if !a.put {
		err = ErrKeyDoesNotExist
		return
	}

	value = a.value
	return
}

func (t txnStore) exists(key string) (ok bool) {
	_, ok = t[key]
	return
}

type action struct {
	put   bool
	value Value
}

// Txn is a basic transaction interface
type Txn interface {
	clear()

	Get(key string) (Value, error)
	Put(key string, value Value) error
	Delete(key string) error
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
