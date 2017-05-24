package bytes

import (
	"fmt"
	"os"
	"testing"
)

func TestBytes(t *testing.T) {
	var (
		db  *DB
		err error
	)

	if db, err = New("test", "./data", marshal, unmarshal); err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll("./data")

	if err = db.Update(func(txn Txn) (err error) {
		return txn.Put("greeting", []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	if db, err = New("test", "./data", marshal, unmarshal); err != nil {
		t.Fatal(err)
	}

	if err = db.Read(func(txn Txn) (err error) {
		var b []byte
		if b, err = txn.Get("greeting"); err != nil {
			return
		}

		if string(b) == "hello world!" {
			return
		}

		return fmt.Errorf("invalid value, expected %s and received %s", "hello world!", string(b))
	}); err != nil {
		t.Fatal(err)
	}
}

func marshal(b []byte) ([]byte, error) {
	return b, nil
}

func unmarshal(b []byte) ([]byte, error) {
	return b, nil
}
