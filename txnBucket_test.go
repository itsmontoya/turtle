package turtleDB

import (
	"testing"
)

func TestTxnBucket(t *testing.T) {
	var (
		val Value
		err error
	)

	b := newTxnBucket(nil)

	if _, err = b.Get("0"); err == nil {
		t.Fatal("error not encountered when expected")
	}

	if err = b.Put("0", "hello_world"); err != nil {
		t.Fatal(err)
	}

	if val, err = b.Get("0"); err != nil {
		t.Fatal(err)
	}

	if str := val.(string); str != "hello_world" {
		t.Fatalf("invalid value, expected %s and received %s", "hello_world", str)
	}

	var cnt int
	b.ForEach(func(key string, val Value) error {
		cnt++
		return nil
	})

	if cnt != 1 {
		t.Fatalf("invalid count, expected %d and received %d", 1, cnt)
	}

	if err = b.Put("1", "hello_world"); err != nil {
		t.Fatal(err)
	}

	cnt = 0
	b.ForEach(func(key string, val Value) error {
		cnt++
		return Break
	})

	if cnt != 1 {
		t.Fatalf("invalid count, expected %d and received %d", 1, cnt)
	}

	if err = b.Delete("0"); err != nil {
		t.Fatal(err)
	}

	if val, err = b.Get("0"); err == nil {
		t.Fatal("error not encountered when expected")
	}
}
