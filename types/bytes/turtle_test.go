package turtleDB

import (
	"fmt"
	"os"
	"testing"
)

var (
	testVal1 = []byte("value one")
	testVal2 = []byte("value two")
	testVal3 = []byte("value three")
)

func TestMain(t *testing.T) {
	var (
		tdb *Turtle
		err error
	)

	fm := NewFuncsMap(testMarshal, testUnmarshal)

	if tdb, err = New("test", "./test_data", fm); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if err = tdb.Update(func(txn Txn) (err error) {
		var bkt Bucket
		if bkt, err = txn.Create("TEST_BKT"); err != nil {
			return
		}

		if err = bkt.Put("1", testVal1); err != nil {
			return
		}

		if err = bkt.Put("2", testVal2); err != nil {
			return
		}

		if err = bkt.Put("3", testVal3); err != nil {
			return
		}

		if bkt, err = txn.Create("TEST_BKT_2"); err != nil {
			return
		}

		if err = bkt.Put("3", testVal3); err != nil {
			return
		}

		return testCheckValues(txn)
	}); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Read(testReadCheck); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Close(); err != nil {
		t.Fatal(err)
	}

	if tdb, err = New("test", "./test_data", fm); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Read(testReadCheck); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Update(func(txn Txn) (err error) {
		if err = testCheckValues(txn); err != nil {
			return
		}

		if _, err = txn.Get("TEST_BKT"); err != nil {
			return
		}

		if err = txn.Delete("TEST_BKT"); err != nil {
			return
		}

		if _, err = txn.Get("TEST_BKT"); err == nil {
			return fmt.Errorf("nil error encountered when error was expected")
		}

		if _, err = txn.Create("TEST_BKT"); err != nil {
			return
		}

		if err = testCheckValues(txn); err == nil {
			return fmt.Errorf("nil error encountered when error was expected")
		}

		err = nil
		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Close(); err != nil {
		t.Fatal(err)
	}
}

func testMarshal(in []byte) (b []byte, err error) {
	b = make([]byte, len(in))
	copy(b, in)
	return
}

func testUnmarshal(b []byte) (out []byte, err error) {
	out = make([]byte, len(b))
	copy(out, b)
	return
}

func testReadCheck(txn Txn) (err error) {
	var bkt Bucket
	if bkt, err = txn.Get("TEST_BKT"); err != nil {
		return
	}

	if err = bkt.Put("1", testVal1); err == nil {
		return fmt.Errorf("nil error encountered when error was expected")
	}

	return testCheckValues(txn)
}

func testCheckValues(txn Txn) (err error) {
	var bkt Bucket
	if bkt, err = txn.Get("TEST_BKT"); err != nil {
		return
	}

	if err = testCheckValue(bkt, "1", testVal1); err != nil {
		return
	}

	if err = testCheckValue(bkt, "2", testVal2); err != nil {
		return
	}

	if err = testCheckValue(bkt, "3", testVal3); err != nil {
		return
	}

	if bkt, err = txn.Get("TEST_BKT_2"); err != nil {
		return
	}

	if err = testCheckValue(bkt, "3", testVal3); err != nil {
		return
	}

	return
}

func testCheckValue(bkt Bucket, key string, ref []byte) (err error) {
	var bv []byte
	if bv, err = bkt.Get(key); err != nil {
		return
	}

	if refStr, strVal := string(ref), string(bv); refStr != strVal {
		return fmt.Errorf("invalid value, expected %s and received %s", refStr, strVal)
	}

	return
}
