package turtleDB

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
)

var (
	testVal1 = []byte("value one")
	testVal2 = []byte("value two")
	testVal3 = []byte("value three")
	testFM   = NewFuncsMap(testMarshal, testUnmarshal)
)

func TestMain(t *testing.T) {
	var (
		tdb *Turtle
		err error
	)

	if tdb, err = New("test", "./test_data", testFM); err != nil {
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

	if tdb, err = New("test", "./test_data", testFM); err != nil {
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

func testMarshal(val Value) (b []byte, err error) {
	in, ok := val.([]byte)
	if !ok {
		err = errors.New("invalid type")
		return
	}

	b = make([]byte, len(in))
	copy(b, in)
	return
}

func testUnmarshal(b []byte) (val Value, err error) {
	out := make([]byte, len(b))
	copy(out, b)
	val = out
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
	var (
		val Value
		bv  []byte
		ok  bool
	)

	if val, err = bkt.Get(key); err != nil {
		return
	}

	if bv, ok = val.([]byte); !ok {
		return fmt.Errorf("invalid type provided: %v", val)
	}

	if refStr, strVal := string(ref), string(bv); refStr != strVal {
		return fmt.Errorf("invalid value, expected %s and received %s", refStr, strVal)
	}

	return
}

func TestImportExport(t *testing.T) {
	var (
		a, b *Turtle
		err  error
	)

	if a, err = New("test", ".test_data_a", testFM); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(".test_data_a")

	if b, err = New("test", ".test_data_b", testFM); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(".test_data_b")

	a.Update(func(txn Txn) (err error) {
		var bkt Bucket
		if bkt, err = txn.Create("bkt"); err != nil {
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

		return
	})

	a.Update(func(txn Txn) (err error) {
		var bkt Bucket
		if bkt, err = txn.Get("bkt"); err != nil {
			return
		}
		return bkt.Delete("2")
	})

	buf := bytes.NewBuffer(nil)
	if err = a.Export("", buf); err != nil {
		t.Fatal(err)
	}

	if _, err = b.Import(buf); err != nil {
		t.Fatal(err)
	}

	if err = b.Read(func(txn Txn) (err error) {
		var bkt Bucket
		if bkt, err = txn.Get("bkt"); err != nil {
			return
		}

		bkt.ForEach(func(key string, val Value) (end bool) {
			b := string(val.([]byte))
			switch key {
			case "1":
				if a := string(testVal1); a != b {
					err = fmt.Errorf("invalid value, expected \"%s\" and received \"%s\"", a, b)
					return true
				}
			case "2":
				err = errors.New("Value found at key of '2', but should not exist")
				return true
			case "3":
				if a := string(testVal3); a != b {
					err = fmt.Errorf("invalid value, expected \"%s\" and received \"%s\"", a, b)
					return true
				}
			}

			return
		})
		return
	}); err != nil {
		t.Fatal(err)
	}
}
