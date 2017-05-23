package turtle

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestMain(t *testing.T) {
	var (
		tdb *Turtle
		err error
	)

	if tdb, err = New("test", "./data", testMarshal, testUnmarshal); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Update(func(txn Txn) (err error) {
		ts := &testStruct{
			Name: "John Doe",
			Age:  32,
		}

		return txn.Put("0", ts)
	}); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Read(func(txn Txn) (err error) {
		ts := &testStruct{
			Name: "Foo",
			Age:  13,
		}

		if err := txn.Put("0", ts); err == nil {
			return fmt.Errorf("nil error encountered when error was expected")
		}

		var val Value
		if val, err = txn.Get("0"); err != nil {
			return
		}

		var ok bool
		if ts, ok = val.(*testStruct); !ok {
			return fmt.Errorf("invalid type provided: %v", val)
		}

		if ts.Name != "John Doe" {
			return fmt.Errorf("invalid name provided, expected %s and received %s", "John Doe", ts.Name)
		}

		if ts.Age != 32 {
			return fmt.Errorf("invalid age provided, expected %d and received %d", 32, ts.Age)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Close(); err != nil {
		t.Fatal(err)
	}

	if tdb, err = New("test", "./data", testMarshal, testUnmarshal); err != nil {
		t.Fatal(err)
	}

	if err = tdb.Read(func(txn Txn) (err error) {
		var (
			ts  *testStruct
			val Value
		)

		if val, err = txn.Get("0"); err != nil {
			return
		}

		var ok bool
		if ts, ok = val.(*testStruct); !ok {
			return fmt.Errorf("invalid type provided: %v", val)
		}

		if ts.Name != "John Doe" {
			return fmt.Errorf("invalid name provided, expected %s and received %s", "John Doe", ts.Name)
		}

		if ts.Age != 32 {
			return fmt.Errorf("invalid age provided, expected %d and received %d", 32, ts.Age)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}
}

func testMarshal(val Value) (b []byte, err error) {
	var (
		ts *testStruct
		ok bool
	)

	if ts, ok = val.(*testStruct); !ok {
		err = fmt.Errorf("invalid type provided: %v", val)
		return
	}

	return json.Marshal(ts)
}

func testUnmarshal(b []byte) (val Value, err error) {
	var ts testStruct
	if err = json.Unmarshal(b, &ts); err != nil {
		return
	}

	val = ts
	return
}

type testStruct struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}
