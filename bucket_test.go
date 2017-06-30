package turtleDB

import (
	"testing"
)

func TestBucket(t *testing.T) {
	var err error
	b := newBucket()

	if _, err = b.Get("0"); err == nil {
		t.Fatal("error not encountered when expected")
	}

	if err = b.Put("0", "hello_world"); err == nil {
		t.Fatal("error not encountered when expected")
	}
}
