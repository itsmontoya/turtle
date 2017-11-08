package turtleDB

import (
	"testing"
)

func TestVerbosity(t *testing.T) {
	v := DefaultVerbosity
	if !v.CanError() {
		t.Fatal("cannot error when it should be able to")
	}

	if v.CanSuccess() {
		t.Fatal("can success when it shouldn't be able to")
	}

	if v.CanNotify() {
		t.Fatal("can notify when it shouldn't be able to")
	}

	v = AllVerbosity
	if !v.CanError() {
		t.Fatal("cannot error when it should be able to")
	}

	if !v.CanSuccess() {
		t.Fatal("cannot success when it should be able to")
	}

	if !v.CanNotify() {
		t.Fatal("cannot notify when it should be able to")
	}
}
