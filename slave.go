package turtleDB

import (
	"io"
	"time"

	// HTTP imports
	"net/http"
	"net/url"

	"github.com/Path94/atoms"
	"github.com/missionMeteora/journaler"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrEmptyImporter is reutrned when the importer is nil when calling NewSlave
	ErrEmptyImporter = errors.Error("importer cannot be nil")
)

// NewSlave will return a new slave
// Note: importInterval is interval time in seconds
func NewSlave(name, path string, fm FuncsMap, imp Importer, importInterval int) (sp *Slave, err error) {
	var s Slave
	if fn == nil {
		// Import function cannot be nil, return
		err = ErrEmptyImporter
		return
	}

	if s.db, err = New(name, path, fm); err != nil {
		// Error initializing db, return
		return
	}

	// Set journaler for Stdout logging
	s.out = journaler.New("TurtleDB", name)
	// Set import func
	s.fn = imp.Import

	// Start update loop in a new goroutine
	go s.loop(importInterval)
	return
}

// Slave is a read-only db
type Slave struct {
	db  *Turtle
	out *journaler.Journaler
	// Function used for importing on loop calls
	fn ImportFn
	// Last transaction id
	lastTxn atoms.String
	// Closed state
	closed atoms.Bool
}

func (s *Slave) loop(importInterval int) {
	var (
		r   io.Reader
		err error
	)

	for !s.closed.Get() {
		if r, err = s.fn(s.lastTxn.Load()); err != nil {
			// We encountered an error while importing, log the error and continue on
			s.out.Error("Error importing: %v", err)
		} else {
			// We successfully received the reader, import reader
			s.importReader(r)
		}

		time.Sleep(time.Second * time.Duration(importInterval))
	}
}

func (s *Slave) importReader(r io.Reader) {
	var (
		ltxn string
		err  error
	)

	if ltxn, err = s.db.Import(r); err != nil {
		// We encountered an error while importing, log the error and continue on
		s.out.Error("Error importing: %v", err)
		return
	}

	// Update the last txn value
	s.lastTxn.Store(ltxn)
	return
}

// Read opens a read transaction
func (s *Slave) Read(fn TxnFn) (err error) {
	return s.db.Read(fn)
}

// Update opens an update transaction
func (s *Slave) Update(fn TxnFn) (err error) {
	return ErrSlaveUpdate
}

// TxnID will return the current transaction id
func (s *Slave) TxnID() (txnID string) {
	return s.lastTxn.Load()
}

// Close will close the slave
func (s *Slave) Close() (err error) {
	if !s.closed.Set(true) {
		return errors.ErrIsClosed
	}

	return s.db.Close()
}

// ImportFn is called when an import is requested
type ImportFn func(txnID string) (io.Reader, error)

// Importer is the interface needed to call import for Slave DBs
type Importer interface {
	Import(txnID string) (r io.Reader, err error)
}
