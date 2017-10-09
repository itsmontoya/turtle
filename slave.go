package turtleDB

import (
	"io"
	"time"

	"github.com/PathDNA/turtleDB/importers"

	"github.com/PathDNA/atoms"
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
	if imp == nil {
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

	s.db.Read(func(txn Txn) (err error) {
		lastTxn, err := s.db.mrT.LastTxn()
		s.lastTxn.Store(lastTxn)
		return
	})

	// Start update loop in a new goroutine
	go s.loop(importInterval)

	sp = &s
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
		rc  io.ReadCloser
		err error
	)

	for !s.closed.Get() {
		if rc, err = s.fn(s.lastTxn.Load()); err != nil {
			if err != importers.ErrNoContent {
				// We encountered an error while importing, log the error and continue on
				s.out.Error("Error importing: %v", err)
			}
		} else {
			// We successfully received the reader, import reader
			s.importReader(rc)
			rc.Close()
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

	if ltxn == "" {
		// No import occurred, do not update txn
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

// Import will process an export
func (s *Slave) Import(r io.Reader) (txnID string, err error) {
	return "", ErrSlaveUpdate
}

// Export will stream
func (s *Slave) Export(txnID string, w io.Writer) (err error) {
	return s.db.Export(txnID, w)
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
type ImportFn func(txnID string) (io.ReadCloser, error)

// Importer is the interface needed to call import for Slave DBs
type Importer interface {
	Import(txnID string) (rc io.ReadCloser, err error)
}
