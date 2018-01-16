package turtleDB

import (
	"io"
	"time"

	"github.com/PathDNA/turtleDB/importers"
	"github.com/itsmontoya/mrT"

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

	oi onImport
	// vey

	// Closed state
	closed atoms.Bool
}

func (s *Slave) loop(importInterval int) {
	var err error
	for !s.closed.Get() {
		s.db.logNotification("About to import from %v", s.lastTxn.Load())
		err = s.importMaster()
		switch err {
		case nil:
			s.db.logSuccess("Imported successfully to %v", s.lastTxn.Load())
			if fn := s.oi.Get(); fn != nil {
				fn()
			}

		case importers.ErrNoContent, ErrNoTxn:
			s.db.logNotification("Import attempted, no new transactions available")
		default:
			s.db.logError("Error encountered while importing: %v", err)
		}

		time.Sleep(time.Second * time.Duration(importInterval))
	}
}

// importMaster will attempt to import new transactions from master
func (s *Slave) importMaster() (err error) {
	var rc io.ReadCloser
	if rc, err = s.fn(s.lastTxn.Load()); err != nil {
		return
	}
	defer rc.Close()
	// We successfully received the reader, import reader
	return s.importReader(rc)
}

func (s *Slave) importReader(r io.Reader) (err error) {
	var ltxn string
	if ltxn, err = s.db.Import(r); err != nil {
		// We encountered an error while importing, log the error and continue on
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

// ForEachTxn will iterate through all actions from a given txn
func (s *Slave) ForEachTxn(txnID string, archive bool, fn mrT.ForEachFn) (err error) {
	return s.db.ForEachTxn(txnID, archive, fn)
}

// TxnID will return the current transaction id
func (s *Slave) TxnID() (txnID string) {
	return s.lastTxn.Load()
}

// SetVerbosity will set the verbosity level for Turtle
func (s *Slave) SetVerbosity(v Verbosity) {
	s.db.SetVerbosity(v)
}

// SetAoC will set the archive on close value
func (s *Slave) SetAoC(aoc bool) {
	s.db.SetAoC(aoc)
}

// SetOnImport will set the onImport callback func
func (s *Slave) SetOnImport(fn func()) {
	s.oi.Put(fn)
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
