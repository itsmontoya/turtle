package importers

import (
	"net/http"
	"net/url"

	"github.com/missionMeteora/toolkit/errors"
)

// ErrEmptyLoc is returned when an empty location is provided when calling a new http importer
const ErrEmptyLoc = errors.Error("empty importer location")

// NewHTTPImporter will return a new http importer
// Note: This is intended to be used for Slave DB's
func NewHTTPImporter(loc string) (hp *HTTPImporter, err error) {
	var h HTTPImporter
	if loc == "" {
		err = ErrEmptyLoc
		return
	}
	if h.url, err = url.Parse(loc); err != nil {
		return
	}

	hp = &h
	return
}

// HTTP is an http importer to be used for Slave db's
type HTTP struct {
	hc  http.Client
	url *url.URL
}

// Import will import from a given txnID and return a reader
func (h *HTTP) Import(txnID string) (r io.Reader, err error) {
	var resp *http.Response
	if resp, err = h.hc.Get(h.url.String()); err != nil {
		return
	}

	var buf bytes.Buffer
	io.Copy(buf, resp.Body)
	resp.Body.Close()
	r = buf
	return
}
