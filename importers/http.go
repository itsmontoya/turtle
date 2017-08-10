package importers

import (
	"bytes"
	"io"
	"net/http"
	"net/url"

	"github.com/missionMeteora/toolkit/errors"
)

// ErrEmptyLoc is returned when an empty location is provided when calling a new http importer
const ErrEmptyLoc = errors.Error("empty importer location")

// NewHTTP will return a new http importer
func NewHTTP(loc string) (hp *HTTP, err error) {
	var h HTTP
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

// SetJar will set a cookie jar for an HTTP importer
func (h *HTTP) SetJar(jar http.CookieJar) {
	h.hc.Jar = jar
}

// Import will import from a given txnID and return a reader
func (h *HTTP) Import(txnID string) (r io.Reader, err error) {
	var resp *http.Response
	if resp, err = h.hc.Get(h.url.String()); err != nil {
		return
	}

	buf := bytes.NewBuffer(nil)
	io.Copy(buf, resp.Body)
	resp.Body.Close()
	r = buf
	return
}
