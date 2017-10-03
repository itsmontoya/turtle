package importers

import (
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"

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
	mux sync.RWMutex

	hc  http.Client
	url *url.URL
	// HTTP headers
	headers http.Header
}

func (h *HTTP) newRequest(txnID string) (req *http.Request, err error) {
	oPath := h.url.Path
	defer func() {
		// Set path to original path before txnID
		h.url.Path = oPath
	}()

	h.url.Path = path.Join(oPath, txnID)
	if req, err = http.NewRequest("GET", h.url.String(), nil); err != nil {
		return
	}

	if h.headers == nil {
		return
	}

	req.Header = h.headers
	return
}

// SetJar will set a cookie jar for an HTTP importer
func (h *HTTP) SetJar(jar http.CookieJar) {
	h.mux.Lock()
	defer h.mux.Unlock()

	h.hc.Jar = jar
}

// SetHeader will set an http header
func (h *HTTP) SetHeader(key string, values ...string) {
	h.mux.Lock()
	defer h.mux.Unlock()

	if h.headers == nil {
		h.headers = make(http.Header)
	}

	h.headers[key] = values
}

// Import will import from a given txnID and return a reader
func (h *HTTP) Import(txnID string) (rc io.ReadCloser, err error) {
	h.mux.RLock()
	defer h.mux.RUnlock()

	var req *http.Request
	if req, err = h.newRequest(txnID); err != nil {
		return
	}

	var resp *http.Response
	if resp, err = h.hc.Do(req); err != nil {
		return
	}

	rc = resp.Body
	return
}
