package idempotency

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Options struct {
	HeaderName   string        // default "Idempotency-Key"
	TTL          time.Duration // default 10m: how long a cached response stays replayable
	WaitTimeout  time.Duration // default 30s: how long a concurrent retry blocks before 409
	MaxKeys      int           // default 10000: hard cap on stored entries (memory bound)
	MaxBodyBytes int64         // default 1<<20: cap on request body read for hashing
}

// hold shared table , all keys are stored here in memory
type Idempotency struct {
	header       string
	ttl          time.Duration
	waitTimeout  time.Duration
	maxKeys      int
	maxBodyBytes int64
	now          func() time.Time  // time.Now in production; swapped in tests for deterministic TTLs
	mu           sync.Mutex        // the ONE lock guarding the two fields below (and every entry field)
	entries      map[string]*entry // key -> its record
	lru          *list.List        // recency order of COMPLETED entries only, for eviction
}

// entry is the record for one key.
type entry struct {
	key         string
	fingerprint [32]byte      // sha256(method + path + body); detects "same key, different request"
	done        chan struct{} // OPEN = in progress; CLOSED = finished (success or failure)
	ready       bool          // read after done closes: true => cached success; false => it failed
	status      int
	header      http.Header
	body        []byte
	expiresAt   time.Time
	elem        *list.Element // node in lru; nil while in progress (so it can never be evicted)
}

// New builds an Idempotency, applying defaults for any zero-valued option.
func New(opts Options) *Idempotency {
	if opts.HeaderName == "" {
		opts.HeaderName = "Idempotency-Key"
	}
	if opts.TTL == 0 {
		opts.TTL = 10 * time.Minute
	}
	if opts.WaitTimeout == 0 {
		opts.WaitTimeout = 30 * time.Second
	}
	if opts.MaxKeys == 0 {
		opts.MaxKeys = 10000
	}
	if opts.MaxBodyBytes == 0 {
		opts.MaxBodyBytes = 1 << 20
	}
	return &Idempotency{
		header:       opts.HeaderName,
		ttl:          opts.TTL,
		waitTimeout:  opts.WaitTimeout,
		maxKeys:      opts.MaxKeys,
		maxBodyBytes: opts.MaxBodyBytes,
		now:          time.Now,
		entries:      make(map[string]*entry),
		lru:          list.New(),
	}
}

func (i *Idempotency) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.Header.Get(i.header))
		if key == "" { // no key , rejecct request , user can remove our middleware if wants to allow req with no keys
			http.Error(w, "Idempotency-Key required", http.StatusBadRequest)
			return
		}
		//validations
		if len(key) > 255 {
			http.Error(w, "Idempotency-Key too long", http.StatusBadRequest)
			return
		}
		body, ok := i.readBody(w, r)
		if !ok {
			return
		}
		//unique hash to prevent same kye with diff bodys
		fp := fingerprint(r.Method, r.URL.Path, body)
		for {
			e, leader, mismatch := i.begin(key, fp)
			//same key diff req body case
			if mismatch {
				http.Error(w, "Idempotency-Key reused with a different request", http.StatusUnprocessableEntity)
				return
			}
			//new key request
			if leader {
				i.execute(e, next, w, r)
				return
			}

			//else wait for running request then retry or return
			select {
			case <-e.done:
				if e.ready {
					replay(w, e)
					return
				}
				continue
			case <-r.Context().Done():
				return // client hung up; nothing to write
			case <-time.After(i.waitTimeout):
				http.Error(w, "request with this Idempotency-Key is still processing", http.StatusConflict) // 409
				return
			}
		}
	})
}

func (i *Idempotency) begin(key string, fp [32]byte) (e *entry, leader, mismatch bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if cur, found := i.entries[key]; found {
		if cur.ready && i.now().After(cur.expiresAt) {
			i.remove(cur) // expired cache entry remove
		} else {
			if cur.fingerprint != fp {
				return nil, false, true
			}
			if cur.elem != nil {
				i.lru.MoveToFront(cur.elem) // touching a cached entry refreshes recency
			}
			return cur, false, false // follower
		}
	}

	e = &entry{key: key, fingerprint: fp, done: make(chan struct{})}
	i.entries[key] = e // NOTE: not added to lru yet, so eviction can't touch an in-flight entry
	return e, true, false
}

func (i *Idempotency) execute(e *entry, next http.Handler, w http.ResponseWriter, r *http.Request) {
	cw := &captureWriter{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		if p := recover(); p != nil {
			i.fail(e)
			panic(p) // re-throw so the host's own recovery/logging middleware still runs
		}
	}()

	next.ServeHTTP(cw, r)

	if cw.status >= 500 { // failure: no cache, allowing fresh retry
		i.fail(e)
	} else { // 2xx/3xx/4xx: a deliberate, repeatable answer -> cache it
		i.complete(e, cw)
	}
}

func (i *Idempotency) complete(e *entry, cw *captureWriter) {
	i.mu.Lock()
	e.status = cw.status
	e.header = cw.Header().Clone()
	e.body = cw.body
	e.ready = true
	e.expiresAt = i.now().Add(i.ttl)
	e.elem = i.lru.PushFront(e) // now eviction-eligible
	i.evict()
	i.mu.Unlock()
	close(e.done) // broadcast to all writeer req is done, future writers will receive immediate response
}

func (i *Idempotency) fail(e *entry) {
	i.mu.Lock()
	if i.entries[e.key] == e { // still the mapped entry (a leader owns its key until it finishes)
		i.remove(e)
	}
	i.mu.Unlock()
	close(e.done) // waiters wake, see ready==false, and re-execute
}

// remove drops an entry from the map and (if present) the lru list. Caller holds mu.
func (i *Idempotency) remove(e *entry) {
	delete(i.entries, e.key)
	if e.elem != nil {
		i.lru.Remove(e.elem)
		e.elem = nil
	}
}

func (i *Idempotency) evict() {
	for len(i.entries) > i.maxKeys {
		back := i.lru.Back()
		if back == nil {
			return // everything is in flight; accept a temporary soft overage
		}
		i.remove(back.Value.(*entry))
	}
}

func (i *Idempotency) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, i.maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, true
}

// hashing body to prenvent user from sending diff body with same keys
func fingerprint(method, path string, body []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

// captureWriter records status and body while writing through to the real client,
// so the leader's response can be replayed to later callers.
type captureWriter struct {
	http.ResponseWriter
	status      int
	body        []byte
	wroteHeader bool
}

func (c *captureWriter) WriteHeader(status int) {
	if c.wroteHeader {
		return
	}
	c.status = status
	c.wroteHeader = true
	c.ResponseWriter.WriteHeader(status)
}

func (c *captureWriter) Write(b []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	c.body = append(c.body, b...)
	return c.ResponseWriter.Write(b)
}

// replay writes a completed entry's recorded response to a later caller.
func replay(w http.ResponseWriter, e *entry) {
	for k, vs := range e.header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("Idempotent-Replayed", "true")
	w.WriteHeader(e.status)
	_, _ = w.Write(e.body)
}
