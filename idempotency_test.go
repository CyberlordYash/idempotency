package idempotency

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// do fires a single POST at the middleware and returns the recorder.
func do(mw http.Handler, key, body string) *httptest.ResponseRecorder {
	return doPath(mw, "/charge", key, body)
}

func doPath(mw http.Handler, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)
	return rec
}

func okHandler(counter *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		fmt.Fprint(w, "ok")
	}
}

func TestReplaysDuplicate(t *testing.T) {
	var calls atomic.Int64
	mw := New(Options{}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("X-Charge", "ch_1")
		fmt.Fprint(w, "created")
	}))

	first := do(mw, "k", "b")
	if first.Header().Get("Idempotent-Replayed") != "" {
		t.Fatal("the original response should not be flagged as a replay")
	}

	second := do(mw, "k", "b")
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	if got := second.Body.String(); got != "created" {
		t.Fatalf("replay body = %q, want %q", got, "created")
	}
	if second.Header().Get("X-Charge") != "ch_1" {
		t.Fatal("replay dropped a header the handler had set")
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("replay was not marked with Idempotent-Replayed")
	}
}

// The core promise: many identical retries racing at once still only execute
// the handler a single time, and every caller sees the same response.
func TestConcurrentSameKeyRunsOnce(t *testing.T) {
	var calls atomic.Int64
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	mw := New(Options{}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		fmt.Fprintf(w, "run-%d", n)
	}))

	const n = 20
	var wg sync.WaitGroup
	results := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = do(mw, "same", "body").Body.String()
		}(i)
	}

	<-entered      // wait until one of them is inside the handler
	close(release) // then let everyone finish
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	for i, r := range results {
		if r != "run-1" {
			t.Fatalf("caller %d got %q, want run-1", i, r)
		}
	}
}

// Different keys must not block each other; this is the main race-detector workout.
func TestConcurrentDistinctKeys(t *testing.T) {
	var calls atomic.Int64
	mw := New(Options{}).Middleware(okHandler(&calls))

	var wg sync.WaitGroup
	for k := 0; k < 40; k++ {
		key := fmt.Sprintf("k%d", k)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				do(mw, key, "b")
			}(key)
		}
	}
	wg.Wait()

	if calls.Load() != 40 {
		t.Fatalf("handler ran %d times, want 40 (one per key)", calls.Load())
	}
}

func TestRejectsMismatchedBody(t *testing.T) {
	var calls atomic.Int64
	mw := New(Options{}).Middleware(okHandler(&calls))

	if rec := do(mw, "k", "body-A"); rec.Code != http.StatusOK {
		t.Fatalf("first request got %d", rec.Code)
	}
	if rec := do(mw, "k", "body-B"); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reused key with new body got %d, want 422", rec.Code)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
}

func TestRejectsMismatchedPath(t *testing.T) {
	mw := New(Options{}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	if rec := doPath(mw, "/a", "k", "b"); rec.Code != http.StatusOK {
		t.Fatalf("first request got %d", rec.Code)
	}
	if rec := doPath(mw, "/b", "k", "b"); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("same key on a different path got %d, want 422", rec.Code)
	}
}

func TestServerErrorNotCached(t *testing.T) {
	var calls atomic.Int64
	mw := New(Options{}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "try again", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "ok")
	}))

	if rec := do(mw, "k", "b"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("first got %d, want 500", rec.Code)
	}
	rec := do(mw, "k", "b")
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("retry got %d %q, want 200 ok", rec.Code, rec.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", calls.Load())
	}
}

func TestClientErrorCached(t *testing.T) {
	var calls atomic.Int64
	mw := New(Options{}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "nope", http.StatusBadRequest)
	}))

	do(mw, "k", "b")
	do(mw, "k", "b")
	if calls.Load() != 1 {
		t.Fatalf("a 4xx should be cached, but handler ran %d times", calls.Load())
	}
}

func TestPanicPropagatesAndFreesKey(t *testing.T) {
	var calls atomic.Int64
	mw := New(Options{}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		panic("boom")
	}))

	expectPanic := func() {
		defer func() {
			if recover() == nil {
				t.Error("panic did not propagate to the caller")
			}
		}()
		do(mw, "k", "b")
	}

	expectPanic()
	expectPanic() // key must have been freed, so the handler runs again

	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", calls.Load())
	}
}

// A retry that was parked on the in-flight request must re-run the handler when
// that request ends up failing, not replay the failure.
func TestWaiterReexecutesAfterLeaderFails(t *testing.T) {
	var calls atomic.Int64
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	mw := New(Options{}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "ok")
	}))

	leaderCode := make(chan int, 1)
	go func() { leaderCode <- do(mw, "k", "b").Code }()
	<-entered

	waiter := make(chan *httptest.ResponseRecorder, 1)
	go func() { waiter <- do(mw, "k", "b") }()

	close(release)
	if code := <-leaderCode; code != http.StatusInternalServerError {
		t.Fatalf("leader got %d, want 500", code)
	}
	got := <-waiter
	if got.Code != http.StatusOK || got.Body.String() != "ok" {
		t.Fatalf("waiter got %d %q, want 200 ok", got.Code, got.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", calls.Load())
	}
}

func TestStillProcessingTimesOut(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	mw := New(Options{WaitTimeout: 80 * time.Millisecond}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		fmt.Fprint(w, "ok")
	}))

	leaderDone := make(chan struct{})
	go func() { do(mw, "k", "b"); close(leaderDone) }()
	<-entered

	rec := do(mw, "k", "b") // leader is stuck; this one should give up
	if rec.Code != http.StatusConflict {
		t.Fatalf("waiter got %d, want 409", rec.Code)
	}

	close(release)
	<-leaderDone
}

func TestExpiredKeyReexecutes(t *testing.T) {
	var calls atomic.Int64
	idem := New(Options{TTL: time.Minute})
	clock := time.Unix(1000, 0)
	idem.now = func() time.Time { return clock }
	mw := idem.Middleware(okHandler(&calls))

	do(mw, "k", "b")
	do(mw, "k", "b")
	if calls.Load() != 1 {
		t.Fatalf("within TTL handler ran %d times, want 1", calls.Load())
	}

	clock = clock.Add(2 * time.Minute) // let the cached entry expire
	do(mw, "k", "b")
	if calls.Load() != 2 {
		t.Fatalf("after TTL handler ran %d times, want 2", calls.Load())
	}
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	var calls atomic.Int64
	idem := New(Options{MaxKeys: 2})
	mw := idem.Middleware(okHandler(&calls))

	do(mw, "a", "b")
	do(mw, "b", "b")
	do(mw, "c", "b") // over the cap -> "a" (least recently used) is dropped

	idem.mu.Lock()
	size := len(idem.entries)
	idem.mu.Unlock()
	if size != 2 {
		t.Fatalf("stored %d entries, want 2", size)
	}

	before := calls.Load()
	do(mw, "b", "b") // still cached
	if calls.Load() != before {
		t.Fatal("b should have replayed from cache")
	}
	do(mw, "a", "b") // was evicted, so it runs again
	if calls.Load() != before+1 {
		t.Fatal("a should have re-run after eviction")
	}
}

func TestMissingKeyRejected(t *testing.T) {
	var calls atomic.Int64
	mw := New(Options{}).Middleware(okHandler(&calls))

	rec := do(mw, "", "b")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing key got %d, want 400", rec.Code)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times, want 0", calls.Load())
	}
}

func TestOverlongKeyRejected(t *testing.T) {
	mw := New(Options{}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	rec := do(mw, strings.Repeat("x", 256), "b")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("overlong key got %d, want 400", rec.Code)
	}
}

func TestOversizeBodyRejected(t *testing.T) {
	mw := New(Options{MaxBodyBytes: 8}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	rec := do(mw, "k", "this body is clearly longer than eight bytes")
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize body got %d, want 413", rec.Code)
	}
}
