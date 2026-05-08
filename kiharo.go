// Package kiharo implements an adaptive hedged-requests pattern for reducing tail latency.
//
// kiharo accumulates per-key latency statistics from successful first attempts
// and uses a configurable percentile of those latencies as the delay before
// launching hedged attempts. No external metrics system is required.
package kiharo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Errors returned by Hedger.
var (
	// ErrUnknownKey is returned when Do is called with a key that was never
	// passed to Register.
	ErrUnknownKey = errors.New("kiharo: unknown key (did you call Register?)")

	// ErrNilFunc is returned when the function passed to Do is nil.
	ErrNilFunc = errors.New("kiharo: function must not be nil")

	// ErrNilHedger is returned when Do is called with a nil *Hedger.
	ErrNilHedger = errors.New("kiharo: Hedger is nil")
)

// Option configures a Hedger at construction time.
type Option func(*Hedger)

// WithMetrics sets the metrics recorder used for all keys on this Hedger.
// If unset, metrics are silently dropped.
func WithMetrics(r MetricsRecorder) Option {
	return func(h *Hedger) {
		if r != nil {
			h.metrics = r
		}
	}
}

// Hedger holds per-key latency statistics and dispatches hedged calls.
//
// One Hedger per application is the expected pattern. Construct via New;
// the zero value is not usable.
//
// Hedger is safe for concurrent use.
type Hedger struct {
	// keys: string -> *stats. Read on every Do, written only at Register
	// time, so sync.Map's read-mostly path fits well.
	keys    sync.Map
	metrics MetricsRecorder
}

// New creates a new Hedger.
func New(opts ...Option) *Hedger {
	h := &Hedger{metrics: noopRecorder{}}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Register configures a key. It must be called before Do is called with the
// same key.
//
// Register panics on invalid cfg or duplicate keys, following the http.Handle
// precedent: registration is a startup-time concern, and silent overwrites
// are almost always bugs.
func (h *Hedger) Register(key string, cfg RegisterConfig) {
	if err := cfg.validate(); err != nil {
		panic(fmt.Sprintf("kiharo: invalid RegisterConfig for key %q: %v", key, err))
	}
	s := newStats(cfg)
	if _, loaded := h.keys.LoadOrStore(key, s); loaded {
		panic(fmt.Sprintf("kiharo: key %q already registered", key))
	}
}

// stats returns the *stats for key, or nil if unknown.
func (h *Hedger) statsFor(key string) *stats {
	v, ok := h.keys.Load(key)
	if !ok {
		return nil
	}
	return v.(*stats)
}

// Do executes fn up to MaxCalls times in parallel, using the adaptive delay
// schedule for the registered key. The first successful result wins; remaining
// in-flight attempts are cancelled via context.
//
// Latency of the first attempt — and only the first attempt, only when it
// succeeds — is recorded into the key's sliding window. This keeps the baseline
// clean: hedged latencies do not feed back into the percentile.
//
// If every attempt fails, Do returns the error from the first attempt that
// completed. If ctx is cancelled before any attempt completes, Do returns ctx.Err().
//
// Do is a top-level function rather than a method on *Hedger because Go does
// not currently allow type parameters on methods.
//
// fn must be safe for concurrent execution; each invocation receives a child
// context derived from ctx.
func Do[T any](
	ctx context.Context,
	h *Hedger,
	key string,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	var zero T

	if h == nil {
		return zero, ErrNilHedger
	}
	if fn == nil {
		return zero, ErrNilFunc
	}

	s := h.statsFor(key)
	if s == nil {
		return zero, fmt.Errorf("%w: %q", ErrUnknownKey, key)
	}

	maxCalls := s.cfg.MaxCalls
	metrics := h.metrics

	// Fast path: no hedging configured for this key.
	if maxCalls == 1 {
		start := time.Now()
		metrics.RecordRequest(false)
		v, err := fn(ctx)
		dur := time.Since(start)
		metrics.RecordResponse(false, err, dur)
		if err == nil {
			s.observe(dur)
		}
		return v, err
	}

	// Cancellable scope shared by all attempts. cancel() fires once we pick
	// a winner so still-running attempts can tear down promptly.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		value  T
		err    error
		hedged bool
	}
	results := make(chan result, maxCalls)
	var wg sync.WaitGroup

	launch := func(attempt int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hedged := attempt > 1
			start := time.Now()
			metrics.RecordRequest(hedged)
			v, err := fn(runCtx)
			dur := time.Since(start)
			metrics.RecordResponse(hedged, err, dur)
			// Only the first attempt's successful latency feeds the window.
			// Otherwise smaller delays would produce more hedged samples and
			// push the percentile down further — a feedback loop.
			if !hedged && err == nil {
				s.observe(dur)
			}
			select {
			case results <- result{value: v, err: err, hedged: hedged}:
			case <-runCtx.Done():
			}
		}()
	}

	// Scheduler: launch attempt 1 immediately, then read s.delay() fresh
	// before each subsequent attempt. Reading fresh lets a fast first
	// attempt update the window before attempt 3 is scheduled.
	go func() {
		launch(1)
		for attempt := 2; attempt <= maxCalls; attempt++ {
			d := s.delay()
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
				launch(attempt)
			case <-runCtx.Done():
				timer.Stop()
				return
			}
		}
	}()

	var firstErr error
	failed := 0

	for {
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case r := <-results:
			if r.err == nil {
				if r.hedged {
					metrics.RecordHedgeWin()
				}
				return r.value, nil
			}
			if firstErr == nil {
				firstErr = r.err
			}
			failed++
			if failed >= maxCalls {
				return zero, firstErr
			}
		}
	}
}
