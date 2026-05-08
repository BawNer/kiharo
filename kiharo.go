package kiharo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrUnknownKey = errors.New("kiharo: unknown key (did you call Register?)")
	ErrNilFunc    = errors.New("kiharo: function must not be nil")
	ErrNilHedger  = errors.New("kiharo: Hedger is nil")
)

type Option func(*Hedger)

func WithMetrics(r MetricsRecorder) Option {
	return func(h *Hedger) {
		if r != nil {
			h.metrics = r
		}
	}
}

type Hedger struct {
	keys    sync.Map
	metrics MetricsRecorder
}

func New(opts ...Option) *Hedger {
	h := &Hedger{metrics: noopRecorder{}}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *Hedger) Register(key string, cfg RegisterConfig) {
	if err := cfg.validate(); err != nil {
		panic(fmt.Sprintf("kiharo: invalid RegisterConfig for key %q: %v", key, err))
	}
	s := newStats(cfg)
	if _, loaded := h.keys.LoadOrStore(key, s); loaded {
		panic(fmt.Sprintf("kiharo: key %q already registered", key))
	}
}

func (h *Hedger) statsFor(key string) *stats {
	v, ok := h.keys.Load(key)
	if !ok {
		return nil
	}
	return v.(*stats)
}

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

	cfg := s.cfg
	metrics := h.metrics

	// Fast path: no hedging configured.
	if cfg.MaxCalls == 1 {
		v, err, dur := runAttempt(ctx, cfg, fn)
		metrics.RecordRequest(false)
		metrics.RecordResponse(false, err, dur)
		if err == nil {
			s.observe(dur)
		}
		return v, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		value     T
		err       error
		hedged    bool
		retryable bool
	}
	results := make(chan result, cfg.MaxCalls)
	var wg sync.WaitGroup

	launch := func(attempt int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hedged := attempt > 1

			metrics.RecordRequest(hedged)
			v, err, dur := runAttempt(runCtx, cfg, fn)
			metrics.RecordResponse(hedged, err, dur)

			// Only the first attempt's successful latency feeds the window.
			if !hedged && err == nil {
				s.observe(dur)
			}

			retryable := classifyRetryable(err, cfg, runCtx)

			select {
			case results <- result{value: v, err: err, hedged: hedged, retryable: retryable}:
			case <-runCtx.Done():
			}
		}()
	}

	go func() {
		launch(1)
		for attempt := 2; attempt <= cfg.MaxCalls; attempt++ {
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
			// Non-retryable error: this is the final answer, no point waiting
			// for or launching more attempts.
			if !r.retryable {
				return zero, r.err
			}
			if firstErr == nil {
				firstErr = r.err
			}
			failed++
			if failed >= cfg.MaxCalls {
				return zero, firstErr
			}
		}
	}
}

// runAttempt executes a single fn invocation with the configured AttemptTimeout
// applied (if any). Returns the result, error, and wall-clock duration of the call.
func runAttempt[T any](
	parent context.Context,
	cfg RegisterConfig,
	fn func(ctx context.Context) (T, error),
) (T, error, time.Duration) {
	attemptCtx := parent
	var cancel context.CancelFunc
	if cfg.AttemptTimeout > 0 {
		attemptCtx, cancel = context.WithTimeout(parent, cfg.AttemptTimeout)
		defer cancel()
	}

	start := time.Now()
	v, err := fn(attemptCtx)
	dur := time.Since(start)

	// If the parent is fine but the attempt context's deadline fired, this is
	// our AttemptTimeout. Replace the generic deadline error with our sentinel
	// so callers can distinguish attempt timeout from parent cancellation.
	if err != nil && cfg.AttemptTimeout > 0 && parent.Err() == nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = ErrAttemptTimeout
		}
	}
	return v, err, dur
}

// classifyRetryable decides whether an error should trigger more hedging or
// be returned to the caller immediately.
//
//   - nil: not applicable (caller checks err == nil first)
//   - parent context cancelled: parent will short-circuit anyway, value doesn't matter
//   - ErrAttemptTimeout: always retryable (infrastructure-level failure)
//   - IsRetryable nil: treat all errors as retryable (back-compat default)
//   - otherwise: ask the user's filter
func classifyRetryable(err error, cfg RegisterConfig, runCtx context.Context) bool {
	if err == nil {
		return true
	}
	if runCtx.Err() != nil {
		// Context already torn down; the main loop is about to exit anyway.
		return true
	}
	if errors.Is(err, ErrAttemptTimeout) {
		return true
	}
	if cfg.IsRetryable == nil {
		return true
	}
	return cfg.IsRetryable(err)
}
