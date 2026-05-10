// Package kiharo implements adaptive hedged requests for reducing tail latency.
//
// A Hedger keeps per-key latency stats and uses a configurable percentile
// of recent successful first-attempt latencies as the delay before launching
// hedged attempts.
package kiharo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrUnknownKey = errors.New("kiharo: unknown key")
	ErrNilFunc    = errors.New("kiharo: nil func")
	ErrNilHedger  = errors.New("kiharo: nil Hedger")
)

type Option func(*Hedger)

func WithMetrics(r MetricsRecorder) Option {
	return func(h *Hedger) {
		if r != nil {
			h.metrics = r
		}
	}
}

// Hedger dispatches hedged calls and tracks per-key latency.
// One per application; safe for concurrent use.
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

// Register configures a key. Panics on invalid cfg or duplicate keys.
func (h *Hedger) Register(key string, cfg RegisterConfig) {
	if err := cfg.validate(); err != nil {
		panic(fmt.Sprintf("kiharo: invalid config for %q: %v", key, err))
	}
	if _, loaded := h.keys.LoadOrStore(key, newStats(cfg)); loaded {
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

// Do runs fn up to MaxCalls times in parallel. The first success wins;
// remaining attempts are cancelled. If all fail, returns the first error.
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

			if !hedged && err == nil {
				s.observe(dur)
			}

			r := result{
				value:     v,
				err:       err,
				hedged:    hedged,
				retryable: classifyRetryable(err, cfg, runCtx),
			}
			select {
			case results <- r:
			case <-runCtx.Done():
			}
		}()
	}

	go func() {
		launch(1)
		for attempt := 2; attempt <= cfg.MaxCalls; attempt++ {
			t := time.NewTimer(s.delay())
			select {
			case <-t.C:
				launch(attempt)
			case <-runCtx.Done():
				t.Stop()
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

func runAttempt[T any](
	parent context.Context,
	cfg RegisterConfig,
	fn func(ctx context.Context) (T, error),
) (T, error, time.Duration) {
	ctx := parent
	var cancel context.CancelFunc
	if cfg.AttemptTimeout > 0 {
		ctx, cancel = context.WithTimeout(parent, cfg.AttemptTimeout)
		defer cancel()
	}

	start := time.Now()
	v, err := fn(ctx)
	dur := time.Since(start)

	if err != nil && cfg.AttemptTimeout > 0 && parent.Err() == nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = ErrAttemptTimeout
		}
	}
	return v, err, dur
}

func classifyRetryable(err error, cfg RegisterConfig, runCtx context.Context) bool {
	if err == nil {
		return true
	}
	if runCtx.Err() != nil {
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
