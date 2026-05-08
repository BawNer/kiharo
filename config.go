package kiharo

import (
	"context"
	"errors"
	"time"
)

type WindowSize int

const (
	WindowSmall  WindowSize = 100
	WindowMedium WindowSize = 500
	WindowLarge  WindowSize = 1000
)

func (w WindowSize) valid() bool {
	switch w {
	case WindowSmall, WindowMedium, WindowLarge:
		return true
	}
	return false
}

type Percentile int

const (
	P75 Percentile = 75
	P90 Percentile = 90
	P95 Percentile = 95
)

func (p Percentile) valid() bool {
	switch p {
	case P75, P90, P95:
		return true
	}
	return false
}

const maxAllowedCalls = 3

// Config validation errors. Wrap-friendly: use errors.Is to check.
var (
	ErrInvalidMaxCalls       = errors.New("kiharo: MaxCalls must be between 1 and 3")
	ErrInvalidWindow         = errors.New("kiharo: Window must be WindowSmall, WindowMedium, or WindowLarge")
	ErrInvalidPercentile     = errors.New("kiharo: Percentile must be P75, P90, or P95")
	ErrInvalidMinDelay       = errors.New("kiharo: MinDelay must be >= 0")
	ErrInvalidMaxDelay       = errors.New("kiharo: MaxDelay must be > 0")
	ErrInvalidDelayBounds    = errors.New("kiharo: MinDelay must be <= MaxDelay")
	ErrInvalidDefaultDelay   = errors.New("kiharo: DefaultDelay must be > 0")
	ErrInvalidAttemptTimeout = errors.New("kiharo: AttemptTimeout must be >= 0")
)

// ErrAttemptTimeout is returned (as the cause) when an individual attempt
// exceeds RegisterConfig.AttemptTimeout. It wraps context.DeadlineExceeded,
// so errors.Is works for both:
//
//	errors.Is(err, kiharo.ErrAttemptTimeout) // attempt-level timeout
//	errors.Is(err, context.DeadlineExceeded) // any deadline-related error
var ErrAttemptTimeout = attemptTimeoutError{}

type attemptTimeoutError struct{}

func (attemptTimeoutError) Error() string        { return "kiharo: attempt timeout exceeded" }
func (attemptTimeoutError) Is(target error) bool { return target == context.DeadlineExceeded }
func (attemptTimeoutError) Unwrap() error        { return context.DeadlineExceeded }

// RegisterConfig is the per-key configuration passed to Hedger.Register.
type RegisterConfig struct {
	MaxCalls     int           // 1, 2, or 3. 1 disables hedging.
	Window       WindowSize    // sliding-window size for latency stats
	Percentile   Percentile    // window percentile used as hedge delay
	MinDelay     time.Duration // lower clamp on computed delay
	MaxDelay     time.Duration // upper clamp on computed delay
	DefaultDelay time.Duration // delay used until the window fills

	// AttemptTimeout caps wall-clock time of a single attempt. Zero means no
	// per-attempt timeout (only the parent context's deadline applies).
	// When an attempt times out, it is automatically treated as retryable
	// regardless of IsRetryable, since timeout is an infrastructure-level
	// failure rather than a domain-level outcome.
	AttemptTimeout time.Duration

	// IsRetryable filters which fn errors should trigger hedging vs return
	// immediately. Returning false means "this is a final answer, don't waste
	// effort on more attempts". Useful for HTTP 4xx, gRPC NotFound,
	// validation errors, etc.
	//
	// Nil means all errors are retryable (the previous default behaviour).
	// AttemptTimeout failures are always retryable regardless of this filter.
	IsRetryable func(error) bool
}

func (c RegisterConfig) validate() error {
	switch {
	case c.MaxCalls < 1 || c.MaxCalls > maxAllowedCalls:
		return ErrInvalidMaxCalls
	case !c.Window.valid():
		return ErrInvalidWindow
	case !c.Percentile.valid():
		return ErrInvalidPercentile
	case c.MinDelay < 0:
		return ErrInvalidMinDelay
	case c.MaxDelay <= 0:
		return ErrInvalidMaxDelay
	case c.MinDelay > c.MaxDelay:
		return ErrInvalidDelayBounds
	case c.DefaultDelay <= 0:
		return ErrInvalidDefaultDelay
	case c.AttemptTimeout < 0:
		return ErrInvalidAttemptTimeout
	}
	return nil
}

type MetricsRecorder interface {
	RecordRequest(hedged bool)
	RecordResponse(hedged bool, err error, duration time.Duration)
	RecordHedgeWin()
}

type noopRecorder struct{}

func (noopRecorder) RecordRequest(bool)                        {}
func (noopRecorder) RecordResponse(bool, error, time.Duration) {}
func (noopRecorder) RecordHedgeWin()                           {}
