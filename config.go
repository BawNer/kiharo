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

var (
	ErrInvalidMaxCalls       = errors.New("kiharo: MaxCalls must be 1..3")
	ErrInvalidWindow         = errors.New("kiharo: invalid Window")
	ErrInvalidPercentile     = errors.New("kiharo: invalid Percentile")
	ErrInvalidMinDelay       = errors.New("kiharo: MinDelay must be >= 0")
	ErrInvalidMaxDelay       = errors.New("kiharo: MaxDelay must be > 0")
	ErrInvalidDelayBounds    = errors.New("kiharo: MinDelay > MaxDelay")
	ErrInvalidDefaultDelay   = errors.New("kiharo: DefaultDelay must be > 0")
	ErrInvalidAttemptTimeout = errors.New("kiharo: AttemptTimeout must be >= 0")
)

// ErrAttemptTimeout is returned when a single attempt exceeds AttemptTimeout.
// Wraps context.DeadlineExceeded.
var ErrAttemptTimeout = attemptTimeoutError{}

type attemptTimeoutError struct{}

func (attemptTimeoutError) Error() string   { return "kiharo: attempt timeout" }
func (attemptTimeoutError) Is(t error) bool { return t == context.DeadlineExceeded }
func (attemptTimeoutError) Unwrap() error   { return context.DeadlineExceeded }

type RegisterConfig struct {
	MaxCalls       int
	Window         WindowSize
	Percentile     Percentile
	MinDelay       time.Duration
	MaxDelay       time.Duration
	DefaultDelay   time.Duration
	AttemptTimeout time.Duration
	IsRetryable    func(error) bool
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
