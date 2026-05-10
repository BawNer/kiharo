package kiharo

import (
	"sort"
	"sync"
	"time"
)

type stats struct {
	cfg    RegisterConfig
	window int

	mu     sync.Mutex
	buf    []time.Duration
	count  int
	cursor int
}

func newStats(cfg RegisterConfig) *stats {
	w := int(cfg.Window)
	return &stats{
		cfg:    cfg,
		window: w,
		buf:    make([]time.Duration, w),
	}
}

func (s *stats) observe(d time.Duration) {
	s.mu.Lock()
	s.buf[s.cursor] = d
	s.cursor = (s.cursor + 1) % s.window
	if s.count < s.window {
		s.count++
	}
	s.mu.Unlock()
}

func (s *stats) delay() time.Duration {
	s.mu.Lock()
	if s.count < s.window {
		s.mu.Unlock()
		return clamp(s.cfg.DefaultDelay, s.cfg.MinDelay, s.cfg.MaxDelay)
	}
	snap := make([]time.Duration, s.window)
	copy(snap, s.buf)
	s.mu.Unlock()

	sort.Slice(snap, func(i, j int) bool { return snap[i] < snap[j] })

	idx := (int(s.cfg.Percentile)*s.window+99)/100 - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= s.window {
		idx = s.window - 1
	}
	return clamp(snap[idx], s.cfg.MinDelay, s.cfg.MaxDelay)
}

func clamp(v, lo, hi time.Duration) time.Duration {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
