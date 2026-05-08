package kiharo

import (
	"errors"
	"testing"
	"time"
)

func validConfig() RegisterConfig {
	return RegisterConfig{
		MaxCalls:     2,
		Window:       WindowSmall,
		Percentile:   P75,
		MinDelay:     time.Millisecond,
		MaxDelay:     time.Second,
		DefaultDelay: 10 * time.Millisecond,
	}
}

func TestRegisterConfig_Validate_OK(t *testing.T) {
	if err := validConfig().validate(); err != nil {
		t.Fatalf("valid config should pass: %v", err)
	}
}

func TestRegisterConfig_Validate_Errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*RegisterConfig)
		wantErr error
	}{
		{"MaxCalls too low", func(c *RegisterConfig) { c.MaxCalls = 0 }, ErrInvalidMaxCalls},
		{"MaxCalls too high", func(c *RegisterConfig) { c.MaxCalls = 4 }, ErrInvalidMaxCalls},
		{"MaxCalls negative", func(c *RegisterConfig) { c.MaxCalls = -1 }, ErrInvalidMaxCalls},
		{"Window invalid", func(c *RegisterConfig) { c.Window = 42 }, ErrInvalidWindow},
		{"Window zero", func(c *RegisterConfig) { c.Window = 0 }, ErrInvalidWindow},
		{"Percentile invalid", func(c *RegisterConfig) { c.Percentile = 50 }, ErrInvalidPercentile},
		{"Percentile zero", func(c *RegisterConfig) { c.Percentile = 0 }, ErrInvalidPercentile},
		{"MinDelay negative", func(c *RegisterConfig) { c.MinDelay = -time.Millisecond }, ErrInvalidMinDelay},
		{"MaxDelay zero", func(c *RegisterConfig) { c.MaxDelay = 0 }, ErrInvalidMaxDelay},
		{"MaxDelay negative", func(c *RegisterConfig) { c.MaxDelay = -time.Second }, ErrInvalidMaxDelay},
		{"Min > Max", func(c *RegisterConfig) { c.MinDelay = time.Second; c.MaxDelay = time.Millisecond }, ErrInvalidDelayBounds},
		{"DefaultDelay zero", func(c *RegisterConfig) { c.DefaultDelay = 0 }, ErrInvalidDefaultDelay},
		{"DefaultDelay negative", func(c *RegisterConfig) { c.DefaultDelay = -time.Millisecond }, ErrInvalidDefaultDelay},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := cfg.validate()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestWindowSize_Valid(t *testing.T) {
	for _, w := range []WindowSize{WindowSmall, WindowMedium, WindowLarge} {
		if !w.valid() {
			t.Errorf("%d should be valid", w)
		}
	}
	for _, w := range []WindowSize{0, 1, 99, 101, 999, 1001} {
		if w.valid() {
			t.Errorf("%d should not be valid", w)
		}
	}
}

func TestPercentile_Valid(t *testing.T) {
	for _, p := range []Percentile{P75, P90, P95} {
		if !p.valid() {
			t.Errorf("%d should be valid", p)
		}
	}
	for _, p := range []Percentile{0, 50, 76, 99} {
		if p.valid() {
			t.Errorf("%d should not be valid", p)
		}
	}
}
