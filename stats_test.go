package kiharo

import (
	"sync"
	"testing"
	"time"
)

func newTestStats(t *testing.T, cfg RegisterConfig) *stats {
	t.Helper()
	if err := cfg.validate(); err != nil {
		t.Fatalf("invalid test config: %v", err)
	}
	return newStats(cfg)
}

func TestStats_DefaultUntilFull(t *testing.T) {
	cfg := validConfig()
	cfg.Window = WindowSmall // 100
	cfg.DefaultDelay = 50 * time.Millisecond
	cfg.MinDelay = time.Millisecond
	cfg.MaxDelay = time.Second
	s := newTestStats(t, cfg)

	// Не наблюдаем ничего — окно пустое.
	if got := s.delay(); got != cfg.DefaultDelay {
		t.Errorf("empty window: got %v, want %v", got, cfg.DefaultDelay)
	}

	// Заполняем 99 значениями — всё ещё неполное.
	for i := 0; i < 99; i++ {
		s.observe(time.Millisecond)
	}
	if got := s.delay(); got != cfg.DefaultDelay {
		t.Errorf("almost-full window: got %v, want %v", got, cfg.DefaultDelay)
	}

	// 100-е заполняет окно — теперь возвращаем перцентиль.
	s.observe(time.Millisecond)
	if got := s.delay(); got != time.Millisecond {
		t.Errorf("full window with all 1ms: got %v, want 1ms", got)
	}
}

func TestStats_Percentile(t *testing.T) {
	cfg := validConfig()
	cfg.Window = WindowSmall // 100
	cfg.Percentile = P75
	cfg.MinDelay = 0
	cfg.MaxDelay = time.Hour
	s := newTestStats(t, cfg)

	// Заполняем значениями 1..100 ms. P75 при N=100 = индекс 74 (75-й элемент).
	for i := 1; i <= 100; i++ {
		s.observe(time.Duration(i) * time.Millisecond)
	}

	got := s.delay()
	want := 75 * time.Millisecond
	if got != want {
		t.Errorf("P75 of 1..100: got %v, want %v", got, want)
	}
}

func TestStats_PercentileBoundaries(t *testing.T) {
	tests := []struct {
		percentile Percentile
		wantIdx    int // 0-based index into sorted [1ms..100ms]
	}{
		{P75, 74}, // 75th element
		{P90, 89},
		{P95, 94},
	}

	for _, tt := range tests {
		t.Run(tt.percentile.String(), func(t *testing.T) {
			cfg := validConfig()
			cfg.Window = WindowSmall
			cfg.Percentile = tt.percentile
			cfg.MinDelay = 0
			cfg.MaxDelay = time.Hour
			s := newTestStats(t, cfg)

			for i := 1; i <= 100; i++ {
				s.observe(time.Duration(i) * time.Millisecond)
			}

			want := time.Duration(tt.wantIdx+1) * time.Millisecond
			if got := s.delay(); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestStats_ClampMin(t *testing.T) {
	cfg := validConfig()
	cfg.Window = WindowSmall
	cfg.MinDelay = 50 * time.Millisecond
	cfg.MaxDelay = time.Second
	s := newTestStats(t, cfg)

	for i := 0; i < 100; i++ {
		s.observe(time.Microsecond) // быстрее MinDelay
	}

	if got := s.delay(); got != cfg.MinDelay {
		t.Errorf("got %v, want clamped to MinDelay %v", got, cfg.MinDelay)
	}
}

func TestStats_ClampMax(t *testing.T) {
	cfg := validConfig()
	cfg.Window = WindowSmall
	cfg.MinDelay = 0
	cfg.MaxDelay = 100 * time.Millisecond
	s := newTestStats(t, cfg)

	for i := 0; i < 100; i++ {
		s.observe(time.Hour) // намного больше MaxDelay
	}

	if got := s.delay(); got != cfg.MaxDelay {
		t.Errorf("got %v, want clamped to MaxDelay %v", got, cfg.MaxDelay)
	}
}

func TestStats_ClampDefaultDelay(t *testing.T) {
	cfg := validConfig()
	cfg.Window = WindowSmall
	cfg.MinDelay = 50 * time.Millisecond
	cfg.MaxDelay = time.Second
	cfg.DefaultDelay = time.Microsecond // ниже MinDelay
	s := newTestStats(t, cfg)

	if got := s.delay(); got != cfg.MinDelay {
		t.Errorf("DefaultDelay should clamp: got %v, want %v", got, cfg.MinDelay)
	}
}

func TestStats_RingOverwrite(t *testing.T) {
	cfg := validConfig()
	cfg.Window = WindowSmall // 100
	cfg.Percentile = P75
	cfg.MinDelay = 0
	cfg.MaxDelay = time.Hour
	s := newTestStats(t, cfg)

	// Заполняем 100 значениями по 100ms.
	for i := 0; i < 100; i++ {
		s.observe(100 * time.Millisecond)
	}
	// Перезаписываем 100 раз новым значением 1ms.
	for i := 0; i < 100; i++ {
		s.observe(time.Millisecond)
	}

	// Теперь все 100 элементов = 1ms.
	if got := s.delay(); got != time.Millisecond {
		t.Errorf("after full overwrite: got %v, want 1ms", got)
	}
}

func TestStats_ConcurrentObserveAndDelay(t *testing.T) {
	cfg := validConfig()
	cfg.Window = WindowSmall
	s := newTestStats(t, cfg)

	const goroutines = 50
	const ops = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				s.observe(time.Duration(j) * time.Microsecond)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				_ = s.delay()
			}
		}()
	}

	wg.Wait()
	// Проверка race-detector'а — если этот тест прошёл с -race, всё ок.
}

// String для удобства имени t.Run в TestStats_PercentileBoundaries.
func (p Percentile) String() string {
	switch p {
	case P75:
		return "P75"
	case P90:
		return "P90"
	case P95:
		return "P95"
	}
	return "unknown"
}
