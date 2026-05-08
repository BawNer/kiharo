package kiharo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fastConfig — конфиг с маленькими delay для быстрых тестов.
func fastConfig(maxCalls int) RegisterConfig {
	return RegisterConfig{
		MaxCalls:     maxCalls,
		Window:       WindowSmall,
		Percentile:   P75,
		MinDelay:     time.Microsecond,
		MaxDelay:     time.Second,
		DefaultDelay: 5 * time.Millisecond,
	}
}

func TestNew(t *testing.T) {
	h := New()
	if h == nil {
		t.Fatal("New returned nil")
	}
	if h.metrics == nil {
		t.Error("metrics should default to noopRecorder")
	}
}

func TestNew_WithMetrics(t *testing.T) {
	rec := &recordingMetrics{}
	h := New(WithMetrics(rec))
	if h.metrics != rec {
		t.Error("WithMetrics did not apply")
	}
}

func TestNew_WithMetricsNil(t *testing.T) {
	h := New(WithMetrics(nil))
	// noopRecorder остаётся.
	if _, ok := h.metrics.(noopRecorder); !ok {
		t.Errorf("nil metrics should be ignored, got %T", h.metrics)
	}
}

func TestRegister_PanicOnDuplicate(t *testing.T) {
	h := New()
	h.Register("k", fastConfig(2))

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate Register")
		}
	}()
	h.Register("k", fastConfig(2))
}

func TestRegister_PanicOnInvalidConfig(t *testing.T) {
	h := New()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on invalid config")
		}
	}()
	h.Register("k", RegisterConfig{}) // пустой = всё невалидно
}

func TestDo_NilHedger(t *testing.T) {
	_, err := Do[int](context.Background(), nil, "k", func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if !errors.Is(err, ErrNilHedger) {
		t.Errorf("got %v, want ErrNilHedger", err)
	}
}

func TestDo_NilFunc(t *testing.T) {
	h := New()
	h.Register("k", fastConfig(2))
	_, err := Do[int](context.Background(), h, "k", nil)
	if !errors.Is(err, ErrNilFunc) {
		t.Errorf("got %v, want ErrNilFunc", err)
	}
}

func TestDo_UnknownKey(t *testing.T) {
	h := New()
	_, err := Do[int](context.Background(), h, "missing", func(ctx context.Context) (int, error) {
		return 0, nil
	})
	if !errors.Is(err, ErrUnknownKey) {
		t.Errorf("got %v, want ErrUnknownKey", err)
	}
}

func TestDo_FirstSucceeds(t *testing.T) {
	h := New()
	h.Register("k", fastConfig(2))

	got, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestDo_HedgedWins(t *testing.T) {
	h := New()
	cfg := fastConfig(2)
	cfg.DefaultDelay = 5 * time.Millisecond
	cfg.MaxDelay = 50 * time.Millisecond
	h.Register("k", cfg)

	var attempts atomic.Int32
	got, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		n := attempts.Add(1)
		if n == 1 {
			// Первая попытка — медленнее delay, отдаём поздно.
			select {
			case <-time.After(200 * time.Millisecond):
				return 1, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		// Вторая попытка — быстрая.
		return 2, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 2 {
		t.Errorf("hedged should win: got %d, want 2", got)
	}
}

func TestDo_AllFailReturnsFirstError(t *testing.T) {
	h := New()
	cfg := fastConfig(3)
	cfg.DefaultDelay = time.Millisecond
	h.Register("k", cfg)

	errFirst := errors.New("first")
	errOther := errors.New("other")

	var n atomic.Int32
	_, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		i := n.Add(1)
		if i == 1 {
			// Первая возвращает ошибку быстро — она и должна быть выбрана как firstErr.
			return 0, errFirst
		}
		// Остальные — позже и с другой ошибкой.
		time.Sleep(50 * time.Millisecond)
		return 0, errOther
	})

	if !errors.Is(err, errFirst) {
		t.Errorf("got %v, want first error %v", err, errFirst)
	}
}

func TestDo_ParentCancelled(t *testing.T) {
	h := New()
	h.Register("k", fastConfig(2))

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := Do(ctx, h, "k", func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
}

func TestDo_LosersGetCancelled(t *testing.T) {
	h := New()
	cfg := fastConfig(2)
	cfg.DefaultDelay = 5 * time.Millisecond
	h.Register("k", cfg)

	loserCancelled := make(chan struct{}, 1)

	var n atomic.Int32
	got, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		i := n.Add(1)
		if i == 1 {
			// Первая ждёт отмены.
			<-ctx.Done()
			loserCancelled <- struct{}{}
			return 0, ctx.Err()
		}
		// Вторая отвечает быстро и побеждает.
		return 42, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}

	select {
	case <-loserCancelled:
		// Ок: первая получила сигнал отмены.
	case <-time.After(time.Second):
		t.Error("loser was not cancelled")
	}
}

func TestDo_MaxCallsOne_NoHedge(t *testing.T) {
	h := New()
	h.Register("k", fastConfig(1))

	var n atomic.Int32
	_, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		n.Add(1)
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.Load() != 1 {
		t.Errorf("MaxCalls=1 should call once, got %d", n.Load())
	}
}

func TestDo_StatsRecordOnlyFirstSuccess(t *testing.T) {
	h := New()
	cfg := fastConfig(2)
	cfg.DefaultDelay = 5 * time.Millisecond
	h.Register("k", cfg)
	s := h.statsFor("k")

	// Сценарий: первая медленная и упадёт, вторая быстрая и победит.
	// Окно НЕ должно получить запись, потому что первая попытка не успешна.
	_, _ = Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		select {
		case <-time.After(50 * time.Millisecond):
			return 0, errors.New("slow fail")
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	// На этой итерации обе ветки могут отрезолвиться по-разному в зависимости от тайминга,
	// но в любом случае: никакая успешная первая попытка не зарегистрирована.
	s.mu.Lock()
	count := s.count
	s.mu.Unlock()
	if count != 0 {
		t.Errorf("no successful first attempt expected, got count=%d", count)
	}

	// Сценарий: первая успешная и быстрая.
	_, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	s.mu.Lock()
	count = s.count
	s.mu.Unlock()
	if count != 1 {
		t.Errorf("first successful attempt should be recorded, got count=%d", count)
	}
}

func TestDo_Concurrent(t *testing.T) {
	h := New()
	h.Register("k", fastConfig(2))

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			got, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
				return i, nil
			})
			if err != nil {
				t.Errorf("goroutine %d: %v", i, err)
			}
			// Может вернуться любое значение из конкурирующих, но не должно быть ошибки.
			_ = got
		}(i)
	}
	wg.Wait()
}

// recordingMetrics — простой счётчик для проверки метрик.
type recordingMetrics struct {
	mu          sync.Mutex
	requests    int
	hedgedReqs  int
	responses   int
	hedgedResps int
	hedgeWins   int
}

func (r *recordingMetrics) RecordRequest(hedged bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests++
	if hedged {
		r.hedgedReqs++
	}
}

func (r *recordingMetrics) RecordResponse(hedged bool, _ error, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.responses++
	if hedged {
		r.hedgedResps++
	}
}

func (r *recordingMetrics) RecordHedgeWin() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hedgeWins++
}

func (r *recordingMetrics) snapshot() recordingMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return recordingMetrics{
		requests:    r.requests,
		hedgedReqs:  r.hedgedReqs,
		responses:   r.responses,
		hedgedResps: r.hedgedResps,
		hedgeWins:   r.hedgeWins,
	}
}

func TestMetrics_FirstSucceeds(t *testing.T) {
	rec := &recordingMetrics{}
	h := New(WithMetrics(rec))
	h.Register("k", fastConfig(2))

	_, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Дать таймеру в scheduler и hedge-горутине времени, если они стартуют.
	// Первая попытка отвечает мгновенно, поэтому hedged даже не должен запуститься.
	time.Sleep(50 * time.Millisecond)
	snap := rec.snapshot()

	if snap.requests < 1 {
		t.Errorf("requests=%d, want >=1", snap.requests)
	}
	if snap.hedgeWins != 0 {
		t.Errorf("hedgeWins=%d, want 0 (first won)", snap.hedgeWins)
	}
}

func TestMetrics_HedgedWins(t *testing.T) {
	rec := &recordingMetrics{}
	h := New(WithMetrics(rec))
	cfg := fastConfig(2)
	cfg.DefaultDelay = 5 * time.Millisecond
	h.Register("k", cfg)

	var n atomic.Int32
	_, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		i := n.Add(1)
		if i == 1 {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return 42, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Подождать, пока проигравший доберётся до RecordResponse после отмены.
	time.Sleep(50 * time.Millisecond)

	snap := rec.snapshot()
	if snap.hedgedReqs < 1 {
		t.Errorf("hedgedReqs=%d, want >=1", snap.hedgedReqs)
	}
	if snap.hedgeWins != 1 {
		t.Errorf("hedgeWins=%d, want 1", snap.hedgeWins)
	}
}

// Демонстрация того, что метрики не паникуют, если recorder не задан.
func TestMetrics_NoopByDefault(t *testing.T) {
	h := New()
	h.Register("k", fastConfig(2))
	_, err := Do(context.Background(), h, "k", func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Sanity check: убедиться, что Hedger.statsFor возвращает nil для незнакомого ключа.
func TestHedger_StatsForUnknown(t *testing.T) {
	h := New()
	if s := h.statsFor("nope"); s != nil {
		t.Errorf("statsFor unknown should return nil, got %v", s)
	}
}

// fmtErrorWrapping — гарантия, что обёрнутая ошибка от Do содержит ключ.
func TestDo_UnknownKeyContainsKey(t *testing.T) {
	h := New()
	_, err := Do[int](context.Background(), h, "the-key", func(ctx context.Context) (int, error) {
		return 0, nil
	})
	if err == nil || !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("got %v, want ErrUnknownKey", err)
	}
	if msg := err.Error(); !contains(msg, "the-key") {
		t.Errorf("error %q should contain key", msg)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(s) > 0 && (s[0:len(sub)] == sub || contains(s[1:], sub))))
}

// Покажем, что обёрнутая ошибка форматируется через %w.
func TestDo_UnknownKeyWrapped(t *testing.T) {
	h := New()
	_, err := Do[int](context.Background(), h, "x", func(ctx context.Context) (int, error) {
		return 0, nil
	})
	want := fmt.Sprintf("%s: %q", ErrUnknownKey.Error(), "x")
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}
